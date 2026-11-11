// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// shippedInboxRangeWait is the budget as built: package variables initialise before init, which shortens it for
// every other test of the package (their pages decide from the reply, or end at stop 2 at once).
var shippedInboxRangeWait = inboxRangeWait

func init() { inboxRangeWait = 50 * time.Millisecond }

func TestShippedInboxRangeWait(t *testing.T) {
	if shippedInboxRangeWait != 10*time.Second {
		t.Fatalf("shipped inbox range wait = %v, want 10s", shippedInboxRangeWait)
	}
}

// laterFrame is an event the library hands the session's event handler after the reply to a request: a table on
// another frame (socket.go handleFrame -> HandleEvent), or a connection event. A zero delay delivers it before the
// reply returns (the live frame overtakes the one-off stream's data frame).
type laterFrame struct {
	after time.Duration
	ev    any
}

// framedInbox is the listing's client for a server that splits its answer: ExecuteTasks returns the scripted reply
// of each main-inbox request (the one-off stream's first data frame, all ExecuteTasks ever returns) and delivers that
// request's scripted later events through handleMetaEvent, the session's real live path. onRequest, if set, runs
// inside each inbox request (to stand in for something the library does while it is in flight). Other scopes' first
// pages are answered empty. It does not model the transport below ExecuteTasks: a second frame on the same one-off
// stream never reaches the session through any path, so that case is covered only by its count (see
// TestLibraryDropWarningsAreCountedByMessageOnly).
type framedInbox struct {
	s         *messagixSession
	replies   []*table.LSTable
	later     [][]laterFrame
	onRequest func()
	tasks     []socket.FetchThreadsTask
	wg        sync.WaitGroup
}

func (p *framedInbox) GetCursor(int64) string { return "" }

func (p *framedInbox) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if !isInboxTask(tasks[0]) {
		return &table.LSTable{}, nil
	}
	p.tasks = append(p.tasks, *tasks[0].(*socket.FetchThreadsTask))
	i := len(p.tasks) - 1
	if i >= len(p.replies) {
		return nil, errors.New("walked past the scripted pages")
	}
	if p.onRequest != nil {
		p.onRequest()
	}
	if i < len(p.later) {
		for _, f := range p.later[i] {
			if f.after == 0 {
				p.s.handleMetaEvent(ctx, f.ev)
				continue
			}
			p.wg.Add(1)
			go func(f laterFrame) {
				defer p.wg.Done()
				time.Sleep(f.after)
				p.s.handleMetaEvent(context.Background(), f.ev)
			}(f)
		}
	}
	return p.replies[i], nil
}

// tpage is a reply holding one inbox thread whose last activity is ts (the page's oldest thread), and range rows.
func tpage(thread, ts int64, rows ...*table.LSUpsertSyncGroupThreadsRange) *table.LSTable {
	return &table.LSTable{
		LSDeleteThenInsertThread:      []*table.LSDeleteThenInsertThread{{ThreadKey: thread, ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "T", LastActivityTimestampMs: ts}},
		LSUpsertSyncGroupThreadsRange: rows,
	}
}

func rangeOnly(rows ...*table.LSUpsertSyncGroupThreadsRange) *table.LSTable {
	return &table.LSTable{LSUpsertSyncGroupThreadsRange: rows}
}

func inboxTableRow(more bool, minKey, minTS int64) *table.LSUpsertInboxThreadsRange {
	return &table.LSUpsertInboxThreadsRange{SyncGroup: 1, HasMoreBefore: more, MinThreadKey: minKey, MinLastActivityTimestampMs: minTS}
}

func inboxTable(rows ...*table.LSUpsertInboxThreadsRange) *table.LSTable {
	return &table.LSTable{LSUpsertInboxThreadsRange: rows}
}

func v2Table(parent, minKey, minTS int64) *table.LSTable {
	return &table.LSTable{LSUpdateThreadsRangesV2: []*table.LSUpdateThreadsRangesV2{{FolderName: "inbox", ParentThreadKey: parent, MinThreadKey: minKey, MinLastActivityTimestampMs: minTS}}}
}

func withThread(thread, ts int64, tbl *table.LSTable) *table.LSTable {
	out := *tbl
	out.LSDeleteThenInsertThread = tpage(thread, ts).LSDeleteThenInsertThread
	return &out
}

func late(after time.Duration, ev any) laterFrame { return laterFrame{after: after, ev: ev} }

var disconnect = &messagix.TransientDisconnectEvent{Err: errors.New("closed")}

func withRangeWait(t *testing.T, d time.Duration) {
	t.Helper()
	old, oldDelay := inboxRangeWait, threadBatchDelay
	inboxRangeWait, threadBatchDelay = d, time.Millisecond
	t.Cleanup(func() { inboxRangeWait, threadBatchDelay = old, oldDelay })
}

type walkWant struct {
	gap         string
	stop, pages int
	refs        [][2]int64 // the reference (thread key, activity ms) of each request after the first
}

func runFramed(t *testing.T, p *framedInbox) (listingDone, time.Duration) {
	t.Helper()
	start := time.Now()
	_, res, err, done := listGap(t, p.s, p, context.Background(), 10*time.Second)
	took := time.Since(start)
	p.wg.Wait()
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	return res, took
}

func checkWalk(t *testing.T, p *framedInbox, res listingDone, want walkWant) {
	t.Helper()
	if res.Gap != want.gap || res.Stats["inbox_stop"] != want.stop || res.Stats["inbox_pages"] != want.pages {
		t.Fatalf("gap=%q stop=%d pages=%d, want gap %q stop %d pages %d; stats=%v", res.Gap, res.Stats["inbox_stop"], res.Stats["inbox_pages"], want.gap, want.stop, want.pages, res.Stats)
	}
	if len(p.tasks) != 1+len(want.refs) {
		t.Fatalf("requests = %+v, want %d", p.tasks, 1+len(want.refs))
	}
	for i, ref := range want.refs {
		task := p.tasks[i+1]
		if task.ParentThreadKey != -1 || task.SyncGroup != 1 || task.ReferenceThreadKey != ref[0] || task.ReferenceActivityTimestamp != ref[1] {
			t.Fatalf("request %d = %+v, want parent -1 group 1 at %v", i+2, task, ref)
		}
	}
}

// (a) Page 1's reply has its thread but no range; the range comes on a later frame through the live path. The walk
// continues to page 2 from it and ends where page 2 says. 55b3df5 stopped with "no range" (stop 2) on page 1.
func TestWalkTakesTheRangeFromALaterFrame(t *testing.T) {
	for name, frame := range map[string]laterFrame{
		"after the reply":         late(30*time.Millisecond, rangeOnly(rangeRow(true, -1, 90, 5000))),
		"before the reply":        late(0, rangeOnly(rangeRow(true, -1, 90, 5000))),
		"parent-0 more cursor":    late(30*time.Millisecond, rangeOnly(rangeRow(true, 0, 90, 5000))),
		"inbox table more":        late(30*time.Millisecond, inboxTable(inboxTableRow(true, 90, 5000))),
		"after a stray row":       late(30*time.Millisecond, rangeOnly(rangeRow(false, 7, 0, 0), rangeRow(true, -1, 90, 5000))),
		"a deeper row, then ours": late(30*time.Millisecond, rangeOnly(rangeRow(true, -1, 10, 100), rangeRow(true, -1, 90, 5000))),
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			s := newTestSession()
			p := &framedInbox{s: s,
				replies: []*table.LSTable{tpage(101, 5000), tpage(102, 4000, rangeRow(false, -1, 80, 4000))},
				later:   [][]laterFrame{{frame}},
			}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
			if res.Stats["inbox_pages_late"] != 1 || res.Stats["inbox_rows_late"] == 0 {
				t.Fatalf("late stats = %v", res.Stats)
			}
		})
	}
}

// (b) The range is only in one of the other tables. A group-1 inbox-table row in the reply continues and ends the walk;
// one on a later frame continues it. V2 is counted but never moves the cursor: with only V2 rows the page stops 2.
// 55b3df5 read neither table and stopped 2 on the inbox-table cases; 7d56aa68 moved the cursor on a V2 row (the
// review's cursor skip), which fails "V2 ... is counted only".
func TestWalkReadsTheOtherRangeTables(t *testing.T) {
	for name, tc := range map[string]struct {
		replies []*table.LSTable
		later   [][]laterFrame
		want    walkWant
		stats   map[string]int
	}{
		"inbox table in the reply, more then end": {
			replies: []*table.LSTable{withThread(101, 5000, inboxTable(inboxTableRow(true, 90, 5000))), withThread(102, 4000, inboxTable(inboxTableRow(false, 80, 4000)))},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
			stats:   map[string]int{"inbox_rows_inbox_table": 2, "inbox_pages_late": 0},
		},
		"inbox table on a later frame": {
			replies: []*table.LSTable{tpage(101, 5000), tpage(102, 4000, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{late(30*time.Millisecond, inboxTable(inboxTableRow(true, 90, 5000)))}},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
			stats:   map[string]int{"inbox_late_inbox_more": 1, "inbox_pages_late": 1},
		},
		"V2 in the reply is counted only": {
			replies: []*table.LSTable{withThread(101, 5000, v2Table(-1, 90, 5000)), tpage(102, 4000, rangeRow(false, -1, 80, 4000))},
			want:    walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1},
			stats:   map[string]int{"inbox_rows_v2": 1},
		},
		"V2 on a later frame is counted only": {
			replies: []*table.LSTable{tpage(101, 5000), tpage(102, 4000, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{late(20*time.Millisecond, v2Table(-1, 90, 5000)), late(40*time.Millisecond, v2Table(7, 90, 5000))}},
			want:    walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1},
			stats:   map[string]int{"inbox_late_v2_inbox": 1, "inbox_late_v2_other": 1, "inbox_rows_late": 2},
		},
		"the sync-group row outranks the inbox table": {
			replies: []*table.LSTable{withThread(101, 5000, &table.LSTable{
				LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{rangeRow(false, -1, 90, 5000)},
				LSUpsertInboxThreadsRange:     []*table.LSUpsertInboxThreadsRange{inboxTableRow(true, 80, 4000)},
			})},
			want: walkWant{stop: 0, pages: 1},
		},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 300*time.Millisecond)
			s := newTestSession()
			p := &framedInbox{s: s, replies: tc.replies, later: tc.later}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, tc.want)
			for k, v := range tc.stats {
				if res.Stats[k] != v {
					t.Fatalf("stats[%s]=%d, want %d; %v", k, res.Stats[k], v, res.Stats)
				}
			}
		})
	}
}

// (c) No range that answers the page within the budget: the same stop as today (stop 2, inbox_incomplete), after
// waiting out the budget. Rows that cannot answer the page are not taken for its range. "older than the page" is the
// review's cursor skip: a row from deeper in the inbox would move the walk past unread threads.
func TestWalkWithoutARangeStopsAsBefore(t *testing.T) {
	const wait = 200 * time.Millisecond
	two := []*table.LSTable{tpage(101, 5000, rangeRow(true, -1, 90, 5000)), tpage(102, 4000)}
	for name, tc := range map[string]struct {
		before   *table.LSTable // delivered live before the walk starts
		replies  []*table.LSTable
		later    [][]laterFrame
		pages    int
		refs     [][2]int64
		rejected int
	}{
		"nothing":                         {replies: []*table.LSTable{tpage(101, 5000)}, pages: 1},
		"another parent":                  {replies: []*table.LSTable{tpage(101, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, rangeOnly(rangeRow(true, 7, 90, 5000), rangeRow(false, 7, 0, 0)))}}, pages: 1},
		"another sync group":              {replies: []*table.LSTable{tpage(101, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, &table.LSTable{LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{{SyncGroup: 95, ParentThreadKey: -1, HasMoreBefore: true, MinThreadKey: 90, MinLastActivityTimestampMS: 5000}}, LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{{SyncGroup: 95, HasMoreBefore: true, MinThreadKey: 90, MinLastActivityTimestampMs: 5000}}})}}, pages: 1},
		"before the walk":                 {before: rangeOnly(rangeRow(true, -1, 90, 5000)), replies: []*table.LSTable{tpage(101, 5000)}, pages: 1},
		"after the window":                {replies: []*table.LSTable{tpage(101, 5000)}, later: [][]laterFrame{{late(3*wait, rangeOnly(rangeRow(true, -1, 90, 5000)))}}, pages: 1},
		"newer than the reference":        {replies: two, later: [][]laterFrame{nil, {late(20*time.Millisecond, rangeOnly(rangeRow(true, -1, 95, 6000)))}}, pages: 2, refs: [][2]int64{{90, 5000}}, rejected: 1},
		"at the reference":                {replies: two, later: [][]laterFrame{nil, {late(20*time.Millisecond, rangeOnly(rangeRow(true, -1, 90, 5000)))}}, pages: 2, refs: [][2]int64{{90, 5000}}, rejected: 1},
		"older than the page":             {replies: two, later: [][]laterFrame{nil, {late(20*time.Millisecond, rangeOnly(rangeRow(true, -1, 10, 100)))}}, pages: 2, refs: [][2]int64{{90, 5000}}, rejected: 1},
		"inbox table older than the page": {replies: two, later: [][]laterFrame{nil, {late(20*time.Millisecond, inboxTable(inboxTableRow(true, 10, 100)))}}, pages: 2, refs: [][2]int64{{90, 5000}}, rejected: 1},
		"a reply with no threads":         {replies: []*table.LSTable{{}}, later: [][]laterFrame{{late(20*time.Millisecond, rangeOnly(rangeRow(true, -1, 90, 5000)))}}, pages: 1, rejected: 1},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, wait)
			s := newTestSession()
			if tc.before != nil {
				s.handleMetaEvent(context.Background(), tc.before)
			}
			p := &framedInbox{s: s, replies: tc.replies, later: tc.later}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: tc.pages, refs: tc.refs})
			if took < wait || res.Stats["inbox_range_wait_ms"] < int(wait.Milliseconds())-1 {
				t.Fatalf("walk ended after %v (waited %d ms), before the %v budget", took, res.Stats["inbox_range_wait_ms"], wait)
			}
			if res.Stats["inbox_pages_late"] != 0 || res.Stats["inbox_late_rejected"] != tc.rejected {
				t.Fatalf("late stats = %v, want %d rejected", res.Stats, tc.rejected)
			}
		})
	}
}

// Review P1: a late end row that does not belong to the page never completes the walk. Page 1 says more; page 2's
// reply has no range; the late rows below arrive while page 2 waits, and every one must leave today's honest stop 2.
// On 7d56aa68 the parent-0 end, the inbox-table end and the other-connection end each completed the walk (stop 0).
func TestStaleLateEndRowsNeverComplete(t *testing.T) {
	for name, frames := range map[string][]laterFrame{
		"parent-0 end (group-wide, from another request)": {late(20*time.Millisecond, rangeOnly(rangeRow(false, 0, 0, 0)))},
		"inbox-table end":                      {late(20*time.Millisecond, inboxTable(inboxTableRow(false, 0, 0)))},
		"inbox-table end at the page":          {late(20*time.Millisecond, inboxTable(inboxTableRow(false, 80, 4000)))},
		"inbox end from an earlier connection": {late(10*time.Millisecond, disconnect), late(30*time.Millisecond, rangeOnly(rangeRow(false, -1, 0, 0)))},
		"inbox end from deeper in the inbox":   {late(20*time.Millisecond, rangeOnly(rangeRow(false, -1, 10, 100)))},
		"inbox end from an earlier page":       {late(20*time.Millisecond, rangeOnly(rangeRow(false, -1, 95, 6000)))},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			s := newTestSession()
			p := &framedInbox{s: s,
				replies: []*table.LSTable{tpage(101, 5000, rangeRow(true, -1, 90, 5000)), tpage(102, 4000)},
				later:   [][]laterFrame{nil, frames},
			}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}})
			if strings.Contains(name, "connection") && res.Stats["inbox_late_other_epoch"] != 1 {
				t.Fatalf("other-epoch row not counted: %v", res.Stats)
			}
		})
	}
}

// Review P2: the wait is ONE budget per walk. Every page's range comes 120 ms late against a 200 ms budget: page 1
// spends 120 ms, page 2 can wait only the 80 ms left and stops 2. On 7d56aa68 the budget reset per page and the walk
// went on to page 3 and beyond. A row that arrived before the budget ran out still decides a page without waiting.
func TestOneWaitBudgetPerWalk(t *testing.T) {
	const budget, lag = 200 * time.Millisecond, 120 * time.Millisecond
	t.Run("spent across pages", func(t *testing.T) {
		withRangeWait(t, budget)
		s := newTestSession()
		p := &framedInbox{s: s}
		for i := int64(0); i < 4; i++ {
			p.replies = append(p.replies, tpage(100+i, 5000-i))
			p.later = append(p.later, []laterFrame{late(lag, rangeOnly(rangeRow(true, -1, 90-i, 5000-i)))})
		}
		res, took := runFramed(t, p)
		checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}})
		if w := res.Stats["inbox_range_wait_ms"]; w < int(budget.Milliseconds())-1 || w > int(budget.Milliseconds())+100 {
			t.Fatalf("waited %d ms in all, want about %v", w, budget)
		}
		if took > 3*budget {
			t.Fatalf("walk took %v", took)
		}
	})
	t.Run("a row that already arrived decides with no budget left", func(t *testing.T) {
		s := newTestSession()
		w := s.openWatch()
		s.handleMetaEvent(context.Background(), rangeOnly(rangeRow(true, -1, 90, 5000)))
		var counts inboxRowCounts
		rng, waited, err := s.awaitInboxRange(context.Background(), w, inboxTop, 5000, true, 0, &counts)
		s.closeWatch(w, &counts)
		if err != nil || rng == nil || !rng.more || rng.minKey != 90 || rng.minTS != 5000 || waited > 50*time.Millisecond {
			t.Fatalf("rng=%+v waited=%v err=%v", rng, waited, err)
		}
		w = s.openWatch()
		rng, _, err = s.awaitInboxRange(context.Background(), w, inboxTop, 5000, true, 0, &counts)
		s.closeWatch(w, &counts)
		if err != nil || rng != nil {
			t.Fatalf("with nothing arrived and no budget: rng=%+v err=%v", rng, err)
		}
	})
}

// (d) The final page: an inbox-parent end row on a later frame, at the page's position or with none, ends the listing
// complete without another request. 55b3df5 stopped incomplete on that page.
func TestWalkCompletesOnALateEndRow(t *testing.T) {
	for name, end := range map[string]*table.LSTable{
		"inbox end at the page":  rangeOnly(rangeRow(false, -1, 80, 4000)),
		"inbox end, no position": rangeOnly(rangeRow(false, -1, 0, 0)),
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			s := newTestSession()
			p := &framedInbox{s: s,
				replies: []*table.LSTable{tpage(101, 5000, rangeRow(true, -1, 90, 5000)), tpage(102, 4000)},
				later:   [][]laterFrame{nil, {late(30*time.Millisecond, end)}},
			}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
		})
	}
	t.Run("single page", func(t *testing.T) {
		withRangeWait(t, 2*time.Second)
		s := newTestSession()
		p := &framedInbox{s: s, replies: []*table.LSTable{tpage(101, 5000)}, later: [][]laterFrame{{late(30*time.Millisecond, rangeOnly(rangeRow(false, -1, 101, 5000)))}}}
		res, _ := runFramed(t, p)
		checkWalk(t, p, res, walkWant{stop: 0, pages: 1})
	})
}

// The walk cap and resume hold when every page's range is late: an endless inbox ends at the cap (stop 4) after
// exactly that many requests, saving its continuation for the next walk.
func TestLateRangesKeepTheWalkCapAndResume(t *testing.T) {
	withRangeWait(t, 2*time.Second)
	old := threadBatchLimit
	threadBatchLimit = 3
	defer func() { threadBatchLimit = old }()
	s := newTestSession()
	p := &framedInbox{s: s}
	for i := int64(0); i < 5; i++ {
		p.replies = append(p.replies, tpage(100+i, 5000-i))
		p.later = append(p.later, []laterFrame{late(5*time.Millisecond, rangeOnly(rangeRow(true, -1, 90-i, 5000-i)))})
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 4, pages: 3, refs: [][2]int64{{90, 5000}, {89, 4999}}})
	s.mu.Lock()
	resume := s.inboxResume
	s.mu.Unlock()
	if resume == nil || resume.threadKey != 88 || resume.activityMS != 4998 {
		t.Fatalf("resume = %+v, want 88/4998", resume)
	}
}

// The counts are behaviour-neutral and always on: range rows of every table, kind and parent that arrive while a
// page's watch is open are counted, even when the reply itself decides the page, and they change nothing.
func TestLateRangeRowsAreCountedByKind(t *testing.T) {
	withRangeWait(t, time.Second)
	s := newTestSession()
	before := &table.LSTable{
		LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{
			rangeRow(true, -1, 1, 1), rangeRow(false, -1, 0, 0), rangeRow(true, 0, 1, 1), rangeRow(false, 0, 0, 0), rangeRow(false, 7, 0, 0),
			{SyncGroup: 95, ParentThreadKey: -1},
		},
		LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{inboxTableRow(true, 1, 1), inboxTableRow(false, 0, 0), {SyncGroup: 95}},
		LSUpdateThreadsRangesV2:   v2Table(-1, 1, 1).LSUpdateThreadsRangesV2,
	}
	before.LSUpdateThreadsRangesV2 = append(before.LSUpdateThreadsRangesV2, v2Table(7, 1, 1).LSUpdateThreadsRangesV2...)
	p := &framedInbox{s: s,
		replies: []*table.LSTable{withThread(101, 5000, &table.LSTable{
			LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{rangeRow(false, -1, 90, 5000)},
			LSUpsertInboxThreadsRange:     []*table.LSUpsertInboxThreadsRange{inboxTableRow(false, 90, 5000)},
			LSUpdateThreadsRangesV2:       v2Table(-1, 90, 5000).LSUpdateThreadsRangesV2,
		})},
		later: [][]laterFrame{{late(0, before), late(0, disconnect), late(0, rangeOnly(rangeRow(true, -1, 1, 1)))}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{stop: 0, pages: 1})
	want := map[string]int{
		"inbox_rows_inbox_table": 1, "inbox_rows_v2": 1, "inbox_rows_late": 10, "inbox_pages_late": 0, "inbox_range_wait_ms": 0,
		"inbox_late_sync_inbox_more": 2, "inbox_late_sync_inbox_end": 1, "inbox_late_sync_p0_more": 1, "inbox_late_sync_p0_end": 1,
		"inbox_late_sync_other": 1, "inbox_late_inbox_more": 1, "inbox_late_inbox_end": 1, "inbox_late_v2_inbox": 1,
		"inbox_late_v2_other": 1, "inbox_late_other_epoch": 1, "inbox_late_rejected": 0,
		"oneoff_extra_frames": 0, "oneoff_unknown_stream_frames": 0,
	}
	for k, v := range want {
		if got, ok := res.Stats[k]; !ok || got != v {
			t.Fatalf("stats[%s]=%d (present %v), want %d; %v", k, got, ok, v, res.Stats)
		}
	}
}

// The library's dropped-frame warnings are counted by their fixed message through the logger the session gives the
// client, also through the derived loggers the library builds (socket, stream), and per listing on threads_done. The
// test cannot make the pinned library drop a frame (that needs its transport), so it emits the warning as the
// library does, on a derived logger, while the walk's request is in flight.
func TestLibraryDropWarningsAreCountedByMessageOnly(t *testing.T) {
	s := newTestSession()
	root := s.libraryLogger()
	sock := root.With().Str("socket", "main").Logger()
	stream := sock.With().Uint16("stream_id", 7).Logger()
	stream.Warn().Uint16("stream_id", 7).Msg(msgOneOffExtraFrame)
	sock.Warn().Any("frame", map[string]any{"payload": "x"}).Msg(msgUnknownStreamFrame)
	sock.Debug().Msg(msgOneOffExtraFrame)         // below the logger's level: never counted
	sock.Warn().Msg("some other library warning") // not a dropped frame
	if extra, unknown := s.frameDrops(); extra != 1 || unknown != 1 {
		t.Fatalf("drops = %d, %d; want 1, 1", extra, unknown)
	}

	withRangeWait(t, 50*time.Millisecond)
	p := &framedInbox{s: s, replies: []*table.LSTable{tpage(101, 5000, rangeRow(false, -1, 90, 5000))},
		onRequest: func() { stream.Warn().Msg(msgOneOffExtraFrame); stream.Warn().Msg(msgOneOffExtraFrame) }}
	res, _ := runFramed(t, p)
	if res.Stats["oneoff_extra_frames"] != 2 || res.Stats["oneoff_unknown_stream_frames"] != 0 {
		t.Fatalf("listing drop counts = %v", res.Stats)
	}
}

// The counted messages are the pinned library's own strings: a library bump that rewords them fails here instead
// of silently counting nothing.
func TestDropWarningMessagesMatchThePinnedLibrary(t *testing.T) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "go.mau.fi/mautrix-meta").Output()
	if err != nil {
		t.Skipf("go list: %v", err)
	}
	dir := filepath.Join(strings.TrimSpace(string(out)), "pkg", "messagix", "dgw")
	for file, msg := range map[string]string{"oneoffstream.go": msgOneOffExtraFrame, "socket.go": msgUnknownStreamFrame} {
		src, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if !strings.Contains(string(src), `Warn()`) || !strings.Contains(string(src), `Msg("`+msg+`")`) {
			t.Fatalf("%s no longer warns %q", file, msg)
		}
	}
}

// The watch closes with the walk.
func TestRangeWatchClosesWithTheWalk(t *testing.T) {
	withRangeWait(t, 50*time.Millisecond)
	s := newTestSession()
	p := &framedInbox{s: s, replies: []*table.LSTable{tpage(101, 5000)}}
	runFramed(t, p)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rangeWatch != nil {
		t.Fatal("a range watch outlived its walk")
	}
}

// A cancel during the wait ends the listing with the cancel at once, not after the budget.
func TestRangeWaitHonoursCancel(t *testing.T) {
	withRangeWait(t, time.Minute)
	s := newTestSession()
	ctx, cancel := context.WithCancel(context.Background())
	p := &framedInbox{s: s, replies: []*table.LSTable{tpage(101, 5000)}}
	time.AfterFunc(50*time.Millisecond, cancel)
	_, _, err, done := listGap(t, s, p, ctx, 5*time.Second)
	if !done || !errors.Is(err, context.Canceled) {
		t.Fatalf("done=%v err=%v", done, err)
	}
}
