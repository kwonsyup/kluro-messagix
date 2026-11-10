// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// shippedInboxRangeWait is the bound as built: package variables initialise before init, which shortens the wait
// for every other test of the package (their pages decide from the reply, or end at stop 2 at once).
var shippedInboxRangeWait = inboxRangeWait

func init() { inboxRangeWait = 50 * time.Millisecond }

func TestShippedInboxRangeWait(t *testing.T) {
	if shippedInboxRangeWait != 10*time.Second {
		t.Fatalf("shipped inbox range wait = %v, want 10s", shippedInboxRangeWait)
	}
}

// laterFrame is a table the server sends after the reply to a request, on another frame: the library hands it to the
// session's event handler (socket.go handleFrame -> HandleEvent), as handleMetaEvent receives it here. A zero delay
// delivers it before the reply returns (the live frame overtakes the one-off stream's data frame).
type laterFrame struct {
	after time.Duration
	tbl   *table.LSTable
}

// framedInbox is the listing's client for a server that splits its answer: ExecuteTasks returns the scripted reply
// of each main-inbox request (the one-off stream's first data frame, all ExecuteTasks ever returns) and delivers that
// request's scripted later frames through the live path. Other scopes' first pages are answered empty.
type framedInbox struct {
	s       *messagixSession
	replies []*table.LSTable
	later   [][]laterFrame
	tasks   []socket.FetchThreadsTask
	wg      sync.WaitGroup
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
	if i < len(p.later) {
		for _, f := range p.later[i] {
			if f.after == 0 {
				p.s.handleMetaEvent(ctx, f.tbl)
				continue
			}
			p.wg.Add(1)
			go func(f laterFrame) {
				defer p.wg.Done()
				time.Sleep(f.after)
				p.s.handleMetaEvent(context.Background(), f.tbl)
			}(f)
		}
	}
	return p.replies[i], nil
}

func rangeOnly(rows ...*table.LSUpsertSyncGroupThreadsRange) *table.LSTable {
	return &table.LSTable{LSUpsertSyncGroupThreadsRange: rows}
}

func inboxTableRow(more bool, minKey, minTS int64) *table.LSUpsertInboxThreadsRange {
	return &table.LSUpsertInboxThreadsRange{SyncGroup: 1, HasMoreBefore: more, MinThreadKey: minKey, MinLastActivityTimestampMs: minTS}
}

func v2Row(parent, minKey, minTS int64) *table.LSUpdateThreadsRangesV2 {
	return &table.LSUpdateThreadsRangesV2{FolderName: "inbox", ParentThreadKey: parent, MinThreadKey: minKey, MinLastActivityTimestampMs: minTS}
}

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

func checkWalk(t *testing.T, p *framedInbox, res listingDone, err error, done bool, want walkWant) {
	t.Helper()
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
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

// (a) The reply to page 1 has its threads but no range row; the range comes on a later frame, through the live path.
// The walk continues to page 2 from that row and ends where page 2 says. The old walk read only the reply and stopped
// with "no range" (stop 2) on page 1.
func TestWalkTakesTheRangeFromALaterFrame(t *testing.T) {
	for name, frame := range map[string]laterFrame{
		"after the reply":      {after: 30 * time.Millisecond, tbl: rangeOnly(rangeRow(true, -1, 90, 5000))},
		"before the reply":     {tbl: rangeOnly(rangeRow(true, -1, 90, 5000))},
		"parent-0 more cursor": {after: 30 * time.Millisecond, tbl: rangeOnly(rangeRow(true, 0, 90, 5000))},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			s := newTestSession()
			p := &framedInbox{s: s,
				replies: []*table.LSTable{page(101), page(102, rangeRow(false, -1, 80, 4000))},
				later:   [][]laterFrame{{frame}},
			}
			frames, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
			p.wg.Wait()
			checkWalk(t, p, res, err, done, walkWant{gap: "", stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
			if res.Stats["inbox_pages_late"] != 1 || res.Stats["inbox_rows_late"] != 1 {
				t.Fatalf("late stats = %v", res.Stats)
			}
			got := threadsByID(frames)
			if got["101"] == nil || got["102"] == nil {
				t.Fatalf("threads emitted = %v", got)
			}
		})
	}
}

// (b) The range is only in one of the other two range tables, in the reply or on a later frame: the walk continues.
// An inbox-table row decides (it has a has-more flag); a V2 row is a cursor only and never completes the walk.
func TestWalkReadsTheOtherRangeTables(t *testing.T) {
	inboxMore := &table.LSTable{LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{inboxTableRow(true, 90, 5000)}}
	inboxEnd := &table.LSTable{LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{inboxTableRow(false, 80, 4000)}}
	v2 := &table.LSTable{LSUpdateThreadsRangesV2: []*table.LSUpdateThreadsRangesV2{v2Row(-1, 90, 5000)}}
	withThread := func(thread int64, tbl *table.LSTable) *table.LSTable {
		out := *tbl
		out.LSDeleteThenInsertThread = page(thread).LSDeleteThenInsertThread
		return &out
	}
	for name, tc := range map[string]struct {
		replies []*table.LSTable
		later   [][]laterFrame
		want    walkWant
	}{
		"inbox table in the reply, more then end": {
			replies: []*table.LSTable{withThread(101, inboxMore), withThread(102, inboxEnd)},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
		},
		"inbox table on a later frame": {
			replies: []*table.LSTable{page(101), page(102, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{{after: 30 * time.Millisecond, tbl: inboxMore}}},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
		},
		"V2 in the reply continues": {
			replies: []*table.LSTable{withThread(101, v2), page(102, rangeRow(false, -1, 80, 4000))},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
		},
		"V2 on a later frame continues": {
			replies: []*table.LSTable{page(101), page(102, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{{after: 30 * time.Millisecond, tbl: v2}}},
			want:    walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}},
		},
		// Counterexample: V2 has no has-more flag, so V2 rows alone never claim the inbox complete.
		"V2 alone never completes": {
			replies: []*table.LSTable{withThread(101, v2), withThread(102, v2)},
			want:    walkWant{gap: gapInboxIncomplete, stop: 3, pages: 2, refs: [][2]int64{{90, 5000}}},
		},
		// Counterexample: a V2 row for another parent is a folder's, not the inbox's.
		"V2 of another parent": {
			replies: []*table.LSTable{withThread(101, &table.LSTable{LSUpdateThreadsRangesV2: []*table.LSUpdateThreadsRangesV2{v2Row(7, 90, 5000)}})},
			want:    walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1},
		},
		// The sync-group inbox row outranks an inbox-table row of the same reply, as before this change.
		"sync-group row wins": {
			replies: []*table.LSTable{withThread(101, &table.LSTable{
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
			_, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
			p.wg.Wait()
			checkWalk(t, p, res, err, done, tc.want)
		})
	}
}

// (c) No range anywhere within the bound: the same stop as before (stop 2, inbox_incomplete, one page), after
// waiting out the bound. Range rows that cannot answer the page are not taken for one: another parent's, another
// sync group's, one newer than the request's reference, one at exactly the requested position, one sent before the
// page was requested, and one that arrives after the window.
func TestWalkWithoutARangeStopsAsBefore(t *testing.T) {
	const wait = 200 * time.Millisecond
	for name, tc := range map[string]struct {
		before  *table.LSTable // delivered live before the walk starts
		replies []*table.LSTable
		later   [][]laterFrame
		pages   int
		refs    [][2]int64
	}{
		"nothing":              {replies: []*table.LSTable{page(101)}, pages: 1},
		"another parent":       {replies: []*table.LSTable{page(101)}, later: [][]laterFrame{{{after: 20 * time.Millisecond, tbl: rangeOnly(rangeRow(false, 7, 0, 0))}}}, pages: 1},
		"another sync group":   {replies: []*table.LSTable{page(101)}, later: [][]laterFrame{{{after: 20 * time.Millisecond, tbl: &table.LSTable{LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{{SyncGroup: 95, ParentThreadKey: -1}}, LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{{SyncGroup: 95}}}}}}, pages: 1},
		"before the walk":      {before: rangeOnly(rangeRow(false, -1, 0, 0)), replies: []*table.LSTable{page(101)}, pages: 1},
		"after the window":     {replies: []*table.LSTable{page(101)}, later: [][]laterFrame{{{after: 3 * wait, tbl: rangeOnly(rangeRow(false, -1, 0, 0))}}}, pages: 1},
		"newer than reference": {replies: []*table.LSTable{page(101, rangeRow(true, -1, 90, 5000)), page(102)}, later: [][]laterFrame{nil, {{after: 20 * time.Millisecond, tbl: rangeOnly(rangeRow(true, -1, 95, 6000))}}}, pages: 2, refs: [][2]int64{{90, 5000}}},
		"at the reference":     {replies: []*table.LSTable{page(101, rangeRow(true, -1, 90, 5000)), page(102)}, later: [][]laterFrame{nil, {{after: 20 * time.Millisecond, tbl: rangeOnly(rangeRow(true, -1, 90, 5000))}}}, pages: 2, refs: [][2]int64{{90, 5000}}},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, wait)
			s := newTestSession()
			if tc.before != nil {
				s.handleMetaEvent(context.Background(), tc.before)
			}
			p := &framedInbox{s: s, replies: tc.replies, later: tc.later}
			start := time.Now()
			_, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
			took := time.Since(start)
			p.wg.Wait()
			checkWalk(t, p, res, err, done, walkWant{gap: gapInboxIncomplete, stop: 2, pages: tc.pages, refs: tc.refs})
			if took < wait {
				t.Fatalf("walk ended after %v, before the %v wait", took, wait)
			}
			if res.Stats["inbox_pages_late"] != 0 {
				t.Fatalf("a page was decided late: %v", res.Stats)
			}
		})
	}
}

// (c) A cancel during the wait ends the listing with the cancel at once, not after the bound.
func TestRangeWaitHonoursCancel(t *testing.T) {
	withRangeWait(t, time.Minute)
	s := newTestSession()
	ctx, cancel := context.WithCancel(context.Background())
	p := &framedInbox{s: s, replies: []*table.LSTable{page(101)}}
	time.AfterFunc(50*time.Millisecond, cancel)
	_, _, err, done := listGap(t, s, p, ctx, 5*time.Second)
	if !done || !errors.Is(err, context.Canceled) {
		t.Fatalf("done=%v err=%v", done, err)
	}
}

// (d) The final page: a range row that says nothing more, arriving on a later frame, ends the listing complete (no
// gap, stop 0) without another request. The old walk stopped incomplete on that page.
func TestWalkCompletesOnALateEndRow(t *testing.T) {
	for name, end := range map[string]*table.LSTable{
		"inbox parent end": rangeOnly(rangeRow(false, -1, 80, 4000)),
		"parent-0 end":     rangeOnly(rangeRow(false, 0, 0, 0)),
		"inbox table end":  {LSUpsertInboxThreadsRange: []*table.LSUpsertInboxThreadsRange{inboxTableRow(false, 80, 4000)}},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			s := newTestSession()
			p := &framedInbox{s: s,
				replies: []*table.LSTable{page(101, rangeRow(true, -1, 90, 5000)), page(102)},
				later:   [][]laterFrame{nil, {{after: 30 * time.Millisecond, tbl: end}}},
			}
			_, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
			p.wg.Wait()
			checkWalk(t, p, res, err, done, walkWant{gap: "", stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
		})
	}
	// A one-page inbox whose only range is late.
	t.Run("single page", func(t *testing.T) {
		withRangeWait(t, 2*time.Second)
		s := newTestSession()
		p := &framedInbox{s: s, replies: []*table.LSTable{page(101)}, later: [][]laterFrame{{{after: 30 * time.Millisecond, tbl: rangeOnly(rangeRow(false, -1, 0, 0))}}}}
		_, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
		p.wg.Wait()
		checkWalk(t, p, res, err, done, walkWant{gap: "", stop: 0, pages: 1})
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
		p.replies = append(p.replies, page(100+i))
		p.later = append(p.later, []laterFrame{{after: 5 * time.Millisecond, tbl: rangeOnly(rangeRow(true, -1, 90-i, 5000-i))}})
	}
	_, res, err, done := listGap(t, s, p, context.Background(), 5*time.Second)
	p.wg.Wait()
	checkWalk(t, p, res, err, done, walkWant{gap: gapInboxIncomplete, stop: 4, pages: 3, refs: [][2]int64{{90, 5000}, {89, 4999}}})
	s.mu.Lock()
	resume := s.inboxResume
	s.mu.Unlock()
	if resume == nil || resume.threadKey != 88 || resume.activityMS != 4998 {
		t.Fatalf("resume = %+v, want 88/4998", resume)
	}
}

// The watch closes with the walk: a live range row after it adds nothing anywhere.
func TestRangeWatchClosesWithTheWalk(t *testing.T) {
	withRangeWait(t, 100*time.Millisecond)
	s := newTestSession()
	p := &framedInbox{s: s, replies: []*table.LSTable{page(101)}}
	if _, _, err, done := listGap(t, s, p, context.Background(), 5*time.Second); !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rangeWatch != nil {
		t.Fatal("a range watch outlived its walk")
	}
}
