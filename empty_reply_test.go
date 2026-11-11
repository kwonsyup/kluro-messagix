// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/whatsmeow/types/events"
)

// The reply to a page request can be an empty shell: no thread rows and no range row, with the page's threads and
// range arriving together on a later live frame. With an empty reply there is no page floor, so on 8d3a113 every late
// row was rejected and the walk waited out its whole budget, stopping at page 1. These tests pin the change: a late
// more row may then be checked against the oldest thread of its own frame, only on a watch opened after the socket's
// first sync, with every other rule kept, and the wait ends as soon as nothing more can be accepted.

// threadsFrame is a live table holding the given (thread key, activity ms) threads beside rows.
func threadsFrame(threads [][2]int64, rows *table.LSTable) *table.LSTable {
	out := *rows
	out.LSDeleteThenInsertThread = nil
	for _, th := range threads {
		out.LSDeleteThenInsertThread = append(out.LSDeleteThenInsertThread, &table.LSDeleteThenInsertThread{ThreadKey: th[0], ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "T", LastActivityTimestampMs: th[1]})
	}
	return &out
}

// connected is a session after the socket's first sync (the link is up), as every walk after the first connect sees.
func connected(t *testing.T) *messagixSession {
	t.Helper()
	s := newTestSession()
	s.mu.Lock()
	down := s.linkDown
	s.mu.Unlock()
	if down {
		t.Fatal("test session starts with the link down")
	}
	return s
}

// beforeFirstSync is a session between Start and the socket's first connected event, when the library's own
// connect-time thread fetches are answered.
func beforeFirstSync() *messagixSession {
	s := newTestSession()
	s.mu.Lock()
	s.linkDown = true
	s.mu.Unlock()
	return s
}

func wantLate(t *testing.T, stats map[string]int, want map[string]int) {
	t.Helper()
	for k, v := range want {
		if got, ok := stats[k]; !ok || got != v {
			t.Fatalf("stats[%s]=%d (present %v), want %d; %v", k, got, ok, v, stats)
		}
	}
}

var emptyReply = &table.LSTable{}

// An empty reply, then a frame that carries the page's threads and a more row whose minimum is the frame's oldest
// thread: the walk continues to page 2 from that minimum, and page 2's reply ends it. On 8d3a113 every case stopped 2 at
// page 1 after the full budget.
func TestEmptyReplyContinuesFromTheRowsOwnFrame(t *testing.T) {
	page := [][2]int64{{120, 7000}, {95, 6000}, {90, 5000}}
	for name, tc := range map[string]struct {
		frame laterFrame
		next  [2]int64
	}{
		"inbox parent more, after the reply":  {late(30*time.Millisecond, threadsFrame(page, rangeOnly(rangeRow(true, -1, 90, 5000)))), [2]int64{90, 5000}},
		"inbox parent more, before the reply": {late(0, threadsFrame(page, rangeOnly(rangeRow(true, -1, 90, 5000)))), [2]int64{90, 5000}},
		"inbox table more":                    {late(30*time.Millisecond, threadsFrame(page, inboxTable(inboxTableRow(true, 90, 5000)))), [2]int64{90, 5000}},
		"parent-0 more cursor":                {late(30*time.Millisecond, threadsFrame(page, rangeOnly(rangeRow(true, 0, 90, 5000)))), [2]int64{90, 5000}},
		"floor time, newer key":               {late(30*time.Millisecond, threadsFrame(page, rangeOnly(rangeRow(true, -1, 91, 5000)))), [2]int64{91, 5000}},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			s := connected(t)
			p := &framedInbox{s: s,
				replies: []*table.LSTable{emptyReply, tpage(80, 4000, rangeRow(false, -1, 80, 4000))},
				later:   [][]laterFrame{{tc.frame}},
			}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{tc.next}})
			wantLate(t, res.Stats, map[string]int{
				"inbox_pages_late": 1, "inbox_reply_threads": 1, "inbox_late_frames_threads": 1,
				"inbox_late_rejected": 0, "inbox_late_rej_nofloor": 0, "inbox_late_rej_floor": 0, "inbox_late_rej_ceiling": 0, "inbox_late_rej_epoch": 0,
			})
			if took > time.Second {
				t.Fatalf("walk took %v", took)
			}
		})
	}
}

// An empty reply, then a frame with a more row but no thread rows: there is no floor to check the row against, so it
// is never adopted, and since the page's range has come apart from its threads the wait ends at once (stop 2) instead
// of spending the budget. This is the test the tempting shortcut "drop the floor check when the reply is empty"
// fails: it adopts the row ("a deeper row" would skip every thread between the page and that row).
func TestEmptyReplyRowWithoutThreadsIsNotAdopted(t *testing.T) {
	const budget = 2 * time.Second
	for name, row := range map[string]*table.LSTable{
		"at a plausible page end": rangeOnly(rangeRow(true, -1, 90, 5000)),
		"a deeper row":            rangeOnly(rangeRow(true, -1, 10, 100)),
		"inbox table":             inboxTable(inboxTableRow(true, 90, 5000)),
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, budget)
			s := connected(t)
			p := &framedInbox{s: s,
				replies: []*table.LSTable{emptyReply, tpage(80, 4000, rangeRow(false, -1, 80, 4000))},
				later:   [][]laterFrame{{late(20*time.Millisecond, row)}},
			}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
			wantLate(t, res.Stats, map[string]int{
				"inbox_pages_late": 0, "inbox_reply_threads": 0, "inbox_late_frames_threads": 0,
				"inbox_late_rejected": 1, "inbox_late_rej_nofloor": 1, "inbox_late_rej_floor": 0, "inbox_late_rej_ceiling": 0,
			})
			if took > budget/2 || res.Stats["inbox_range_wait_ms"] > 1000 {
				t.Fatalf("walk took %v (waited %d ms): it should stop once the row was rejected", took, res.Stats["inbox_range_wait_ms"])
			}
		})
	}
}

// With an empty reply, a frame's own floor still rejects rows that do not answer the page: a live delta carrying one
// new thread and a cumulative range reaching deeper than it (floor), and a row not older than the request's reference
// (ceiling). Neither ends the wait early; the walk stops 2 at the budget as before.
func TestEmptyReplyFrameFloorRejectsWhatDoesNotAnswerThePage(t *testing.T) {
	const wait = 200 * time.Millisecond
	first := tpage(101, 5000, rangeRow(true, -1, 90, 5000)) // page 1 says more from (5000, 90)
	for name, tc := range map[string]struct {
		frame  *table.LSTable
		reason string
	}{
		"one new thread beside a deep cumulative range":       {threadsFrame([][2]int64{{200, 9000}}, rangeOnly(rangeRow(true, -1, 10, 100))), "inbox_late_rej_floor"},
		"older than its own frame's oldest thread":            {threadsFrame([][2]int64{{85, 4000}, {88, 4000}}, rangeOnly(rangeRow(true, -1, 80, 4000))), "inbox_late_rej_floor"},
		"an earlier page's answer (newer than the reference)": {threadsFrame([][2]int64{{95, 6000}}, rangeOnly(rangeRow(true, -1, 95, 6000))), "inbox_late_rej_ceiling"},
		"at the reference": {threadsFrame([][2]int64{{90, 5000}}, rangeOnly(rangeRow(true, -1, 90, 5000))), "inbox_late_rej_ceiling"},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, wait)
			s := connected(t)
			p := &framedInbox{s: s,
				replies: []*table.LSTable{first, emptyReply, tpage(70, 3000, rangeRow(false, -1, 70, 3000))},
				later:   [][]laterFrame{nil, {late(20*time.Millisecond, tc.frame)}},
			}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}})
			wantLate(t, res.Stats, map[string]int{"inbox_pages_late": 0, "inbox_late_rejected": 1, tc.reason: 1, "inbox_late_rej_nofloor": 0, "inbox_late_frames_threads": 1})
			if took < wait {
				t.Fatalf("walk ended after %v, before the %v budget", took, wait)
			}
		})
	}
}

// Before the socket's first sync the library sends its own thread fetches: the top of the inbox, and a deeper page
// from its saved state. Their answers carry no request id and look exactly like answers to the walk's request, with
// the deep page's threads and a more row at its oldest thread. With an empty reply, a watch opened before the first
// sync never takes a frame's own floor, so neither is adopted (adopting the deep one would skip every thread between
// page 1 and it), and since no row can ever be accepted the wait ends at once. A reply that has its own floor is
// unaffected by the guard.
func TestConnectTimeFetchesBeforeFirstSyncAreNotAdopted(t *testing.T) {
	const budget = 2 * time.Second
	deep := threadsFrame([][2]int64{{12, 150}, {10, 100}}, rangeOnly(rangeRow(true, -1, 10, 100)))
	top := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(rangeRow(true, -1, 90, 5000)))
	two := []*table.LSTable{emptyReply, tpage(80, 4000, rangeRow(false, -1, 80, 4000))}
	t.Run("answered while the request is in flight", func(t *testing.T) {
		withRangeWait(t, budget)
		s := beforeFirstSync()
		p := &framedInbox{s: s, replies: two, later: [][]laterFrame{{late(0, deep), late(0, top)}}}
		res, took := runFramed(t, p)
		checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
		wantLate(t, res.Stats, map[string]int{"inbox_pages_late": 0, "inbox_late_rejected": 2, "inbox_late_rej_nofloor": 2, "inbox_late_frames_threads": 2})
		if took > budget/2 {
			t.Fatalf("walk took %v", took)
		}
	})
	t.Run("answered after the reply: no wait at all", func(t *testing.T) {
		withRangeWait(t, budget)
		s := beforeFirstSync()
		p := &framedInbox{s: s, replies: two, later: [][]laterFrame{{late(30*time.Millisecond, deep)}}}
		res, took := runFramed(t, p)
		checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
		if res.Stats["inbox_pages_late"] != 0 || res.Stats["inbox_range_wait_ms"] > 20 || took > budget/2 {
			t.Fatalf("took %v, stats %v", took, res.Stats)
		}
	})
	t.Run("the same frames after the first sync are judged by their own floor", func(t *testing.T) {
		// Control: the guard is what keeps the deep page out. After the first sync the walk's request is the only
		// inbox thread fetch in flight, and a frame like top answers it.
		withRangeWait(t, budget)
		s := connected(t)
		p := &framedInbox{s: s, replies: two, later: [][]laterFrame{{late(30*time.Millisecond, top)}}}
		res, _ := runFramed(t, p)
		checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
	})
	t.Run("a reply with its own floor behaves as before", func(t *testing.T) {
		withRangeWait(t, budget)
		s := beforeFirstSync()
		p := &framedInbox{s: s,
			replies: []*table.LSTable{tpage(90, 5000), tpage(80, 4000, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{late(30*time.Millisecond, rangeOnly(rangeRow(true, -1, 90, 5000)))}},
		}
		res, _ := runFramed(t, p)
		checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
	})
	t.Run("an empty reply after the first sync with nothing arriving waits the budget", func(t *testing.T) {
		const wait = 200 * time.Millisecond
		withRangeWait(t, wait)
		s := connected(t)
		p := &framedInbox{s: s, replies: []*table.LSTable{emptyReply}}
		res, took := runFramed(t, p)
		checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
		if took < wait {
			t.Fatalf("walk ended after %v, before the %v budget", took, wait)
		}
	})
}

// Late end rows still never conclude when the reply is empty and the frame carries the page's threads: only the reply
// to the walk's own request can say there is nothing more.
func TestEmptyReplyLateEndRowsNeverConclude(t *testing.T) {
	page := [][2]int64{{120, 7000}, {90, 5000}}
	for name, tc := range map[string]struct {
		rows    *table.LSTable
		counted string
	}{
		"inbox end at the frame's floor": {rangeOnly(rangeRow(false, -1, 90, 5000)), "inbox_late_sync_inbox_end"},
		"inbox end with no position":     {rangeOnly(rangeRow(false, -1, 0, 0)), "inbox_late_sync_inbox_end"},
		"parent-0 end":                   {rangeOnly(rangeRow(false, 0, 90, 5000)), "inbox_late_sync_p0_end"},
		"inbox-table end":                {inboxTable(inboxTableRow(false, 90, 5000)), "inbox_late_inbox_end"},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			s := connected(t)
			p := &framedInbox{s: s, replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, threadsFrame(page, tc.rows))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
			wantLate(t, res.Stats, map[string]int{tc.counted: 1, "inbox_pages_late": 0, "inbox_late_rejected": 0})
		})
	}
}

// When the connection epoch changes during the wait (a disconnect, a connect, the encrypted socket's offline sync
// end), the page can no longer be answered by a late row, so the wait ends at once instead of spending the budget. On
// 8d3a113 it waited the whole budget. A candidate row that arrives in the new epoch is counted by reason.
func TestEpochChangeEndsTheWaitAtOnce(t *testing.T) {
	const budget = 3 * time.Second
	answer := rangeOnly(rangeRow(true, -1, 90, 5000))
	for name, change := range map[string]func(s *messagixSession){
		"disconnect":       func(s *messagixSession) { s.handleMetaEvent(context.Background(), disconnect) },
		"connected":        func(s *messagixSession) { s.handleMetaEvent(context.Background(), &messagix.ConnectedEvent{}) },
		"offline sync end": func(s *messagixSession) { s.handleWAEvent(&events.OfflineSyncCompleted{}) },
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, budget)
			s := connected(t)
			var wg sync.WaitGroup
			p := &framedInbox{s: s, replies: []*table.LSTable{tpage(90, 5000), tpage(80, 4000, rangeRow(false, -1, 80, 4000))}}
			p.onRequest = func() {
				if len(p.tasks) != 1 {
					return
				}
				wg.Add(1)
				time.AfterFunc(30*time.Millisecond, func() { defer wg.Done(); change(s) })
			}
			res, took := runFramed(t, p)
			wg.Wait()
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
			if took > budget/2 || res.Stats["inbox_range_wait_ms"] > 1000 {
				t.Fatalf("walk took %v (waited %d ms) after the epoch changed", took, res.Stats["inbox_range_wait_ms"])
			}
		})
	}
	t.Run("a candidate row from the new epoch is counted, never adopted", func(t *testing.T) {
		withRangeWait(t, budget)
		s := connected(t)
		p := &framedInbox{s: s,
			replies: []*table.LSTable{tpage(90, 5000), tpage(80, 4000, rangeRow(false, -1, 80, 4000))},
			later:   [][]laterFrame{{late(0, disconnect), late(0, answer)}},
		}
		res, took := runFramed(t, p)
		checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
		wantLate(t, res.Stats, map[string]int{"inbox_late_rej_epoch": 1, "inbox_late_other_epoch": 1, "inbox_late_rejected": 0, "inbox_pages_late": 0})
		if took > budget/2 {
			t.Fatalf("walk took %v", took)
		}
	})
}
