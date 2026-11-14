// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// KD-222 diagnostics: the position reject of an inbox end row is split into the three
// judge sub-conditions, and the reject carries the row's minimum against the
// reference, the frame's threads against it, the V2 and cursor facts the stop-6
// counters also record for an accepted end row, and whether the rejecting watch was
// the walk's last requested page's. All counts only; the decisions are unchanged.

// Each case is a position-reject scenario of the F8 shape (or its empty-reply
// analogue): the matching split key counts it, the other two stay 0, and the gap,
// stop, pages and request references are the same stop 2 the position-reject tests
// pin.
func TestKD222PositionRejectIsSplit(t *testing.T) {
	first := tpage(101, 5000, moreRow(-1, 90, 5000))
	for name, tc := range map[string]struct {
		replies []*table.LSTable
		later   [][]laterFrame
		want    walkWant
		key     string
		extra   map[string]int
	}{
		"ceiling": {[]*table.LSTable{first, emptyReply}, [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame([][2]int64{{90, 5000}}, rangeOnly(endRow(-1, 90, 5000))))}},
			walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}}, "inbox_end_rej_pos_ceiling",
			map[string]int{"inbox_end_rej_min_eq_ref": 1}},
		"floor": {[]*table.LSTable{first, emptyReply}, [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame([][2]int64{{90, 5000}, {89, 5000}}, rangeOnly(endRow(-1, 88, 5000))))}},
			walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}}, "inbox_end_rej_pos_floor",
			map[string]int{"inbox_end_rej_threads_below_ref": 1}},
		"no floor": {[]*table.LSTable{emptyReply}, [][]laterFrame{{late(20*time.Millisecond, rangeOnly(endRow(-1, 90, 5000)))}},
			walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1}, "inbox_end_rej_pos_nofloor", nil},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: tc.replies, later: tc.later}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, tc.want)
			want := map[string]int{
				"inbox_end_rej_position":          1,
				"inbox_end_rej_pos_ceiling":       0,
				"inbox_end_rej_pos_floor":         0,
				"inbox_end_rej_pos_nofloor":       0,
				"inbox_end_rej_min_eq_ref":        0,
				"inbox_end_rej_min_untimed":       0,
				"inbox_end_rej_threads_below_ref": 0,
				"inbox_end_rej_v2_agree":          0,
				"inbox_end_rej_cursor_sent":       0,
				"inbox_end_rej_last_page":         1,
				tc.key:                            1,
			}
			for k, v := range tc.extra {
				want[k] = v
			}
			wantLate(t, res.Stats, want)
			if got := res.Stats["inbox_end_rej_pos_ceiling"] + res.Stats["inbox_end_rej_pos_floor"] + res.Stats["inbox_end_rej_pos_nofloor"]; got != res.Stats["inbox_end_rej_position"] {
				t.Fatalf("position rejects %d not covered by the split %d", res.Stats["inbox_end_rej_position"], got)
			}
		})
	}
}

// A position-rejected end row is described by two of the facts the stop-6 counters
// record for an accepted one: an agreeing inbox-parent V2 row beside it and a
// database-1 block naming the cursor the page was sent with.
func TestKD222RejectedEndRowCarriesTheSameFacts(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	frame := withDB1Block(withV2(threadsFrame([][2]int64{{90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), -1, 90, 5000), "c-1", 0)
	p := &framedInbox{s: connected(t), cursor: "c-1",
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 5000)), emptyReply},
		later:   [][]laterFrame{nil, {late(20*time.Millisecond, frame)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 5000}}})
	wantLate(t, res.Stats, map[string]int{
		"inbox_end_rej_position": 1, "inbox_end_rej_pos_ceiling": 1,
		"inbox_end_rej_v2_agree": 1, "inbox_end_rej_cursor_sent": 1,
	})
}

// inbox_end_rej_last_page counts only the position-rejected end rows of the walk's
// last requested page: once the walk moved past a page, its rejects are not the last
// page's. The watch/await seam the budget tests use drives the two pages' watches
// directly with no wait budget, so the reject drains on the first watch and the
// second watch stands in for the page the walk moved on to.
func TestKD222LastPageCountsOnlyTheLastWatch(t *testing.T) {
	s := connected(t)
	pos := pagePosition{
		ref:         cursorPos{ts: 5000, key: 90},
		floor:       cursorPos{ts: 4000, key: 80},
		floorOK:     true,
		postConnect: true,
	}
	var counts inboxRowCounts
	w := s.openWatch("", &counts)
	s.handleMetaEvent(context.Background(), threadsFrame([][2]int64{{90, 5000}}, rangeOnly(endRow(-1, 90, 5000))))
	if rng, _, _, err := s.awaitInboxRange(context.Background(), w, pos, 0, &counts, false); err != nil || rng != nil {
		t.Fatalf("rejecting page: rng=%+v err=%v", rng, err)
	}
	s.closeWatch(w, &counts)
	// The last page's watch is only opened and closed, like a page whose reply decided the
	// range: the tally must restart when the watch opens, not when a wait starts.
	w = s.openWatch("", &counts)
	s.closeWatch(w, &counts)
	stats := map[string]int{}
	counts.end6.addStats(stats)
	wantLate(t, stats, map[string]int{
		"inbox_end_rej_position": 1, "inbox_end_rej_pos_ceiling": 1, "inbox_end_rej_last_page": 0,
	})
}

// The live shape the split counters reported: on the walk's last page the reply is
// empty and a late frame carries the page's threads, an inbox-parent end row whose
// minimum is below the frame's oldest thread (the floor is the frame's own), and an
// inbox-parent V2 row agreeing with the end row's minimum. The frame is a
// candidate, not an answer: the walk asks the server once more, from the page's
// oldest delivered thread — the frame's own — with its own request and cursor.
// An empty bound reply (no threads below the reference) is the server's own
// answer that the range ended, and the walk ends complete. Every thread the end
// row's frame carried is listed, including the one below the page's reference.
func TestKD222LastPageFloorEndCompletesTheWalk(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	s := connected(t)
	var emitted []Frame
	s.emit = func(f Frame) int64 { emitted = append(emitted, f); return 0 }
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	p := &framedInbox{s: s, cursor: "c-1",
		replies: []*table.LSTable{
			tpage(101, 5000, moreRow(-1, 95, 4800)),
			tpage(94, 4700, moreRow(-1, 90, 4600)),
			emptyReply,
			emptyReply, // the continuation: nothing below the frame's oldest thread
		},
		later: [][]laterFrame{nil, nil, {late(20*time.Millisecond, end)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{stop: 0, pages: 4, refs: [][2]int64{{95, 4800}, {90, 4600}, {88, 4400}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 1, "inbox_floor_continuation_completed": 1, "inbox_end_floor_refused": 0})
	if task := p.tasks[3]; task.Cursor != "c-1" {
		t.Fatalf("the continuation request's cursor = %q, want the walk's own %q", task.Cursor, "c-1")
	}
	listed := threadsByID(emitted)
	for _, th := range [][2]int64{{89, 4500}, {88, 4400}} {
		if f := listed[strconv.FormatInt(th[0], 10)]; f == nil || f.LastActivity != th[1] {
			t.Fatalf("thread %v of the end row's frame is not listed: %v", th, listed)
		}
	}
}

// The same continuation ends complete when its bound reply delivers what the end
// row's minimum had skipped — thread 87 below the frame's oldest thread — and a
// genuine end row beside it.
func TestKD222FloorEndContinuationReplyEndCompletesTheWalk(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	s := connected(t)
	var emitted []Frame
	s.emit = func(f Frame) int64 { emitted = append(emitted, f); return 0 }
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	p := &framedInbox{s: s,
		replies: []*table.LSTable{
			tpage(101, 5000, moreRow(-1, 90, 4600)),
			emptyReply,
			tpage(87, 4350, endRow(-1, 85, 4300)),
		},
		later: [][]laterFrame{nil, {late(20 * time.Millisecond, end)}},
	}
	frames, res, err, done := listGap(t, s, p, context.Background(), 10*time.Second)
	p.wg.Wait()
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	checkWalk(t, p, res, walkWant{stop: 0, pages: 3, refs: [][2]int64{{90, 4600}, {88, 4400}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 1, "inbox_floor_continuation_completed": 1})
	// The late frame's threads come through the live emit; the continuation
	// reply's through the walk's.
	listed := threadsByID(append(frames, emitted...))
	for _, th := range [][2]int64{{89, 4500}, {88, 4400}, {87, 4350}} {
		if f := listed[strconv.FormatInt(th[0], 10)]; f == nil || f.LastActivity != th[1] {
			t.Fatalf("thread %v is not listed: %v", th, listed)
		}
	}
}

// An untagged stale or replayed floor-end frame can satisfy every positional
// binding; it still cannot complete the walk — it only earns one bound
// continuation request. When that request answers with a thread the stale end
// row had skipped but no deciding range, the walk stops 2, truthfully.
func TestKD222StaleFloorEndOnlyEarnsAContinuation(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply, tpage(87, 4350)},
		later:   [][]laterFrame{nil, {late(20 * time.Millisecond, end)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 3, refs: [][2]int64{{90, 4600}, {88, 4400}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 1, "inbox_floor_continuation_completed": 0})
}

// Only one continuation: the continuation page can itself carry a floor-end
// candidate, but it is refused — the walk stops 2 and sends no fourth request.
func TestKD222FloorEndContinuationDoesNotRecurse(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	end1 := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	end2 := withV2(threadsFrame([][2]int64{{86, 4300}}, rangeOnly(endRow(-1, 84, 4200))), -1, 84, 4200)
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply, tpage(87, 4350)},
		later:   [][]laterFrame{nil, {late(20 * time.Millisecond, end1)}, {late(20 * time.Millisecond, end2)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 3, refs: [][2]int64{{90, 4600}, {88, 4400}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 1, "inbox_floor_continuation_completed": 0, "inbox_end_floor_refused": 1})
}

// A floor-end candidate on an earlier page changes nothing on its own — a more
// row that answers the page still continues the walk past it.
func TestKD222FloorEndOnAnEarlierPageDoesNotComplete(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	end := withV2(threadsFrame([][2]int64{{120, 7000}, {119, 6900}}, rangeOnly(endRow(-1, 100, 5000))), -1, 100, 5000)
	answer := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(moreRow(-1, 90, 5000)))
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))},
		later:   [][]laterFrame{{late(20*time.Millisecond, end), late(150*time.Millisecond, answer)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
}

// The floor alone rejects: without an inbox-parent V2 row agreeing with the end
// row's minimum — none, or one at another minimum — the end still cannot be taken
// and the walk stays incomplete.
func TestKD222FloorEndWithoutV2AgreementStaysIncomplete(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	threads := [][2]int64{{89, 4500}, {88, 4400}}
	for name, frame := range map[string]*table.LSTable{
		"no V2 row":                   threadsFrame(threads, rangeOnly(endRow(-1, 85, 4300))),
		"a V2 row at another minimum": withV2(threadsFrame(threads, rangeOnly(endRow(-1, 85, 4300))), -1, 84, 4300),
	} {
		t.Run(name, func(t *testing.T) {
			p := &framedInbox{s: connected(t),
				replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply},
				later:   [][]laterFrame{nil, {late(20*time.Millisecond, frame)}},
			}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 4600}}})
			wantLate(t, res.Stats, map[string]int{"inbox_end_rej_pos_floor": 1, "inbox_end_rej_v2_agree": 0})
		})
	}
}

// A position failure other than the floor is not rescued by V2 agreement: an end
// row at the page's reference (the ceiling) with an agreeing V2 row is still no
// end for the page.
func TestKD222CeilingEndWithV2AgreementStaysIncomplete(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	frame := withV2(threadsFrame([][2]int64{{90, 4600}}, rangeOnly(endRow(-1, 90, 4600))), -1, 90, 4600)
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply},
		later:   [][]laterFrame{nil, {late(20*time.Millisecond, frame)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 4600}}})
	wantLate(t, res.Stats, map[string]int{"inbox_end_rej_pos_ceiling": 1, "inbox_end_rej_v2_agree": 1})
}

// An end row with no floor to be judged against — a frame without thread rows — is
// never taken, V2 agreement or not.
func TestKD222EndWithoutFloorStaysIncomplete(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	frame := withV2(rangeOnly(endRow(-1, 85, 4300)), -1, 85, 4300)
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{emptyReply},
		later:   [][]laterFrame{{late(20*time.Millisecond, frame)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end_rej_pos_nofloor": 1, "inbox_end_rej_nothreads": 1})
}

// A floor-only, V2-agreeing end row still cannot be taken when the frame is not
// bound to the page it would end: on the first page the walk has no walked
// frontier for the frame to continue — the end row could be any older range state
// replayed on the live path — and on a descended page a stray deeper end whose
// threads are not this page's continuation leaves a gap of known threads between
// the frame's newest and the reference.
func TestKD222FloorEndOnAnUnboundPageStaysIncomplete(t *testing.T) {
	first := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	stray := withV2(threadsFrame([][2]int64{{12, 150}, {10, 100}}, rangeOnly(endRow(-1, 5, 50))), -1, 5, 50)
	for name, tc := range map[string]struct {
		replies []*table.LSTable
		later   [][]laterFrame
		seed    bool
		pages   int
		refs    [][2]int64
	}{
		"the first page has no frontier": {[]*table.LSTable{emptyReply}, [][]laterFrame{{late(20 * time.Millisecond, first)}}, false, 1, nil},
		"a stray deep end on page two":   {[]*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply}, [][]laterFrame{nil, {late(20 * time.Millisecond, stray)}}, true, 2, [][2]int64{{90, 4600}}},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			s := connected(t)
			if tc.seed {
				s.threads["50"] = &threadInfo{Key: 50, Kind: "user", LastActivity: 3000}
			}
			p := &framedInbox{s: s, replies: tc.replies, later: tc.later}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: tc.pages, refs: tc.refs})
			wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 0, "inbox_end_floor_refused": 1})
		})
	}
}

// A floor-end frame that identifies itself as another request's — the frame
// request_id of an earlier page — can never answer this page, V2 agreement or not.
func TestKD222FloorEndTaggedToAnotherRequestStaysIncomplete(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	p := &framedInbox{s: connected(t), ids: f0IDs,
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply},
		later:   [][]laterFrame{nil, {late(20*time.Millisecond, tagged{id: f0IDs[0].PacketID, ev: end})}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 4600}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 0, "inbox_end_floor_refused": 1, "inbox_tie_req_prev": 1})
}

// The bindings that are checked at the wait's end: the whole walk must run in one
// connection epoch, the page's watch must have seen no database-1 resync marker
// (a replayed end row is not the page's answer), and no more row may stand after
// the end row — a further cursor contradicts the end.
func TestKD222FloorEndNeedsAWholeWalkEpoch(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	fr := f0Frames()
	inner := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply, emptyReply},
		later: [][]laterFrame{
			{late(20*time.Millisecond, fr[0])},
			{late(20 * time.Millisecond, withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300))},
		}}
	_, res, err, done := listGap(t, inner.s, &epochBumpPager{framedInbox: inner}, context.Background(), 10*time.Second)
	inner.wg.Wait()
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	checkWalk(t, inner, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: f0Refs[:1]})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 0, "inbox_end_floor_refused": 1})
}

func TestKD222FloorEndAfterResyncStaysIncomplete(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	marker := withDB1Block(&table.LSTable{}, "c", table.SyncStatusResync)
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply},
		later:   [][]laterFrame{nil, {late(10 * time.Millisecond, marker), late(30 * time.Millisecond, end)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 4600}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 0, "inbox_end_floor_refused": 1, "inbox_watch_resync": 1})
}

func TestKD222FloorEndWithALaterCursorStaysIncomplete(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	end := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	stray := threadsFrame([][2]int64{{200, 9000}}, rangeOnly(moreRow(-1, 10, 100)))
	p := &framedInbox{s: connected(t),
		replies: []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply},
		later:   [][]laterFrame{nil, {late(20 * time.Millisecond, end), late(120 * time.Millisecond, stray)}},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 2, refs: [][2]int64{{90, 4600}}})
	wantLate(t, res.Stats, map[string]int{"inbox_floor_continuation_sent": 0, "inbox_end_floor_refused": 1})
}

// The roster refresh shares walkInbox: a bound end row earns it the same
// continuation, whose empty bound reply ends the walk complete — its stop code
// only feeds the refresh log line, and the folder fallback is decided by its own
// target check, not the stop — while an end row the walk cannot bind (the first
// page's) stays incomplete.
func TestKD222FloorEndAndTheRosterRefresh(t *testing.T) {
	target := func(s *messagixSession) {
		s.threads["7"] = &threadInfo{Key: 7, Typed: true, Kind: "group", MemberCount: 2, LastActivity: 8000}
	}
	bound := withV2(threadsFrame([][2]int64{{89, 4500}, {88, 4400}}, rangeOnly(endRow(-1, 85, 4300))), -1, 85, 4300)
	for name, tc := range map[string]struct {
		replies []*table.LSTable
		later   [][]laterFrame
		code    string
	}{
		// The page count includes the one request-scope page scanScopes always adds.
		"a bound end completes the refresh walk": {[]*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 4600)), emptyReply, emptyReply}, [][]laterFrame{nil, {late(20 * time.Millisecond, bound)}}, "roster_refresh.p4.s0.m1"},
		"the first page's end stays incomplete":  {[]*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))}, [][]laterFrame{{late(20 * time.Millisecond, bound)}}, "roster_refresh.p2.s2.m1"},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			buf := captureCodes(t)
			s := connected(t)
			target(s)
			p := &framedInbox{s: s, replies: tc.replies, later: tc.later}
			s.rosterRefresh(context.Background(), p, func(Frame) int64 { return 0 })
			p.wg.Wait()
			var codes []string
			for _, c := range codesAt(t, buf, "info") {
				if strings.HasPrefix(c, "roster_refresh.") {
					codes = append(codes, c)
				}
			}
			if len(codes) != 1 || codes[0] != tc.code {
				t.Fatalf("roster_refresh codes = %v, want [%s]", codes, tc.code)
			}
		})
	}
}
