// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
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
	if rng, _, _, err := s.awaitInboxRange(context.Background(), w, pos, 0, &counts); err != nil || rng != nil {
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
