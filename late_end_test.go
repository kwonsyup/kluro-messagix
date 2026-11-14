// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	waTypes "go.mau.fi/whatsmeow/types"
)

// These tests pin stop 6 ("end not confirmed"), that an end row changes nothing but the stop code, the request-id tie counts and the
// counts-only diagnostics. Fixture names (F0..F14) follow the walk notes' fixture list; each says what it changes from
// F0, the base case: empty replies after the first sync, one epoch, pages 1 and 2 answered on a later frame by their
// threads, a tight inbox-parent more row and a matching V2 row, and page 3 by its threads, an inbox-parent end row at
// the frame's oldest thread and a matching V2 row.

// tagged is a live table delivered with its frame's request_id, as the library's handleFrame hands it over.
type tagged struct {
	id int64
	ev any
}

type testFrameKey struct{}

// deliver hands ev to the session's event handler as the library would, with a frame request id for a tagged table.
func deliver(s *messagixSession, ctx context.Context, ev any) {
	if tg, ok := ev.(tagged); ok {
		ctx, ev = context.WithValue(ctx, testFrameKey{}, tg.id), tg.ev
	}
	s.handleMetaEvent(ctx, ev)
}

// shippedFrameRequestID is the seam as built; the tests read their own key first, then the library's.
var shippedFrameRequestID = frameRequestID

func init() {
	frameRequestID = func(ctx context.Context) (int64, bool) {
		if id, ok := ctx.Value(testFrameKey{}).(int64); ok {
			return id, true
		}
		return shippedFrameRequestID(ctx)
	}
}

func TestFrameRequestIDIsTheLibrarys(t *testing.T) {
	if reflect.ValueOf(shippedFrameRequestID).Pointer() != reflect.ValueOf(messagix.FrameRequestID).Pointer() {
		t.Fatal("the frame request id seam must be the library's FrameRequestID as built")
	}
	if _, ok := shippedFrameRequestID(context.Background()); ok {
		t.Fatal("a plain context carries no frame request id")
	}
	var _ requestIDPager = tallyPager{} // the walk's wrapper passes the ids through
}

func endRow(parent, key, ts int64) *table.LSUpsertSyncGroupThreadsRange {
	return rangeRow(false, parent, key, ts)
}

func moreRow(parent, key, ts int64) *table.LSUpsertSyncGroupThreadsRange {
	return rangeRow(true, parent, key, ts)
}

// withV2 adds an inbox-folder V2 range row for parent at (key, ts).
func withV2(tbl *table.LSTable, parent, key, ts int64) *table.LSTable {
	out := *tbl
	out.LSUpdateThreadsRangesV2 = append(slices.Clone(out.LSUpdateThreadsRangesV2), v2Table(parent, key, ts).LSUpdateThreadsRangesV2...)
	return &out
}

func withRemoveTask(tbl *table.LSTable, ids ...int64) *table.LSTable {
	out := *tbl
	for _, id := range ids {
		out.LSRemoveTask = append(out.LSRemoveTask, &table.LSRemoveTask{TaskId: id})
	}
	return &out
}

func withDB1Block(tbl *table.LSTable, cursor string, status table.SyncStatus) *table.LSTable {
	out := *tbl
	out.LSExecuteFirstBlockForSyncTransactionV4 = append(out.LSExecuteFirstBlockForSyncTransactionV4, &table.LSExecuteFirstBlockForSyncTransactionV4{DatabaseID: 1, CurrentCursor: cursor, SyncStatus: status})
	return &out
}

var (
	f0Page1 = [][2]int64{{120, 7000}, {100, 6000}}
	f0Page2 = [][2]int64{{95, 5500}, {90, 5000}}
	f0Page3 = [][2]int64{{85, 4500}, {80, 4000}}
)

func f0Frames() [3]*table.LSTable {
	return [3]*table.LSTable{
		withV2(threadsFrame(f0Page1, rangeOnly(moreRow(-1, 100, 6000))), -1, 100, 6000),
		withV2(threadsFrame(f0Page2, rangeOnly(moreRow(-1, 90, 5000))), -1, 90, 5000),
		withV2(threadsFrame(f0Page3, rangeOnly(endRow(-1, 80, 4000))), -1, 80, 4000),
	}
}

var f0IDs = []messagix.RequestIDs{{PacketID: 11, TaskIDs: []int64{21}}, {PacketID: 12, TaskIDs: []int64{22}}, {PacketID: 13, TaskIDs: []int64{23}}}

var f0Refs = [][2]int64{{100, 6000}, {90, 5000}}

// newDiagKeys is every stat key this change adds to threads_done, beside the ones the older tests pin.
var newDiagKeys = []string{
	"inbox_late_sync_other_end", "inbox_late_remove_task", "inbox_late_task_exists", "inbox_late_task_done", "inbox_late_db1_blocks", "inbox_late_cursor_sent", "inbox_watch_resync",
	"inbox_rows_g1_inbox_more", "inbox_rows_g1_inbox_end", "inbox_rows_g1_p0_more", "inbox_rows_g1_p0_end", "inbox_rows_g1_other_more", "inbox_rows_g1_other_end",
	"inbox_reply_remove_task", "inbox_reply_task_exists", "inbox_reply_task_done", "inbox_reply_db1_blocks", "inbox_reply_remove_page",
	"inbox_tie_pages_ids", "inbox_tie_frames", "inbox_tie_noid", "inbox_tie_req_page", "inbox_tie_req_page_range", "inbox_tie_req_page_end", "inbox_tie_req_page_threads",
	"inbox_tie_req_prev", "inbox_tie_req_prev_range", "inbox_tie_req_prev_end", "inbox_tie_req_zero", "inbox_tie_req_other",
	"inbox_tie_remove_page", "inbox_tie_remove_prev", "inbox_tie_remove_other", "inbox_tie_exists_page", "inbox_tie_exists_prev", "inbox_tie_done_page", "inbox_tie_done_prev",
	"inbox_end6_pages", "inbox_end6_tied_req", "inbox_end6_tied_task", "inbox_end6_tight", "inbox_end6_v2_agree", "inbox_end6_v2_disagree", "inbox_end6_resync",
	"inbox_end6_cursor_sent", "inbox_end6_cursor_other", "inbox_end6_walk_epoch_moved", "inbox_end6_known_gap", "inbox_end6_c1to8", "inbox_end6_contradicted",
	"inbox_end_rej_preconnect", "inbox_end_rej_nothreads", "inbox_end_rej_untimed", "inbox_end_rej_newer", "inbox_end_rej_more_in_frame", "inbox_end_rej_position",
	"inbox_end_rej_pos_ceiling", "inbox_end_rej_pos_floor", "inbox_end_rej_pos_nofloor", "inbox_end_rej_min_eq_ref", "inbox_end_rej_min_untimed",
	"inbox_end_rej_threads_below_ref", "inbox_end_rej_v2_agree", "inbox_end_rej_cursor_sent", "inbox_end_rej_last_page",
	"inbox_end_floor_refused", "inbox_floor_continuation_sent", "inbox_floor_continuation_completed",
	"start_tbl_src", "start_tbl_threads", "start_tbl_verify_threads", "start_tbl_g1_inbox_more", "start_tbl_g1_inbox_end", "start_tbl_g1_p0_more", "start_tbl_g1_p0_end",
	"start_tbl_g1_other_more", "start_tbl_g1_other_end", "start_tbl_inbox_more", "start_tbl_inbox_end", "start_tbl_v2_inbox", "start_tbl_v2_other", "start_tbl_ranges_query",
	"start_tbl_sync_blocks", "start_tbl_db1_blocks",
	"emitted_msgs_unknown", "emitted_msgs_enc_marked", "emitted_msgs_enc", "emitted_msgs_spam", "emitted_msgs_request", "emitted_msgs_marketplace", "emitted_msgs_folder",
	"emitted_msgs_group_untyped", "emitted_msgs_open",
}

// withProbeKeys is base plus every new key at 0, then nonzero.
func withProbeKeys(base, nonzero map[string]int) map[string]int {
	out := map[string]int{}
	for k, v := range base {
		out[k] = v
	}
	for _, k := range newDiagKeys {
		out[k] = 0
	}
	for k, v := range nonzero {
		out[k] = v
	}
	return out
}

// F0 without and with a tie: the walk follows the late more rows to page 3, whose late end row stops it 6, never
// complete, after page 3 waited the rest of the walk's budget. With every frame carrying its own page's request_id and page 3's frame its
// task's removeTask row, the tie counts say so, and the walk still stops 6: no decision reads them.
func TestF0LateEndStopsSixNeverComplete(t *testing.T) {
	for name, tied := range map[string]bool{"untied": false, "tied by request and task id": true} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 300*time.Millisecond)
			fr := f0Frames()
			var ev [3]any
			for i := range fr {
				ev[i] = fr[i]
				if tied {
					tbl := fr[i]
					if i == 2 {
						tbl = withRemoveTask(tbl, 23)
					}
					ev[i] = tagged{id: f0IDs[i].PacketID, ev: tbl}
				}
			}
			p := &framedInbox{s: connected(t), ids: f0IDs,
				replies: []*table.LSTable{emptyReply, emptyReply, emptyReply},
				later:   [][]laterFrame{{late(20*time.Millisecond, ev[0])}, {late(20*time.Millisecond, ev[1])}, {late(20*time.Millisecond, ev[2])}},
			}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 3, refs: f0Refs})
			want := map[string]int{
				"inbox_pages_late": 2, "inbox_end6_pages": 1, "inbox_end6_tight": 1, "inbox_end6_v2_agree": 1, "inbox_end6_v2_disagree": 0,
				"inbox_end6_c1to8": 1, "inbox_end6_resync": 0, "inbox_end6_walk_epoch_moved": 0, "inbox_late_sync_inbox_end": 1,
				"inbox_tie_pages_ids": 3, "inbox_tie_frames": 3,
			}
			if tied {
				for k, v := range map[string]int{"inbox_end6_tied_req": 1, "inbox_end6_tied_task": 1, "inbox_tie_req_page": 3, "inbox_tie_req_page_range": 3,
					"inbox_tie_req_page_end": 1, "inbox_tie_req_page_threads": 3, "inbox_tie_remove_page": 1, "inbox_tie_noid": 0, "inbox_tie_req_prev": 0} {
					want[k] = v
				}
			} else {
				for k, v := range map[string]int{"inbox_end6_tied_req": 0, "inbox_end6_tied_task": 0, "inbox_tie_req_page": 0, "inbox_tie_noid": 3} {
					want[k] = v
				}
			}
			wantLate(t, res.Stats, want)
			if w := res.Stats["inbox_range_wait_ms"]; w < 299 || took > 2*time.Second {
				t.Fatalf("waited %d ms in all, took %v: page 3 must wait out the 300 ms budget", w, took)
			}
		})
	}
}

// F1: an end row answering an abandoned, deeper request (the roster refresh's last page, or an earlier walk's) arrives
// during the listing's page 1 or 2. It passes every check the walk has, so the page stops 6: never complete. Its
// request_id is not the page's, and the tie counts show it.
func TestF1StrayDeepEndNeverCompletes(t *testing.T) {
	deep := tagged{id: 7, ev: withV2(threadsFrame([][2]int64{{12, 150}, {10, 100}}, rangeOnly(endRow(-1, 10, 100))), -1, 10, 100)}
	for name, tc := range map[string]struct {
		later [][]laterFrame
		pages int
		refs  [][2]int64
	}{
		"during page 1": {[][]laterFrame{{late(20*time.Millisecond, deep)}}, 1, nil},
		"during page 2": {[][]laterFrame{{late(20*time.Millisecond, f0Frames()[0])}, {late(20*time.Millisecond, deep)}}, 2, f0Refs[:1]},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 300*time.Millisecond)
			p := &framedInbox{s: connected(t), ids: f0IDs, replies: []*table.LSTable{emptyReply, emptyReply, emptyReply}, later: tc.later}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: tc.pages, refs: tc.refs})
			wantLate(t, res.Stats, map[string]int{"inbox_end6_tied_req": 0, "inbox_tie_req_page": 0, "inbox_tie_req_other": 1})
		})
	}
}

// F2: before the socket's first sync the library's own connect-time fetches are answered; a watch opened then never
// takes a late end row, even when the reply has its own floor. The same frames after the first sync stop 6.
func TestF2EndBeforeFirstSyncIsNotTaken(t *testing.T) {
	frame := threadsFrame([][2]int64{{95, 6000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	for name, tc := range map[string]struct {
		s         func(*testing.T) *messagixSession
		stop, rej int
	}{
		"before first sync": {func(*testing.T) *messagixSession { return beforeFirstSync() }, 2, 1},
		"after first sync":  {connected, 6, 0},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: tc.s(t), replies: []*table.LSTable{tpage(90, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
			wantLate(t, res.Stats, map[string]int{"inbox_end_rej_preconnect": tc.rej, "inbox_end_rej_nothreads": 0, "inbox_end_rej_newer": 0, "inbox_end_rej_position": 0})
		})
	}
}

// F3: a database-1 resync marker, then a replayed end row (the replay is not a live frame: no request id), in the same
// epoch. The resync check does not exist in the walk yet, so it is counted beside stop 6, not folded into it.
func TestF3ResyncIsCountedBesideStopSix(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	marker := tagged{id: 500, ev: withDB1Block(&table.LSTable{}, "c", table.SyncStatusResync)}
	replay := withV2(threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), -1, 90, 5000)
	p := &framedInbox{s: connected(t), ids: f0IDs, replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(10*time.Millisecond, marker), late(30*time.Millisecond, replay)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_resync": 1, "inbox_watch_resync": 1, "inbox_end6_c1to8": 0, "inbox_end6_v2_agree": 1, "inbox_tie_noid": 1, "inbox_tie_req_other": 1})
}

// F4: an epoch change between the send and the end row, or after the end row: the wait ends at once, as without an end
// row, and the page stops 2.
func TestF4EpochChangeNeverStopsSix(t *testing.T) {
	end := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	for name, later := range map[string][]laterFrame{
		"between the send and the end": {late(10*time.Millisecond, disconnect), late(40*time.Millisecond, end)},
		"after the end":                {late(20*time.Millisecond, end), late(150*time.Millisecond, disconnect)},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{later}}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
			wantLate(t, res.Stats, map[string]int{"inbox_end6_pages": 0})
			if took > time.Second {
				t.Fatalf("walk took %v after the epoch changed", took)
			}
		})
	}
}

// F5: an end row on a frame without thread rows stops 2, even when the reply's own floor would place it.
func TestF5EndWithoutThreadsIsNotStopSix(t *testing.T) {
	untimed := threadsFrame([][2]int64{{95, 6000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	untimed.LSDeleteThenInsertThread[0].LastActivityTimestampMs = 0
	for name, tc := range map[string]struct {
		frame    *table.LSTable
		stop     int
		rej, key string
	}{
		"no thread rows":        {rangeOnly(endRow(-1, 90, 5000)), 2, "inbox_end_rej_nothreads", ""},
		"a thread with no time": {untimed, 2, "inbox_end_rej_untimed", ""},
		"with its threads":      {threadsFrame([][2]int64{{95, 6000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), 6, "", ""},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{tpage(90, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, tc.frame)}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
			want := map[string]int{"inbox_end_rej_nothreads": 0, "inbox_end_rej_untimed": 0, "inbox_end_rej_preconnect": 0}
			if tc.rej != "" {
				want[tc.rej] = 1
			}
			wantLate(t, res.Stats, want)
		})
	}
}

// F6 and the reference bound: on page 2 (reference (5000, 90)) a frame with a thread newer than the reference stops 2;
// threads at or below it (the boundary thread repeated at exactly the reference included) stop 6.
func TestF6ThreadNewerThanTheReference(t *testing.T) {
	first := tpage(101, 5000, moreRow(-1, 90, 5000))
	for name, tc := range map[string]struct {
		threads [][2]int64
		stop    int
	}{
		"a thread newer than the reference": {[][2]int64{{200, 9000}, {85, 4000}}, 2},
		"threads below the reference":       {[][2]int64{{88, 4500}, {85, 4000}}, 6},
		"the boundary thread repeated":      {[][2]int64{{90, 5000}, {85, 4000}}, 6},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{first, emptyReply},
				later: [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame(tc.threads, rangeOnly(endRow(-1, 85, 4000))))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 2, refs: [][2]int64{{90, 5000}}})
			wantLate(t, res.Stats, map[string]int{"inbox_end_rej_newer": map[int]int{2: 1, 6: 0}[tc.stop], "inbox_end_rej_position": 0})
		})
	}
}

// F7 and the tight count: an end row older than its frame's oldest thread stops 2; one not older than it stops 6, and
// only one at exactly the frame's oldest thread counts as tight (the "minimum equals frame floor" condition is
// counted beside stop 6, not folded into it).
func TestF7FloorAndTight(t *testing.T) {
	threads := [][2]int64{{95, 4800}, {85, 4000}}
	for name, tc := range map[string]struct {
		row         *table.LSUpsertSyncGroupThreadsRange
		stop, tight int
	}{
		"older than the frame's oldest thread": {endRow(-1, 80, 3000), 2, 0},
		"at the frame's oldest thread":         {endRow(-1, 85, 4000), 6, 1},
		"newer than the frame's oldest thread": {endRow(-1, 85, 4500), 6, 0},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, threadsFrame(threads, rangeOnly(tc.row)))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
			wantLate(t, res.Stats, map[string]int{"inbox_end6_tight": tc.tight})
		})
	}
}

// F8 and counterexample 10: on page 2 (reference (5000, 90)) an end row at the reference stops 2 (strict ceiling); one
// a key below it at the same time stops 6 (the key breaks the tie); one a key below the frame's oldest thread at the
// same time stops 2 (floor).
func TestF8CeilingAndKeyTiebreak(t *testing.T) {
	first := tpage(101, 5000, moreRow(-1, 90, 5000))
	for name, tc := range map[string]struct {
		threads [][2]int64
		row     *table.LSUpsertSyncGroupThreadsRange
		stop    int
	}{
		"at the reference":                       {[][2]int64{{90, 5000}}, endRow(-1, 90, 5000), 2},
		"a key below the reference, same time":   {[][2]int64{{90, 5000}, {89, 5000}}, endRow(-1, 89, 5000), 6},
		"a key below the frame floor, same time": {[][2]int64{{90, 5000}, {89, 5000}}, endRow(-1, 88, 5000), 2},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{first, emptyReply},
				later: [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame(tc.threads, rangeOnly(tc.row)))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 2, refs: [][2]int64{{90, 5000}}})
			wantLate(t, res.Stats, map[string]int{"inbox_end_rej_position": map[int]int{2: 1, 6: 0}[tc.stop], "inbox_end_rej_newer": 0})
		})
	}
}

// F9: a more row wins over an end row, as it would without one. In the same frame: one that answers the page continues
// the walk; one that does not leaves the end row unusable (stop 2, counted as more_in_frame). After the end row: one that
// answers the page continues the walk; one that does not changes nothing, and the end row stops the page 6 when the
// budget runs out (stop 6 does not look at other frames' more rows); each such frame after the end row is counted in
// inbox_end6_contradicted, and one before it is not.
func TestF9MoreRowWinsOverEnd(t *testing.T) {
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	end := threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000)))
	answer := threadsFrame(pg, rangeOnly(moreRow(-1, 90, 5000)))
	stray := threadsFrame([][2]int64{{200, 9000}}, rangeOnly(moreRow(-1, 10, 100)))
	two := []*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))}
	for name, tc := range map[string]struct {
		later             []laterFrame
		want              walkWant
		end6, rejM, contr int
	}{
		"answering more in the end's frame":        {[]laterFrame{late(20*time.Millisecond, threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000), moreRow(0, 90, 5000))))}, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}}, 0, 0, 0},
		"non-answering more in the end's frame":    {[]laterFrame{late(20*time.Millisecond, threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000), moreRow(-1, 10, 100))))}, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1}, 0, 1, 0},
		"answering more after the end":             {[]laterFrame{late(20*time.Millisecond, end), late(150*time.Millisecond, answer)}, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}}, 0, 0, 0},
		"non-answering more after the end":         {[]laterFrame{late(20*time.Millisecond, end), late(150*time.Millisecond, stray)}, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1}, 1, 0, 1},
		"two non-answering mores after the end":    {[]laterFrame{late(20*time.Millisecond, end), late(120*time.Millisecond, stray), late(220*time.Millisecond, stray)}, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1}, 1, 0, 2},
		"non-answering more before the end":        {[]laterFrame{late(20*time.Millisecond, stray), late(120*time.Millisecond, end)}, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1}, 1, 0, 0},
		"answering more after a stray and the end": {[]laterFrame{late(20*time.Millisecond, end), late(120*time.Millisecond, stray), late(320*time.Millisecond, answer)}, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}}, 0, 0, 0},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 500*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: two, later: [][]laterFrame{tc.later}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, tc.want)
			wantLate(t, res.Stats, map[string]int{"inbox_end6_pages": tc.end6, "inbox_end_rej_more_in_frame": tc.rejM, "inbox_end6_contradicted": tc.contr})
		})
	}
}

// F10: a parent-0 end, a folder's end, an inbox-table end or a V2 row alone is never an end for the page: stop 2.
func TestF10OtherEndRowsAreNotStopSix(t *testing.T) {
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	for name, rows := range map[string]*table.LSTable{
		"parent-0 end":    rangeOnly(endRow(0, 90, 5000)),
		"folder end":      rangeOnly(endRow(777, 90, 5000)),
		"inbox-table end": inboxTable(inboxTableRow(false, 90, 5000)),
		"V2 only":         v2Table(-1, 90, 5000),
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, threadsFrame(pg, rows))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1})
			wantLate(t, res.Stats, map[string]int{"inbox_end6_pages": 0})
		})
	}
}

// F11: the V2 row disagreeing with the end row is counted beside stop 6 (V2 agreement is not a walk check yet).
func TestF11V2DisagreementIsCounted(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	frame := withV2(threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), -1, 91, 5000)
	p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_v2_agree": 0, "inbox_end6_v2_disagree": 1, "inbox_end6_c1to8": 0, "inbox_end6_tight": 1})
}

// F12: a frame carrying an earlier page's request_id or task id is counted as the earlier page's, never this one's.
func TestF12TieOfAnotherPageIsNotThePages(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	fr := f0Frames()
	p := &framedInbox{s: connected(t), ids: f0IDs,
		replies: []*table.LSTable{emptyReply, emptyReply, emptyReply},
		later: [][]laterFrame{
			{late(20*time.Millisecond, tagged{id: 11, ev: fr[0]})},
			{late(20*time.Millisecond, tagged{id: 11, ev: withRemoveTask(fr[1], 21)})},
			{late(20*time.Millisecond, tagged{id: 12, ev: withRemoveTask(fr[2], 22)})},
		},
	}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 3, refs: f0Refs})
	wantLate(t, res.Stats, map[string]int{
		"inbox_end6_tied_req": 0, "inbox_end6_tied_task": 0, "inbox_tie_req_page": 1, "inbox_tie_req_prev": 2, "inbox_tie_req_prev_range": 2,
		"inbox_tie_req_prev_end": 1, "inbox_tie_remove_page": 0, "inbox_tie_remove_prev": 2, "inbox_tie_req_page_end": 0,
	})
}

// F13: an end row for the page never shortens its wait: the page waits out the budget exactly as it would without the
// end row, and only its stop code differs.
func TestF13EndNeverShortensTheWait(t *testing.T) {
	const budget = 300 * time.Millisecond
	for name, tc := range map[string]struct {
		at   time.Duration
		stop int
	}{
		"end early":                  {20 * time.Millisecond, 6},
		"end just before the budget": {250 * time.Millisecond, 6},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, budget)
			frame := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(tc.at, frame)}}}
			res, took := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
			if w := res.Stats["inbox_range_wait_ms"]; w < 299 || w > 400 || took > time.Second {
				t.Fatalf("waited %d ms, took %v: want the whole 300 ms budget, no more, no less", w, took)
			}
		})
	}
}

// stripEnds is frames with every group-1 inbox-parent end row taken out of their tables.
func stripEnds(frames []laterFrame) []laterFrame {
	out := make([]laterFrame, len(frames))
	for i, f := range frames {
		out[i] = f
		ev, tg := f.ev, tagged{}
		if t, ok := ev.(tagged); ok {
			tg, ev = t, t.ev
		}
		tbl, ok := ev.(*table.LSTable)
		if !ok {
			continue
		}
		cp := *tbl
		cp.LSUpsertSyncGroupThreadsRange = nil
		for _, r := range tbl.LSUpsertSyncGroupThreadsRange {
			if !(r.SyncGroup == 1 && r.ParentThreadKey == inboxParentKey && !r.HasMoreBefore) {
				cp.LSUpsertSyncGroupThreadsRange = append(cp.LSUpsertSyncGroupThreadsRange, r)
			}
		}
		if tg.ev != nil {
			out[i].ev = tagged{id: tg.id, ev: &cp}
		} else {
			out[i].ev = &cp
		}
	}
	return out
}

// An end row changes nothing but the stop code. Across the end-row fixtures, the same walk with every late inbox-parent
// end row removed reaches the same pages with the same requests, waits as long, and continues or stops on the same
// pages; the only difference allowed is stop 6 with the end row where the walk without it stops 2.
func TestEndChangesOnlyTheStopCode(t *testing.T) {
	const budget = 250 * time.Millisecond
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	end := threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000)))
	fr := f0Frames()
	two := []*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))}
	type fixture struct {
		s       func(*testing.T) *messagixSession
		replies []*table.LSTable
		later   [][]laterFrame
	}
	for name, fx := range map[string]fixture{
		"F0":                         {connected, []*table.LSTable{emptyReply, emptyReply, emptyReply}, [][]laterFrame{{late(20*time.Millisecond, fr[0])}, {late(20*time.Millisecond, fr[1])}, {late(20*time.Millisecond, fr[2])}}},
		"F1 stray deep end":          {connected, []*table.LSTable{emptyReply}, [][]laterFrame{{late(20*time.Millisecond, threadsFrame([][2]int64{{12, 150}, {10, 100}}, rangeOnly(endRow(-1, 10, 100))))}}},
		"F2 before first sync":       {func(*testing.T) *messagixSession { return beforeFirstSync() }, []*table.LSTable{tpage(90, 5000)}, [][]laterFrame{{late(20*time.Millisecond, threadsFrame([][2]int64{{95, 6000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))))}}},
		"F4 epoch change after end":  {connected, []*table.LSTable{emptyReply}, [][]laterFrame{{late(20*time.Millisecond, end), late(100*time.Millisecond, disconnect)}}},
		"F5 no thread rows":          {connected, []*table.LSTable{tpage(90, 5000)}, [][]laterFrame{{late(20*time.Millisecond, rangeOnly(endRow(-1, 90, 5000)))}}},
		"F9 answering more after":    {connected, two, [][]laterFrame{{late(20*time.Millisecond, end), late(120*time.Millisecond, threadsFrame(pg, rangeOnly(moreRow(-1, 90, 5000))))}}},
		"F9 stray more after":        {connected, two, [][]laterFrame{{late(20*time.Millisecond, end), late(120*time.Millisecond, threadsFrame([][2]int64{{200, 9000}}, rangeOnly(moreRow(-1, 10, 100))))}}},
		"F9 more in the end's frame": {connected, two, [][]laterFrame{{late(20*time.Millisecond, threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000), moreRow(0, 90, 5000))))}}},
		"F8 ceiling reject":          {connected, []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 5000)), emptyReply}, [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame([][2]int64{{90, 5000}}, rangeOnly(endRow(-1, 90, 5000))))}}},
		"F8 floor reject":            {connected, []*table.LSTable{tpage(101, 5000, moreRow(-1, 90, 5000)), emptyReply}, [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame([][2]int64{{90, 5000}, {89, 5000}}, rangeOnly(endRow(-1, 88, 5000))))}}},
		"F13 end late in the budget": {connected, []*table.LSTable{emptyReply}, [][]laterFrame{{late(200*time.Millisecond, end)}}},
		"no-floor end reject":        {connected, []*table.LSTable{emptyReply}, [][]laterFrame{{late(20*time.Millisecond, rangeOnly(endRow(-1, 90, 5000)))}}},
		"no-floor row after the end": {connected, []*table.LSTable{emptyReply}, [][]laterFrame{{late(20*time.Millisecond, end), late(100*time.Millisecond, rangeOnly(moreRow(-1, 90, 5000)))}}},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, budget)
			run := func(later [][]laterFrame) (listingDone, *framedInbox) {
				p := &framedInbox{s: fx.s(t), replies: fx.replies, later: later}
				res, _ := runFramed(t, p)
				return res, p
			}
			stripped := make([][]laterFrame, len(fx.later))
			for i := range fx.later {
				stripped[i] = stripEnds(fx.later[i])
			}
			with, pw := run(fx.later)
			without, po := run(stripped)
			if !slices.Equal(pw.tasks, po.tasks) {
				t.Fatalf("requests differ:\n with end    %+v\n without end %+v", pw.tasks, po.tasks)
			}
			for _, k := range []string{"inbox_pages", "inbox_pages_late"} {
				if with.Stats[k] != without.Stats[k] {
					t.Fatalf("%s: %d with the end row, %d without", k, with.Stats[k], without.Stats[k])
				}
			}
			ws, os := with.Stats["inbox_stop"], without.Stats["inbox_stop"]
			// Premise of the position-reject fixtures: the end row was rejected on position and the stop stayed 2.
			if strings.HasSuffix(name, "reject") && (with.Stats["inbox_end_rej_position"] != 1 || without.Stats["inbox_end_rej_position"] != 0 || ws != 2) {
				t.Fatalf("premise: position rejects %d/%d, stop %d", with.Stats["inbox_end_rej_position"], without.Stats["inbox_end_rej_position"], ws)
			}
			if ws != os && !(ws == 6 && os == 2) {
				t.Fatalf("stop %d with the end row, %d without: only 6 for 2 may differ", ws, os)
			}
			if with.Gap != without.Gap {
				t.Fatalf("gap %q with the end row, %q without", with.Gap, without.Gap)
			}
			if a, b := with.Stats["inbox_range_wait_ms"], without.Stats["inbox_range_wait_ms"]; a-b > 40 || b-a > 40 {
				t.Fatalf("waited %d ms with the end row, %d ms without", a, b)
			}
		})
	}
}

// F14: a known open thread between the frame's newest thread and the reference that the frame does not carry is
// counted beside stop 6 (known-thread contiguity is not a walk check yet); an encrypted one is not counted.
func TestF14KnownThreadGapIsCounted(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	s := connected(t)
	s.threads["150"] = &threadInfo{Key: 150, Kind: "user", LastActivity: 8000}
	s.threads["e:160"] = &threadInfo{Key: 160, Enc: true, Kind: "user", LastActivity: 8100}
	s.threads["10"] = &threadInfo{Key: 10, Kind: "user", LastActivity: 100} // below the frame: not in the gap
	frame := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	p := &framedInbox{s: s, replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_known_gap": 1})
}

// epochBumpPager bumps the connection epoch just before its second inbox request's watch opens (GetCursor is read
// right before it), so page 2 runs in another epoch than page 1.
type epochBumpPager struct {
	*framedInbox
	reads int
}

func (p *epochBumpPager) GetCursor(int64) string {
	p.reads++
	if p.reads == 2 {
		p.s.handleMetaEvent(context.Background(), &messagix.ConnectedEvent{})
	}
	return ""
}

// The walk's first page's epoch: an end row on a page in a later epoch than page 1's is counted beside stop 6.
func TestWalkEpochMovedIsCounted(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	fr := f0Frames()
	inner := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply, emptyReply},
		later: [][]laterFrame{{late(20*time.Millisecond, fr[0])}, {late(20*time.Millisecond, withV2(threadsFrame(f0Page2, rangeOnly(endRow(-1, 90, 5000))), -1, 90, 5000))}}}
	_, res, err, done := listGap(t, inner.s, &epochBumpPager{framedInbox: inner}, context.Background(), 10*time.Second)
	inner.wg.Wait()
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	checkWalk(t, inner, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 2, refs: f0Refs[:1]})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_walk_epoch_moved": 1, "inbox_end6_c1to8": 0, "inbox_end6_tight": 1, "inbox_end6_v2_agree": 1})
}

// The tie counts classify every live table seen during a page's watch: no request id, the page's, zero, another; and
// the task-table rows naming the page's task id. Without the page's ids nothing is compared.
func TestTieProbeClassifiesLiveTables(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	exists := &table.LSTable{LSTaskExists: []*table.LSTaskExists{{TaskId: 21}}, LSMailboxTaskCompletionApiOnTaskCompletion: []*table.LSMailboxTaskCompletionApiOnTaskCompletion{{TaskId: 21}}}
	later := []laterFrame{
		late(0, tagged{id: 11, ev: withRemoveTask(&table.LSTable{}, 21, 99)}), // before the reply returns
		late(10*time.Millisecond, tagged{id: 0, ev: exists}),
		late(20*time.Millisecond, tagged{id: 4242, ev: &table.LSTable{}}),
		late(30*time.Millisecond, &table.LSTable{}),
		late(40*time.Millisecond, tagged{id: 11, ev: threadsFrame(pg, rangeOnly(moreRow(-1, 90, 5000)))}),
	}
	for name, ids := range map[string][]messagix.RequestIDs{"with ids": f0IDs, "without ids": nil} {
		t.Run(name, func(t *testing.T) {
			p := &framedInbox{s: connected(t), ids: ids, replies: []*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))}, later: [][]laterFrame{later}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
			want := map[string]int{"inbox_tie_frames": 5, "inbox_tie_noid": 1, "inbox_tie_pages_ids": 2, "inbox_tie_req_page": 2, "inbox_tie_req_page_range": 1,
				"inbox_tie_req_page_threads": 1, "inbox_tie_req_zero": 1, "inbox_tie_req_other": 1, "inbox_tie_remove_page": 1, "inbox_tie_remove_other": 1,
				"inbox_tie_exists_page": 1, "inbox_tie_done_page": 1}
			if ids == nil {
				want = map[string]int{"inbox_tie_frames": 5, "inbox_tie_noid": 1, "inbox_tie_pages_ids": 0, "inbox_tie_req_page": 0, "inbox_tie_req_zero": 0,
					"inbox_tie_req_other": 0, "inbox_tie_remove_page": 0, "inbox_tie_remove_other": 0, "inbox_tie_exists_page": 0}
			}
			wantLate(t, res.Stats, want)
		})
	}
}

// The counts-only diagnostics of the replies and of the later range frames: task-table rows, database-1 sync blocks
// and the ones naming the cursor the page was sent with, other-parent end rows, the replies' group-1 rows by parent
// class, and the table names, logged once as one code.
func TestWalkDiagnosticsAreCounted(t *testing.T) {
	withRangeWait(t, 300*time.Millisecond)
	buf := captureCodes(t)
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	reply1 := withDB1Block(withRemoveTask(tpage(90, 5000, endRow(777, 1, 1), moreRow(778, 1, 1)), 21, 5), "c-1", 0)
	reply1.LSTaskExists = []*table.LSTaskExists{{TaskId: 21}}
	lateFrame := withDB1Block(withDB1Block(withRemoveTask(threadsFrame(pg, rangeOnly(endRow(888, 1, 1), moreRow(-1, 90, 5000))), 3), "c-1", 0), "c-0", 0)
	lateFrame.LSMailboxTaskCompletionApiOnTaskCompletion = []*table.LSMailboxTaskCompletionApiOnTaskCompletion{{TaskId: 3}}
	p := &framedInbox{s: connected(t), ids: f0IDs, cursor: "c-1",
		replies: []*table.LSTable{reply1, tpage(80, 4000, endRow(-1, 80, 4000))},
		later:   [][]laterFrame{{late(20*time.Millisecond, lateFrame)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}})
	wantLate(t, res.Stats, map[string]int{
		"inbox_reply_remove_task": 2, "inbox_reply_remove_page": 1, "inbox_reply_task_exists": 1, "inbox_reply_db1_blocks": 1,
		"inbox_rows_g1_other_end": 1, "inbox_rows_g1_other_more": 1, "inbox_rows_g1_p0_more": 0, "inbox_rows_g1_inbox_end": 1, "inbox_rows_g1_inbox_more": 0,
		"inbox_late_remove_task": 1, "inbox_late_task_done": 1, "inbox_late_db1_blocks": 2, "inbox_late_cursor_sent": 1,
		"inbox_late_sync_other": 1, "inbox_late_sync_other_end": 1,
	})
	var tables []string
	for _, c := range codesAt(t, buf, "info") {
		if strings.HasPrefix(c, "inbox_tables.") {
			tables = append(tables, c)
		}
	}
	want := "inbox_tables.r.DeleteThenInsertThread+ExecuteFirstBlockForSyncTransactionV4+RemoveTask+TaskExists+UpsertSyncGroupThreadsRange" +
		".l.DeleteThenInsertThread+ExecuteFirstBlockForSyncTransactionV4+MailboxTaskCompletionApiOnTaskCompletion+RemoveTask+UpsertSyncGroupThreadsRange"
	if len(tables) != 1 || tables[0] != want {
		t.Fatalf("table codes = %v, want [%s]", tables, want)
	}
}

// The start table's kinds: the page load's own table is the one table answering the helper's request, so its range
// rows are counted by parent class and more/end, with its other kinds. A resumed start's reload records them as source 3.
func TestStartTableKindsAreCounted(t *testing.T) {
	s := newTestSession()
	tbl := withDB1Block(withV2(&table.LSTable{
		LSDeleteThenInsertThread:      tpage(90, 5000).LSDeleteThenInsertThread,
		LSVerifyThreadExists:          []*table.LSVerifyThreadExists{{ThreadKey: 5}},
		LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{moreRow(-1, 90, 5000), endRow(0, 1, 1), moreRow(0, 1, 1), endRow(7, 1, 1), endRow(-1, 1, 1), moreRow(7, 1, 1)},
		LSUpsertInboxThreadsRange:     []*table.LSUpsertInboxThreadsRange{inboxTableRow(true, 1, 1), inboxTableRow(false, 1, 1), inboxTableRow(false, 1, 1)},
		LSThreadsRangesQuery:          []*table.LSThreadsRangesQuery{{ParentThreadKey: -1}, {ParentThreadKey: 7}},
	}, -1, 90, 5000), "c", 0)
	tbl.LSExecuteFirstBlockForSyncTransactionV4 = append(tbl.LSExecuteFirstBlockForSyncTransactionV4, &table.LSExecuteFirstBlockForSyncTransactionV4{DatabaseID: 2})
	tbl = withV2(tbl, 7, 1, 1)
	s.setStartKinds(startSrcPageLoad, tbl)
	st := s.listingStats(0, 0, 0)
	wantLate(t, st, map[string]int{
		"start_tbl_src": 1, "start_tbl_threads": 1, "start_tbl_verify_threads": 1,
		"start_tbl_g1_inbox_more": 1, "start_tbl_g1_inbox_end": 1, "start_tbl_g1_p0_more": 1, "start_tbl_g1_p0_end": 1, "start_tbl_g1_other_more": 1, "start_tbl_g1_other_end": 1,
		"start_tbl_inbox_more": 1, "start_tbl_inbox_end": 2, "start_tbl_v2_inbox": 1, "start_tbl_v2_other": 1, "start_tbl_ranges_query": 2,
		"start_tbl_sync_blocks": 2, "start_tbl_db1_blocks": 1,
	})

	r := newReloadRig(t, func(db int) error { return map[bool]error{true: reloadPageErr()}[db == 1] }, func(context.Context) (*table.LSTable, error) {
		return joinTables(pageTable(), rangeOnly(endRow(0, 1, 1))), nil
	})
	if _, err := r.open(context.Background(), savedConfig(t, 123)); err != nil {
		t.Fatal(err)
	}
	wantLate(t, r.s.listingStats(0, 0, 0), map[string]int{"start_tbl_src": 3, "start_tbl_threads": 1, "start_tbl_g1_p0_end": 1, "start_tbl_g1_inbox_end": 0})
	ok := newReloadRig(t, func(int) error { return nil }, nil)
	if _, err := ok.open(context.Background(), savedConfig(t, 123)); err != nil {
		t.Fatal(err)
	}
	wantLate(t, ok.s.listingStats(0, 0, 0), map[string]int{"start_tbl_src": 2, "start_tbl_threads": 2}) // databases 1 and 2
}

// Messages are counted by what the helper reported of their thread when it built them, once they went out (it holds
// none itself): a live message once emit returned its seq, an encrypted one once the connector acked it. A failed emit,
// a frame dropped after a cancel, a frame only built and a failed encrypted delivery are not counted; the redelivery
// that succeeds counts once.
func TestEmittedMessagesAreCountedByThreadKind(t *testing.T) {
	s := newTestSession()
	acks := newAckHub()
	s.acks = acks
	failMsg := "" // the message id whose emit fails
	s.emit = func(f Frame) int64 {
		if f.T == TMessage && f.Message.ID == failMsg {
			return -1
		}
		seq := acks.stamp(&f)
		if f.waitAck {
			acks.ack(seq)
		}
		return seq
	}
	threads := &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			{ThreadKey: 41, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"},
			{ThreadKey: 42, ThreadType: table.ONE_TO_ONE, FolderName: folderSpam},
			{ThreadKey: 43, ThreadType: table.ONE_TO_ONE, FolderName: folderPending},
			{ThreadKey: 44, ThreadType: table.MARKETPLACE, FolderName: folderPending},
			{ThreadKey: 45, ThreadType: table.ONE_TO_ONE, FolderName: "archived"},
			{ThreadKey: 46, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"},
			{ThreadKey: 4747, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE},
		},
		LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 48, FolderName: "inbox"}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{
			{ThreadKey: 41, ContactId: 7}, {ThreadKey: 42, ContactId: 7}, {ThreadKey: 43, ContactId: 7}, {ThreadKey: 44, ContactId: 7}, {ThreadKey: 45, ContactId: 7}, {ThreadKey: 46, ContactId: 1},
		},
	}
	s.handleTable(threads)
	s.mu.Lock()
	if th := s.threads["48"]; th != nil {
		th.Kind, th.Typed = "group", false // a group whose type is not learned
	}
	s.mu.Unlock()
	msg := func(key int64, id string) *table.LSTable {
		return &table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: key, MessageId: id, SenderId: 7, TimestampMs: 10}}}
	}
	for i, key := range []int64{41, 41, 42, 43, 44, 45, 46, 48, 4747, 99} {
		s.handleTable(msg(key, "m"+string(rune('a'+i))))
	}
	// Not counted: a failed emit, a frame only built, a reply frame emitted after a cancel.
	failMsg = "mfail"
	s.handleTable(msg(41, "mfail"))
	s.handleTableFrames(msg(41, "mbuilt"))
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	s.handleReplyEmit(msg(41, "mcancel"), issueMark{epoch: 1}, func(f Frame) int64 {
		if cancelled.Err() != nil {
			return 0
		}
		return s.emit(f)
	})
	// Encrypted: one on a marked thread (its open predecessor is in spam), one plain; then a delivery whose emit fails,
	// redelivered with success.
	s.handleTable(&table.LSTable{LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 42, ArmadilloThreadID: 5252}}})
	if !s.deliverEncrypted(e2eeEvent(waTypes.NewJID("5252", waTypes.MessengerServer), waTypes.EmptyJID, "W1")) ||
		!s.deliverEncrypted(e2eeEvent(waTypes.NewJID("6262", waTypes.MessengerServer), waTypes.EmptyJID, "W2")) {
		t.Fatal("encrypted delivery failed")
	}
	failMsg = "wa:W3"
	if s.deliverEncrypted(e2eeEvent(waTypes.NewJID("6262", waTypes.MessengerServer), waTypes.EmptyJID, "W3")) {
		t.Fatal("a delivery whose emit failed must not be acknowledged")
	}
	failMsg = ""
	if !s.deliverEncrypted(e2eeEvent(waTypes.NewJID("6262", waTypes.MessengerServer), waTypes.EmptyJID, "W3")) {
		t.Fatal("the redelivery failed")
	}
	wantLate(t, s.listingStats(0, 0, 0), map[string]int{
		"emitted_msgs_open": 3, "emitted_msgs_spam": 1, "emitted_msgs_request": 1, "emitted_msgs_marketplace": 1, "emitted_msgs_folder": 1,
		"emitted_msgs_group_untyped": 1, "emitted_msgs_unknown": 1, "emitted_msgs_enc": 3, "emitted_msgs_enc_marked": 1,
	})
}

// An encrypted message whose emit succeeded but whose ack never came (the connector went away) is not counted.
func TestEncryptedMessageWithoutAckIsNotCounted(t *testing.T) {
	s := newTestSession()
	acks := newAckHub()
	s.acks = acks
	var seqs []int64
	s.emit = func(f Frame) int64 { seq := acks.stamp(&f); seqs = append(seqs, seq); return seq } // stamped and written
	acks.close()                                                                                 // the connector is gone: no ack can come
	if s.deliverEncrypted(e2eeEvent(waTypes.NewJID("6262", waTypes.MessengerServer), waTypes.EmptyJID, "W9")) {
		t.Fatal("a delivery without its ack must not be acknowledged")
	}
	if len(seqs) == 0 || seqs[len(seqs)-1] <= 0 {
		t.Fatalf("the message must have been emitted (seqs %v): this test is about the missing ack, not a failed emit", seqs)
	}
	wantLate(t, s.listingStats(0, 0, 0), map[string]int{"emitted_msgs_enc": 0, "emitted_msgs_unknown": 0})
}

// The start's frames go out through the initial emitter; a message among them is counted once its emit returned a seq,
// by the kind its thread had when Start built it, and not when the emit failed.
func TestInitialEmitterCountsEmittedMessages(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:        []*table.LSDeleteThenInsertThread{{ThreadKey: 41, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"}, {ThreadKey: 43, ThreadType: table.ONE_TO_ONE, FolderName: folderPending}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 41, ContactId: 7}, {ThreadKey: 43, ContactId: 7}},
		LSInsertMessage:                 []*table.LSInsertMessage{{ThreadKey: 41, MessageId: "s1", SenderId: 7, TimestampMs: 10}, {ThreadKey: 43, MessageId: "s2", SenderId: 7, TimestampMs: 10}, {ThreadKey: 41, MessageId: "s3", SenderId: 7, TimestampMs: 10}},
	})
	emitInitial := s.initialEmitter(frames)
	if emitInitial == nil {
		t.Fatal("no initial emitter")
	}
	wantLate(t, s.listingStats(0, 0, 0), map[string]int{"emitted_msgs_open": 0, "emitted_msgs_request": 0}) // built, not yet emitted
	var seq int64
	emitInitial(func(f Frame) int64 {
		if f.T == TMessage && f.Message.ID == "s3" {
			return -1
		}
		seq++
		return seq
	})
	wantLate(t, s.listingStats(0, 0, 0), map[string]int{"emitted_msgs_open": 1, "emitted_msgs_request": 1})
}
