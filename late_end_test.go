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

// These tests pin stop 6 ("end not confirmed"), the settle after a late end row, the request-id tie counts and the
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

func TestShippedEndSettle(t *testing.T) {
	if endSettle != time.Second {
		t.Fatalf("shipped end settle = %v, want 1s", endSettle)
	}
}

func withEndSettle(t *testing.T, d time.Duration) {
	t.Helper()
	old := endSettle
	endSettle = d
	t.Cleanup(func() { endSettle = old })
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
	"inbox_end6_cursor_sent", "inbox_end6_cursor_other", "inbox_end6_walk_epoch_moved", "inbox_end6_known_gap", "inbox_end6_c1to8", "inbox_end_settle_more", "inbox_end_settle_rejected",
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
// complete, and returns after the settle. With every frame carrying its own page's request_id and page 3's frame its
// task's removeTask row, the tie counts say so, and the walk still stops 6: no decision reads them.
func TestF0LateEndStopsSixNeverComplete(t *testing.T) {
	for name, tied := range map[string]bool{"untied": false, "tied by request and task id": true} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			withEndSettle(t, 100*time.Millisecond)
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
			if took > time.Second {
				t.Fatalf("walk took %v: the end row should end page 3's wait after the settle", took)
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
			withRangeWait(t, time.Second)
			withEndSettle(t, 50*time.Millisecond)
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
		s    func(*testing.T) *messagixSession
		stop int
	}{
		"before first sync": {func(*testing.T) *messagixSession { return beforeFirstSync() }, 2},
		"after first sync":  {connected, 6},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			withEndSettle(t, 50*time.Millisecond)
			p := &framedInbox{s: tc.s(t), replies: []*table.LSTable{tpage(90, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
		})
	}
}

// F3: a database-1 resync marker, then a replayed end row (the replay is not a live frame: no request id), in the same
// epoch. The resync check does not exist in the walk yet, so it is counted beside stop 6, not folded into it.
func TestF3ResyncIsCountedBesideStopSix(t *testing.T) {
	withRangeWait(t, time.Second)
	withEndSettle(t, 50*time.Millisecond)
	marker := tagged{id: 500, ev: withDB1Block(&table.LSTable{}, "c", table.SyncStatusResync)}
	replay := withV2(threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), -1, 90, 5000)
	p := &framedInbox{s: connected(t), ids: f0IDs, replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(10*time.Millisecond, marker), late(30*time.Millisecond, replay)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_resync": 1, "inbox_watch_resync": 1, "inbox_end6_c1to8": 0, "inbox_end6_v2_agree": 1, "inbox_tie_noid": 1, "inbox_tie_req_other": 1})
}

// F4: an epoch change between the send and the end row, or during the end row's settle: the page stops 2.
func TestF4EpochChangeNeverStopsSix(t *testing.T) {
	end := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	for name, later := range map[string][]laterFrame{
		"between the send and the end": {late(10*time.Millisecond, disconnect), late(40*time.Millisecond, end)},
		"during the settle":            {late(20*time.Millisecond, end), late(150*time.Millisecond, disconnect)},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 2*time.Second)
			withEndSettle(t, 600*time.Millisecond)
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
	for name, tc := range map[string]struct {
		frame *table.LSTable
		stop  int
	}{
		"no thread rows":   {rangeOnly(endRow(-1, 90, 5000)), 2},
		"with its threads": {threadsFrame([][2]int64{{95, 6000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), 6},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 200*time.Millisecond)
			withEndSettle(t, 50*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{tpage(90, 5000)}, later: [][]laterFrame{{late(20*time.Millisecond, tc.frame)}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 1})
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
			withEndSettle(t, 50*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{first, emptyReply},
				later: [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame(tc.threads, rangeOnly(endRow(-1, 85, 4000))))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 2, refs: [][2]int64{{90, 5000}}})
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
			withEndSettle(t, 50*time.Millisecond)
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
			withEndSettle(t, 50*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: []*table.LSTable{first, emptyReply},
				later: [][]laterFrame{nil, {late(20*time.Millisecond, threadsFrame(tc.threads, rangeOnly(tc.row)))}}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: tc.stop, pages: 2, refs: [][2]int64{{90, 5000}}})
		})
	}
}

// F9: a more row wins over an end row. In the same frame: one that answers the page continues the walk; one that does
// not leaves the end row unusable (stop 2). During the settle: one that answers the page continues the walk; one that
// does not contradicts the end row (stop 2).
func TestF9MoreRowWinsOverEnd(t *testing.T) {
	pg := [][2]int64{{120, 7000}, {90, 5000}}
	end := threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000)))
	answer := threadsFrame(pg, rangeOnly(moreRow(-1, 90, 5000)))
	stray := threadsFrame([][2]int64{{200, 9000}}, rangeOnly(moreRow(-1, 10, 100)))
	two := []*table.LSTable{emptyReply, tpage(80, 4000, endRow(-1, 80, 4000))}
	for name, tc := range map[string]struct {
		later          []laterFrame
		want           walkWant
		more, rejected int
	}{
		"answering more in the end's frame":     {[]laterFrame{late(20*time.Millisecond, threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000), moreRow(0, 90, 5000))))}, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}}, 0, 0},
		"non-answering more in the end's frame": {[]laterFrame{late(20*time.Millisecond, threadsFrame(pg, rangeOnly(endRow(-1, 90, 5000), moreRow(-1, 10, 100))))}, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1}, 0, 0},
		"answering more during the settle":      {[]laterFrame{late(20*time.Millisecond, end), late(150*time.Millisecond, answer)}, walkWant{stop: 0, pages: 2, refs: [][2]int64{{90, 5000}}}, 1, 0},
		"non-answering more during the settle":  {[]laterFrame{late(20*time.Millisecond, end), late(150*time.Millisecond, stray)}, walkWant{gap: gapInboxIncomplete, stop: 2, pages: 1}, 0, 1},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 500*time.Millisecond)
			withEndSettle(t, 400*time.Millisecond)
			p := &framedInbox{s: connected(t), replies: two, later: [][]laterFrame{tc.later}}
			res, _ := runFramed(t, p)
			checkWalk(t, p, res, tc.want)
			wantLate(t, res.Stats, map[string]int{"inbox_end6_pages": 0, "inbox_end_settle_more": tc.more, "inbox_end_settle_rejected": tc.rejected})
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
			withEndSettle(t, 50*time.Millisecond)
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
	withEndSettle(t, 50*time.Millisecond)
	frame := withV2(threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000))), -1, 91, 5000)
	p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
	res, _ := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	wantLate(t, res.Stats, map[string]int{"inbox_end6_v2_agree": 0, "inbox_end6_v2_disagree": 1, "inbox_end6_c1to8": 0, "inbox_end6_tight": 1})
}

// F12: a frame carrying an earlier page's request_id or task id is counted as the earlier page's, never this one's.
func TestF12TieOfAnotherPageIsNotThePages(t *testing.T) {
	withRangeWait(t, time.Second)
	withEndSettle(t, 50*time.Millisecond)
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

// F13: an end row for the page ends its wait after the settle, not at the end of the budget. 82eada1..d7c4dad spent
// the whole budget.
func TestF13EndEndsTheWait(t *testing.T) {
	withRangeWait(t, 3*time.Second)
	withEndSettle(t, 150*time.Millisecond)
	frame := threadsFrame([][2]int64{{120, 7000}, {90, 5000}}, rangeOnly(endRow(-1, 90, 5000)))
	p := &framedInbox{s: connected(t), replies: []*table.LSTable{emptyReply}, later: [][]laterFrame{{late(20*time.Millisecond, frame)}}}
	res, took := runFramed(t, p)
	checkWalk(t, p, res, walkWant{gap: gapInboxIncomplete, stop: 6, pages: 1})
	if w := res.Stats["inbox_range_wait_ms"]; w < 150 || w > 700 || took > time.Second {
		t.Fatalf("waited %d ms, took %v: want about the settle (170 ms), not the 3 s budget", w, took)
	}
}

// F14: a known open thread between the frame's newest thread and the reference that the frame does not carry is
// counted beside stop 6 (known-thread contiguity is not a walk check yet); an encrypted one is not counted.
func TestF14KnownThreadGapIsCounted(t *testing.T) {
	withRangeWait(t, 200*time.Millisecond)
	withEndSettle(t, 50*time.Millisecond)
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
	withRangeWait(t, time.Second)
	withEndSettle(t, 50*time.Millisecond)
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

// Messages are counted by what the helper reported of their thread when it emitted them (it holds none itself).
func TestEmittedMessagesAreCountedByThreadKind(t *testing.T) {
	s := newTestSession()
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
		LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 48, FolderName: "inbox"}}, // a group-like row with no type
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{
			{ThreadKey: 41, ContactId: 7}, {ThreadKey: 42, ContactId: 7}, {ThreadKey: 43, ContactId: 7}, {ThreadKey: 44, ContactId: 7}, {ThreadKey: 45, ContactId: 7}, {ThreadKey: 46, ContactId: 1},
		},
	}
	s.handleTableFrames(threads)
	s.mu.Lock()
	if th := s.threads["48"]; th != nil {
		th.Kind, th.Typed = "group", false
	}
	s.mu.Unlock()
	var msgs []*table.LSInsertMessage
	for i, key := range []int64{41, 41, 42, 43, 44, 45, 46, 48, 4747, 99} {
		msgs = append(msgs, &table.LSInsertMessage{ThreadKey: key, MessageId: "m" + string(rune('a'+i)), SenderId: 7, TimestampMs: 10})
	}
	s.handleTableFrames(&table.LSTable{LSInsertMessage: msgs})
	// An encrypted message on a marked encrypted thread (its open predecessor is in spam), through the encrypted path.
	s.handleTableFrames(&table.LSTable{
		LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 42, ArmadilloThreadID: 5252}},
	})
	s.waMessageFrames(e2eeEvent(waTypes.NewJID("5252", waTypes.MessengerServer), waTypes.EmptyJID, "W1"))
	s.waMessageFrames(e2eeEvent(waTypes.NewJID("6262", waTypes.MessengerServer), waTypes.EmptyJID, "W2"))
	wantLate(t, s.listingStats(0, 0, 0), map[string]int{
		"emitted_msgs_open": 3, "emitted_msgs_spam": 1, "emitted_msgs_request": 1, "emitted_msgs_marketplace": 1, "emitted_msgs_folder": 1,
		"emitted_msgs_group_untyped": 1, "emitted_msgs_unknown": 1, "emitted_msgs_enc": 2, "emitted_msgs_enc_marked": 1,
	})
}
