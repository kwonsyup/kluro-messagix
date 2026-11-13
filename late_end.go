// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// On some accounts the reply to a thread-list request carries no rows: the page's
// result arrives as a delta on database 1's sync stream, and the library hands the
// helper that delta as a table only. This file holds what the walk does with an end
// row on such a delta (stop 6, "end not confirmed", never complete), and the counts
// that say whether the server ties the delta to the walk's request: the request's
// packet and task ids (from the library's ExecuteTasksWithIDs) against the request_id
// of each live frame (FrameRequestID) and the task-table rows it carries. No decision
// reads those counts.

// requestIDPager is a threadPager that also reports the ids its request was sent with.
// The library's client is one; executeWithIDs falls back to ExecuteTasks otherwise.
type requestIDPager interface {
	ExecuteTasksWithIDs(ctx context.Context, tasks ...socket.Task) (*table.LSTable, messagix.RequestIDs, error)
}

var _ requestIDPager = (*messagix.Client)(nil)

// executeWithIDs sends tasks through cli, with the request's ids when cli reports them.
// It is the same request either way.
func executeWithIDs(ctx context.Context, cli threadPager, tasks ...socket.Task) (*table.LSTable, messagix.RequestIDs, error) {
	if p, ok := cli.(requestIDPager); ok {
		return p.ExecuteTasksWithIDs(ctx, tasks...)
	}
	tbl, err := cli.ExecuteTasks(ctx, tasks...)
	return tbl, messagix.RequestIDs{}, err
}

// frameRequestID reads the request_id of the live frame a table came on, from the
// context the library hands the event handler (a test seam).
var frameRequestID = messagix.FrameRequestID

// liveTag is a live table's frame request_id; ok is false for a table that did not come
// on a live frame (the library's sync replays, or a table the helper applies itself).
type liveTag struct {
	id int64
	ok bool
}

func tagOf(ctx context.Context) liveTag {
	id, ok := frameRequestID(ctx)
	return liveTag{id: id, ok: ok}
}

// handleLiveTable is handleTable for a table from the library's event handler, with its
// frame's tag: the open range watch records it for the tie counts, and the watch's
// offer keeps it with the frame's range rows.
func (s *messagixSession) handleLiveTable(tbl *table.LSTable, tag liveTag) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w := s.rangeWatch; w != nil {
		w.see(tbl, tag, s.connEpoch == w.epoch)
	}
	s.liveTag = tag
	frames := s.buildTableFramesLocked(tbl, s.connEpoch, nil)
	s.liveTag = liveTag{}
	for _, f := range frames {
		s.countEmittedLocked(f, s.emit(f))
	}
}

// liveSeen is what the tie counts keep of one live table: its tag, whether it carried
// a group-1 range row, a group-1 inbox-parent end row, thread rows, and the task ids of
// its task-table rows.
type liveSeen struct {
	tag                   liveTag
	ranges, end, threads  bool
	remove, exists, doneT []int64
}

// see records a live table for the tie counts, and a database-1 resync marker that
// arrives in the watch's epoch. The caller holds s.mu.
func (w *rangeWatch) see(tbl *table.LSTable, tag liveTag, sameEpoch bool) {
	r := rangeRowsOf(tbl)
	ls := liveSeen{tag: tag, ranges: len(r.sync)+len(r.inbox) > 0, threads: threadRowCount(tbl) > 0}
	for _, row := range r.sync {
		if row.ParentThreadKey == inboxParentKey && !row.HasMoreBefore {
			ls.end = true
		}
	}
	for _, t := range tbl.LSRemoveTask {
		ls.remove = append(ls.remove, t.TaskId)
	}
	for _, t := range tbl.LSTaskExists {
		ls.exists = append(ls.exists, t.TaskId)
	}
	for _, t := range tbl.LSMailboxTaskCompletionApiOnTaskCompletion {
		ls.doneT = append(ls.doneT, t.TaskId)
	}
	w.seen = append(w.seen, ls)
	if !sameEpoch {
		return
	}
	for _, b := range tbl.GetLSExecuteFirstBlockForSyncTransactionV4() {
		if b.DatabaseID == 1 && b.SyncStatus == table.SyncStatusResync {
			w.resync = true
			w.counts.resync++
		}
	}
}

// taskIDsOf is the task ids of a table's LSRemoveTask rows.
func taskIDsOf(tbl *table.LSTable) (ids []int64) {
	for _, t := range tbl.LSRemoveTask {
		ids = append(ids, t.TaskId)
	}
	return ids
}

// frameThreads is the newest position among a table's thread rows (in the two tables
// pageFloor reads), how many there are, whether any has no time, and their keys.
func frameThreads(tbl *table.LSTable) (newest cursorPos, n int, untimed bool, keys map[int64]bool) {
	keys = map[int64]bool{}
	see := func(key, ts int64) {
		n++
		keys[key] = true
		if ts <= 0 {
			untimed = true
			return
		}
		if p := (cursorPos{ts: ts, key: key}); n == 1 || newest.olderThan(p) {
			newest = p
		}
	}
	for _, th := range tbl.LSDeleteThenInsertThread {
		see(th.ThreadKey, th.LastActivityTimestampMs)
	}
	for _, th := range tbl.LSUpdateOrInsertThread {
		see(th.ThreadKey, th.LastActivityTimestampMs)
	}
	return newest, n, untimed, keys
}

// endForPage returns the end row of frame f that is one for this page, or nil. Stop 6
// is defined by exactly the conditions named for it in this build, and no others. Some
// are checks the walk already makes for a more row (the epoch, the ceiling and floor,
// the more rows); the others are new, for end rows only (after the first sync for any
// reply, and the thread-row conditions):
//   - the frame is in the watch's connection epoch (offerLocked keeps no other);
//   - the watch was opened after the socket's first sync;
//   - the row is a group-1 sync-group row for the inbox parent (-1) with no more before:
//     not parent 0, not another parent, not the inbox table, not V2;
//   - the frame carries thread rows, every one with a time and none newer than the
//     page's reference;
//   - the row's minimum is strictly older than the reference and not older than the
//     page's floor (pagePosition.judge);
//   - the frame has no more row for the inbox parent or parent 0, and no inbox-table
//     more row.
//
// An end row that fails one of them is counted by each condition it fails (see
// end6Counts.reject). What the walk does not check (a resync marker, the V2 row
// agreeing, the cursor order, the known threads above the frame, the epoch of the
// walk's first page) is counted beside stop 6, never folded into it.
func (p pagePosition) endForPage(f lateFrame) *table.LSUpsertSyncGroupThreadsRange {
	row, why := p.endCheck(f)
	if why != (endRejects{}) {
		return nil
	}
	return row
}

// endRejects is which stop-6 conditions an inbox-parent end row's frame fails.
type endRejects struct {
	preConnect, noThreads, untimed, newer, moreInFrame, position bool
}

// endCheck returns the frame's first group-1 inbox-parent end row (nil if it has none)
// and the stop-6 conditions it fails. With several such rows, a row that passes the
// position check is preferred.
func (p pagePosition) endCheck(f lateFrame) (*table.LSUpsertSyncGroupThreadsRange, endRejects) {
	var row *table.LSUpsertSyncGroupThreadsRange
	var why endRejects
	why.position = true
	for _, r := range f.rows.sync {
		if r.ParentThreadKey != inboxParentKey || r.HasMoreBefore {
			continue
		}
		if row == nil {
			row = r
		}
		if p.judge(cursorPos{ts: r.MinLastActivityTimestampMS, key: r.MinThreadKey}, f) == accepted {
			row, why.position = r, false
			break
		}
	}
	if row == nil {
		return nil, endRejects{}
	}
	why.preConnect = !p.postConnect
	why.noThreads = f.threads == 0
	why.untimed = f.untimed
	why.newer = f.threads > 0 && p.ref.olderThan(f.newest)
	why.moreInFrame = candidateCount(f.rows) > 0
	return row, why
}

// end6Counts is what was counted when a page stopped 6: pages is how many did; the
// rest is beside it. tiedReq: the end frame's request_id is the page's packet id;
// tiedTask: it carries an LSRemoveTask row for the page's task id; tight: the end row's
// minimum is the frame's oldest thread; v2Agree, v2Disagree: an inbox-parent V2 row in
// the frame has, or none of them has, the same minimum; resync: a database-1 resync
// marker arrived while the page's watch was open; cursorSent, cursorOther: the frame's
// database-1 block names the cursor the page was sent with, or another one;
// walkEpochMoved: the epoch is not the walk's first page's; knownGap: known threads
// strictly between the frame's newest thread and the reference that the frame does not
// carry (folder threads count too); c1to8: tight, V2 agrees, no resync and the walk's
// epoch, together. The rej counts are the frames whose inbox-parent end row failed a
// stop-6 condition, by each condition it failed (one frame can count under several).
// For a frame whose end row the position check rejected, the rest says which
// sub-condition of judge did it (their sum is always rejPosition), how the row's
// minimum and the frame's threads sit against the page's reference, the same three
// facts count records for an accepted end row, and whether the rejecting watch was
// the walk's last requested page's.
type end6Counts struct {
	pages, tiedReq, tiedTask, tight, v2Agree, v2Disagree, resync int
	cursorSent, cursorOther, walkEpochMoved, knownGap, c1to8     int
	rejPreConnect, rejNoThreads, rejUntimed, rejNewer            int
	rejMoreInFrame, rejPosition                                  int
	rejPosCeiling, rejPosFloor, rejPosNoFloor                    int
	rejMinEqRef, rejMinUntimed, rejThreadsBelowRef               int
	rejV2Agree, rejCursorSent, rejKnownGap, rejLastPage          int
	contradicted                                                 int // frames after a stop-6 end row whose more rows did not answer the page
}

// reject counts the stop-6 conditions that frame f's inbox-parent end row fails, if it
// has one and it fails any. For an end row the position check rejected it also counts
// which judge sub-condition did it (judge returns the first failing one in its own
// order, so the three split counts always sum to the position rejects), the row's
// minimum against the page's reference and the frame's oldest thread against it, and
// the same three facts count records for an accepted end row. The watch keeps its own
// tally (w.endRejPos) so closeWatch can keep only the last requested page's rejects.
func (c *end6Counts) reject(s *messagixSession, w *rangeWatch, pos pagePosition, f lateFrame) {
	row, why := pos.endCheck(f)
	if row == nil {
		return
	}
	c.rejPreConnect += b2i(why.preConnect)
	c.rejNoThreads += b2i(why.noThreads)
	c.rejUntimed += b2i(why.untimed)
	c.rejNewer += b2i(why.newer)
	c.rejMoreInFrame += b2i(why.moreInFrame)
	c.rejPosition += b2i(why.position)
	if !why.position {
		return
	}
	lo := cursorPos{ts: row.MinLastActivityTimestampMS, key: row.MinThreadKey}
	switch pos.judge(lo, f) {
	case rejCeiling:
		c.rejPosCeiling++
	case rejFloor:
		c.rejPosFloor++
	default: // rejNoFloor; judge cannot return accepted here, why.position says it failed
		c.rejPosNoFloor++
	}
	c.rejMinEqRef += b2i(lo == pos.ref)
	c.rejMinUntimed += b2i(lo.ts == 0)
	c.rejThreadsBelowRef += b2i(f.floorOK && f.floor.olderThan(pos.ref))
	s.mu.Lock()
	_, agree, sent, gap := s.endFacts(w, pos, f, lo)
	w.endRejPos++
	s.mu.Unlock()
	c.rejV2Agree += b2i(agree)
	c.rejCursorSent += b2i(sent)
	c.rejKnownGap += gap
}

// endFacts is three of the beside counts of an end row's frame, computed the same way
// for one that stopped the page 6 (count) and for one the position check rejected
// (reject): v2 is whether the frame carries an inbox-parent V2 row and agree one with
// the row's minimum; sent is a database-1 block naming the cursor the page was sent
// with; gap is the known threads strictly between the frame's newest thread and the
// reference that the frame does not carry (encrypted ones excluded). The caller holds
// s.mu.
func (s *messagixSession) endFacts(w *rangeWatch, pos pagePosition, f lateFrame, lo cursorPos) (v2, agree, sent bool, gap int) {
	for _, r := range f.rows.v2 {
		if r.ParentThreadKey == inboxParentKey {
			v2 = true
			if r.MinLastActivityTimestampMs == lo.ts && r.MinThreadKey == lo.key {
				agree = true
			}
		}
	}
	sent = w.cursor != "" && slices.Contains(f.cursors, w.cursor)
	for _, th := range s.threads {
		at := cursorPos{ts: th.LastActivity, key: th.Key}
		if !th.Enc && th.LastActivity > 0 && f.newest.olderThan(at) && at.olderThan(pos.ref) && !f.keys[th.Key] {
			gap++
		}
	}
	return v2, agree, sent, gap
}

// count records a stop-6 end row. The caller holds s.mu.
func (c *end6Counts) count(s *messagixSession, w *rangeWatch, pos pagePosition, f lateFrame, row *table.LSUpsertSyncGroupThreadsRange) {
	c.pages++
	if f.tag.ok && w.ids.PacketID != 0 && f.tag.id == w.ids.PacketID {
		c.tiedReq++
	}
	for _, id := range f.taskIDs {
		if slices.Contains(w.ids.TaskIDs, id) {
			c.tiedTask++
			break
		}
	}
	lo := cursorPos{ts: row.MinLastActivityTimestampMS, key: row.MinThreadKey}
	tight := f.floorOK && lo == f.floor
	if tight {
		c.tight++
	}
	v2, agree, sent, gap := s.endFacts(w, pos, f, lo)
	switch {
	case agree:
		c.v2Agree++
	case v2:
		c.v2Disagree++
	}
	if w.resync {
		c.resync++
	}
	switch {
	case sent:
		c.cursorSent++
	case len(f.cursors) > 0:
		c.cursorOther++
	}
	sameWalkEpoch := w.epoch == w.firstEpoch
	if !sameWalkEpoch {
		c.walkEpochMoved++
	}
	c.knownGap += gap
	if tight && agree && !w.resync && sameWalkEpoch {
		c.c1to8++
	}
}

func (c *end6Counts) addStats(stats map[string]int) {
	stats["inbox_end6_pages"] = c.pages
	stats["inbox_end6_tied_req"] = c.tiedReq
	stats["inbox_end6_tied_task"] = c.tiedTask
	stats["inbox_end6_tight"] = c.tight
	stats["inbox_end6_v2_agree"] = c.v2Agree
	stats["inbox_end6_v2_disagree"] = c.v2Disagree
	stats["inbox_end6_resync"] = c.resync
	stats["inbox_end6_cursor_sent"] = c.cursorSent
	stats["inbox_end6_cursor_other"] = c.cursorOther
	stats["inbox_end6_walk_epoch_moved"] = c.walkEpochMoved
	stats["inbox_end6_known_gap"] = min(c.knownGap, 999)
	stats["inbox_end6_c1to8"] = c.c1to8
	stats["inbox_end6_contradicted"] = c.contradicted
	stats["inbox_end_rej_preconnect"] = c.rejPreConnect
	stats["inbox_end_rej_nothreads"] = c.rejNoThreads
	stats["inbox_end_rej_untimed"] = c.rejUntimed
	stats["inbox_end_rej_newer"] = c.rejNewer
	stats["inbox_end_rej_more_in_frame"] = c.rejMoreInFrame
	stats["inbox_end_rej_position"] = c.rejPosition
	stats["inbox_end_rej_pos_ceiling"] = c.rejPosCeiling
	stats["inbox_end_rej_pos_floor"] = c.rejPosFloor
	stats["inbox_end_rej_pos_nofloor"] = c.rejPosNoFloor
	stats["inbox_end_rej_min_eq_ref"] = c.rejMinEqRef
	stats["inbox_end_rej_min_untimed"] = c.rejMinUntimed
	stats["inbox_end_rej_threads_below_ref"] = c.rejThreadsBelowRef
	stats["inbox_end_rej_v2_agree"] = c.rejV2Agree
	stats["inbox_end_rej_cursor_sent"] = c.rejCursorSent
	stats["inbox_end_rej_known_gap"] = min(c.rejKnownGap, 999)
	stats["inbox_end_rej_last_page"] = c.rejLastPage
}

// tieProbe counts, over a walk's pages, the live tables seen while a page's watch was
// open, by how their frame's request_id compares with the page's packet id: the page's
// own, an earlier page's of this walk, zero, another, or none (not a live frame). It
// also counts the task-table rows (LSRemoveTask, LSTaskExists, task completion) that
// name the page's task id, an earlier page's, or another. pagesWithIDs is the pages
// whose request ids were known; without them nothing is compared.
type tieProbe struct {
	pagesWithIDs, frames, noID                 int
	reqPage, reqPrev, reqZero, reqOther        int
	reqPageRange, reqPageEnd, reqPageThreads   int
	reqPrevRange, reqPrevEnd                   int
	removePage, removePrev, removeOther        int
	existsPage, existsPrev, donePage, donePrev int
	prevPackets, prevTasks                     map[int64]bool
}

// count adds the tables w saw, against w's ids, then remembers w's ids as an earlier
// page's. The caller holds s.mu.
func (t *tieProbe) count(w *rangeWatch) {
	known := w.ids.PacketID != 0
	if known {
		t.pagesWithIDs++
	}
	own := func(id int64) bool { return slices.Contains(w.ids.TaskIDs, id) }
	for _, ls := range w.seen {
		t.frames++
		switch {
		case !ls.tag.ok:
			t.noID++
		case !known:
		case ls.tag.id == w.ids.PacketID:
			t.reqPage++
			t.reqPageRange += b2i(ls.ranges)
			t.reqPageEnd += b2i(ls.end)
			t.reqPageThreads += b2i(ls.threads)
		case t.prevPackets[ls.tag.id]:
			t.reqPrev++
			t.reqPrevRange += b2i(ls.ranges)
			t.reqPrevEnd += b2i(ls.end)
		case ls.tag.id == 0:
			t.reqZero++
		default:
			t.reqOther++
		}
		if !known {
			continue
		}
		for _, id := range ls.remove {
			switch {
			case own(id):
				t.removePage++
			case t.prevTasks[id]:
				t.removePrev++
			default:
				t.removeOther++
			}
		}
		for _, id := range ls.exists {
			if own(id) {
				t.existsPage++
			} else if t.prevTasks[id] {
				t.existsPrev++
			}
		}
		for _, id := range ls.doneT {
			if own(id) {
				t.donePage++
			} else if t.prevTasks[id] {
				t.donePrev++
			}
		}
	}
	w.seen = nil
	if !known {
		return
	}
	if t.prevPackets == nil {
		t.prevPackets, t.prevTasks = map[int64]bool{}, map[int64]bool{}
	}
	t.prevPackets[w.ids.PacketID] = true
	for _, id := range w.ids.TaskIDs {
		t.prevTasks[id] = true
	}
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

func (t *tieProbe) addStats(stats map[string]int) {
	stats["inbox_tie_pages_ids"] = t.pagesWithIDs
	stats["inbox_tie_frames"] = t.frames
	stats["inbox_tie_noid"] = t.noID
	stats["inbox_tie_req_page"] = t.reqPage
	stats["inbox_tie_req_page_range"] = t.reqPageRange
	stats["inbox_tie_req_page_end"] = t.reqPageEnd
	stats["inbox_tie_req_page_threads"] = t.reqPageThreads
	stats["inbox_tie_req_prev"] = t.reqPrev
	stats["inbox_tie_req_prev_range"] = t.reqPrevRange
	stats["inbox_tie_req_prev_end"] = t.reqPrevEnd
	stats["inbox_tie_req_zero"] = t.reqZero
	stats["inbox_tie_req_other"] = t.reqOther
	stats["inbox_tie_remove_page"] = t.removePage
	stats["inbox_tie_remove_prev"] = t.removePrev
	stats["inbox_tie_remove_other"] = t.removeOther
	stats["inbox_tie_exists_page"] = t.existsPage
	stats["inbox_tie_exists_prev"] = t.existsPrev
	stats["inbox_tie_done_page"] = t.donePage
	stats["inbox_tie_done_prev"] = t.donePrev
}

// rangeClass counts group-1 sync-group range rows by parent class (the inbox -1,
// the group-wide 0, any other) and by more/end.
type rangeClass struct {
	inboxMore, inboxEnd, p0More, p0End, otherMore, otherEnd int
}

func (c *rangeClass) add(row *table.LSUpsertSyncGroupThreadsRange) {
	switch {
	case row.ParentThreadKey == inboxParentKey && row.HasMoreBefore:
		c.inboxMore++
	case row.ParentThreadKey == inboxParentKey:
		c.inboxEnd++
	case row.ParentThreadKey == 0 && row.HasMoreBefore:
		c.p0More++
	case row.ParentThreadKey == 0:
		c.p0End++
	case row.HasMoreBefore:
		c.otherMore++
	default:
		c.otherEnd++
	}
}

func (c rangeClass) addStats(stats map[string]int, prefix string) {
	stats[prefix+"inbox_more"] = c.inboxMore
	stats[prefix+"inbox_end"] = c.inboxEnd
	stats[prefix+"p0_more"] = c.p0More
	stats[prefix+"p0_end"] = c.p0End
	stats[prefix+"other_more"] = c.otherMore
	stats[prefix+"other_end"] = c.otherEnd
}

// replyDiag is the walk replies' task-table rows, database-1 sync blocks, the
// LSRemoveTask rows naming the page's own task id, and the table names present.
type replyDiag struct {
	removeTask, taskExists, taskDone, db1Blocks, removePage int
	names                                                   map[string]bool
}

func (d *replyDiag) see(tbl *table.LSTable, ids messagix.RequestIDs) {
	d.removeTask += len(tbl.LSRemoveTask)
	d.taskExists += len(tbl.LSTaskExists)
	d.taskDone += len(tbl.LSMailboxTaskCompletionApiOnTaskCompletion)
	for _, t := range tbl.LSRemoveTask {
		if slices.Contains(ids.TaskIDs, t.TaskId) {
			d.removePage++
		}
	}
	for _, b := range tbl.GetLSExecuteFirstBlockForSyncTransactionV4() {
		if b.DatabaseID == 1 {
			d.db1Blocks++
		}
	}
	if d.names == nil {
		d.names = map[string]bool{}
	}
	for _, name := range tbl.NonNilFields() {
		d.names[name] = true
	}
}

func (d *replyDiag) addStats(stats map[string]int) {
	stats["inbox_reply_remove_task"] = d.removeTask
	stats["inbox_reply_task_exists"] = d.taskExists
	stats["inbox_reply_task_done"] = d.taskDone
	stats["inbox_reply_db1_blocks"] = d.db1Blocks
	stats["inbox_reply_remove_page"] = d.removePage
}

// tableNames is a set of table names as one fixed code: sorted, without the "LS"
// prefix, joined with "+"; "none" when empty. The names are the library's own fixed
// table names, never content.
func tableNames(set map[string]bool) string {
	if len(set) == 0 {
		return "none"
	}
	names := make([]string, 0, len(set))
	for name := range set {
		names = append(names, strings.TrimPrefix(name, "LS"))
	}
	slices.Sort(names)
	return strings.Join(names, "+")
}

// logWalkTables logs the table names of a listing's replies and of its later range
// frames, only when the code differs from the last one logged.
func (s *messagixSession) logWalkTables(rows *inboxRowCounts) {
	code := "inbox_tables.r." + tableNames(rows.reply.names) + ".l." + tableNames(rows.lateNames)
	s.mu.Lock()
	same := code == s.tablesLogged
	s.tablesLogged = code
	s.mu.Unlock()
	if !same {
		logCode("info", code)
	}
}

// Start table sources, for start_tbl_src.
const (
	startSrcNone = iota
	startSrcPageLoad
	startSrcResumeSync
	startSrcPageReload
)

// startKinds is the kinds of rows in a start's table (section "start table" of the
// walk notes): the page load's (a fresh start, or a resumed start's reload) or the
// resume sync's. It is the one table the helper gets that answers its own request, so
// its range rows are the only tied range rows it can see. Counts only.
type startKinds struct {
	src                                   int
	threads, verify                       int
	g1                                    rangeClass
	inboxMore, inboxEnd, v2Inbox, v2Other int
	rangesQuery, syncBlocks, db1Blocks    int
	names                                 map[string]bool
}

func (k *startKinds) add(tbl *table.LSTable) {
	if tbl == nil {
		return
	}
	k.threads += threadRowCount(tbl)
	k.verify += len(tbl.LSVerifyThreadExists)
	r := rangeRowsOf(tbl)
	for _, row := range r.sync {
		k.g1.add(row)
	}
	for _, row := range r.inbox {
		if row.HasMoreBefore {
			k.inboxMore++
		} else {
			k.inboxEnd++
		}
	}
	for _, row := range r.v2 {
		if row.ParentThreadKey == inboxParentKey {
			k.v2Inbox++
		} else {
			k.v2Other++
		}
	}
	k.rangesQuery += len(tbl.LSThreadsRangesQuery)
	for _, b := range tbl.GetLSExecuteFirstBlockForSyncTransactionV4() {
		k.syncBlocks++
		if b.DatabaseID == 1 {
			k.db1Blocks++
		}
	}
	if k.names == nil {
		k.names = map[string]bool{}
	}
	for _, name := range tbl.NonNilFields() {
		k.names[name] = true
	}
}

func (k startKinds) addStats(stats map[string]int) {
	stats["start_tbl_src"] = k.src
	stats["start_tbl_threads"] = k.threads
	stats["start_tbl_verify_threads"] = k.verify
	k.g1.addStats(stats, "start_tbl_g1_")
	stats["start_tbl_inbox_more"] = k.inboxMore
	stats["start_tbl_inbox_end"] = k.inboxEnd
	stats["start_tbl_v2_inbox"] = k.v2Inbox
	stats["start_tbl_v2_other"] = k.v2Other
	stats["start_tbl_ranges_query"] = k.rangesQuery
	stats["start_tbl_sync_blocks"] = k.syncBlocks
	stats["start_tbl_db1_blocks"] = k.db1Blocks
}

// setStartKinds records the kinds of a start's tables and logs their table names once.
func (s *messagixSession) setStartKinds(src int, tbls ...*table.LSTable) {
	k := startKinds{src: src}
	for _, tbl := range tbls {
		k.add(tbl)
	}
	s.mu.Lock()
	s.startKinds = k
	s.mu.Unlock()
	logCode("info", "start_tables."+strconv.Itoa(src)+"."+tableNames(k.names))
}
