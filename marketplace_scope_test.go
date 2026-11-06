// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// marketFolder is the parent key of a Marketplace folder in these tests.
const marketFolder = 777

// dumpWithParents is a library state dump whose page config names these parent thread keys.
func dumpWithParents(keys ...int64) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"Configs": map[string]any{"ParentThreadKeys": keys}})
	return b
}

// marketThread is one Marketplace (type 5) thread row of self and one buyer (key+1), with
// a two-member snapshot and both participant rows, in folder under parent.
func marketThread(key, parent int64, folder string) *table.LSTable {
	return &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{
			ThreadKey: key, ThreadType: table.MARKETPLACE, MemberCount: 2, FolderName: folder,
			ParentThreadKey: parent, ThreadName: "Bike", LastActivityTimestampMs: 99,
		}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{
			{ThreadKey: key, ContactId: 1}, {ThreadKey: key, ContactId: key + 1},
		},
	}
}

func joinTables(tbls ...*table.LSTable) *table.LSTable {
	out := &table.LSTable{}
	for _, t := range tbls {
		out.LSDeleteThenInsertThread = append(out.LSDeleteThenInsertThread, t.LSDeleteThenInsertThread...)
		out.LSAddParticipantIdToGroupThread = append(out.LSAddParticipantIdToGroupThread, t.LSAddParticipantIdToGroupThread...)
		out.LSDeleteThenInsertMessageRequest = append(out.LSDeleteThenInsertMessageRequest, t.LSDeleteThenInsertMessageRequest...)
		out.LSUpsertSyncGroupThreadsRange = append(out.LSUpsertSyncGroupThreadsRange, t.LSUpsertSyncGroupThreadsRange...)
	}
	return out
}

// certifiable checks that a thread is emitted typed as a two-person group, is a roster
// target, is certified, and that Send to its one buyer passes the roster check.
func certifiable(t *testing.T, s *messagixSession, frames []Frame, id string, buyer string) {
	t.Helper()
	th := lastThread(frames, id)
	if th == nil || !th.Typed || th.Kind != "group" || !th.MembersComplete || !connectorWouldQualify(th) {
		t.Fatalf("thread %s must be emitted typed and certified: %+v", id, th)
	}
	s.mu.Lock()
	targeted := false
	for _, tg := range s.rosterTargetsLocked() {
		if wireID(false, tg.Key) == id {
			targeted = true
		}
	}
	s.mu.Unlock()
	if !targeted {
		t.Fatalf("thread %s is not a roster target", id)
	}
	if err := expectErr(s, id, buyer); err != nil {
		t.Fatalf("send to %s with expect [%s] = %v, want allowed", id, buyer, err)
	}
}

// inert checks that a thread went out with only its id and activity, and cannot be sent to.
func inert(t *testing.T, s *messagixSession, frames []Frame, id string, buyer string) {
	t.Helper()
	th := lastThread(frames, id)
	if th == nil || th.Kind != "" || th.Typed || th.Title != "" || len(th.Participants) != 0 || th.MembersComplete || connectorWouldQualify(th) {
		t.Fatalf("thread %s must be emitted inert: %+v", id, th)
	}
	if err := expectErr(s, id, buyer); err == nil {
		t.Fatalf("send allowed on inert thread %s", id)
	}
}

// KD-156: a new Marketplace buyer's thread under a Marketplace folder (a group-1 parent other than the inbox,
// named by the library's configs.ParentThreadKeys) is reached by that folder's first page, listed typed, and
// certified as a two-person roster from the participant rows of that page.
func TestMarketplaceFolderFirstPageIsListedTypedAndCertifies(t *testing.T) {
	r := &pager{cursor: "cur-1", dump: dumpWithParents(-1, marketFolder), scope: map[scopeKey]*table.LSTable{
		{1, marketFolder}: marketThread(3003, marketFolder, "inbox"),
	}}
	s := newTestSession()
	frames, res, err, done := listGap(t, s, r, context.Background(), 5*time.Second)
	if !done || err != nil || res.Gap != "" {
		t.Fatalf("done=%v err=%v gap=%q", done, err, res.Gap)
	}
	want := socket.FetchThreadsTask{ParentThreadKey: marketFolder, ReferenceActivityTimestamp: 9999999999999, Cursor: "cur-1", SyncGroup: 1}
	found := false
	for _, task := range r.scopes {
		if *task.(*socket.FetchThreadsTask) == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("no first-page request for the Marketplace folder: %+v", r.scopes)
	}
	certifiable(t, s, frames, "3003", "3004")
}

// A folder the server names only in a group-1 range row of the inbox walk (not in the page config) is asked too.
func TestFolderNamedByAnInboxRangeRowIsAsked(t *testing.T) {
	r := &pager{
		pages: []*table.LSTable{page(101, rangeRow(false, -1, 90, 5000), rangeRow(true, 888, 1, 1))},
		scope: map[scopeKey]*table.LSTable{{1, 888}: marketThread(3003, 888, "")},
	}
	s := newTestSession()
	frames, _, err, done := listGap(t, s, r, context.Background(), 5*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	certifiable(t, s, frames, "3003", "3004")
}

// KD-156, the other possible scope: a new Marketplace buyer's thread arrives as a message request (sync group 95,
// pending folder or request status 1). It is listed typed and certifiable. Every other request stays inert: a
// pending one-to-one, a pending plain group, and a Marketplace thread in spam.
func TestPendingMarketplaceRequestIsListedTypedAndCertifies(t *testing.T) {
	for name, pending := range map[string]*table.LSTable{
		"pending folder": marketThread(3005, -1, folderPending),
		"status 1": func() *table.LSTable {
			tbl := marketThread(3005, -1, "")
			tbl.LSDeleteThenInsertMessageRequest = []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 3005, MessageRequestStatus: 1}}
			return tbl
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			oneToOne := &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 3007, ThreadType: table.ONE_TO_ONE, FolderName: folderPending, ThreadName: "Stranger"}}}
			plainGroup := groupRowsIn(folderPending, 2, 7)
			spam := marketThread(3009, -1, folderSpam)
			r := &pager{scope: map[scopeKey]*table.LSTable{
				{requestSyncGroup, -1}: joinTables(pending, oneToOne, plainGroup, spam),
			}}
			s := newTestSession()
			frames, _, err, done := listGap(t, s, r, context.Background(), 5*time.Second)
			if !done || err != nil {
				t.Fatalf("done=%v err=%v", done, err)
			}
			certifiable(t, s, frames, "3005", "3006")
			inert(t, s, frames, "3007", "3007")
			inert(t, s, frames, "2002", "7")
			inert(t, s, frames, "3009", "3010")
			s.mu.Lock()
			for _, tg := range s.rosterTargetsLocked() {
				if tg.Key == 2002 || tg.Key == 3009 {
					t.Fatalf("an unaccepted thread %d is a roster target", tg.Key)
				}
			}
			s.mu.Unlock()
		})
	}
}

// recordingPager records every task of every request, whatever its type.
type recordingPager struct {
	pager
	all []socket.Task
}

func (r *recordingPager) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	r.all = append(r.all, tasks...)
	return r.pager.ExecuteTasks(ctx, tasks...)
}

// No Meta-side effect: listing a pending Marketplace request and a Marketplace folder sends only thread-list reads.
// Nothing marks a thread read (task 21) or accepts a request (task 66).
func TestScopeScanNeverAcceptsOrMarksRead(t *testing.T) {
	if socket.TaskLabels["ThreadMarkRead"] != "21" || socket.TaskLabels["AcceptMessageRequestTask"] != "66" || socket.TaskLabels["FetchThreadsTask"] != "145" {
		t.Fatalf("task labels moved: %v", socket.TaskLabels)
	}
	r := &recordingPager{pager: pager{dump: dumpWithParents(marketFolder), scope: map[scopeKey]*table.LSTable{
		{1, marketFolder}:      marketThread(3003, marketFolder, "inbox"),
		{requestSyncGroup, -1}: marketThread(3005, -1, folderPending),
	}}}
	s := newTestSession()
	frames, _, err, done := listGap(t, s, r, context.Background(), 5*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	certifiable(t, s, frames, "3005", "3006")
	if len(r.all) != 4 { // inbox, folder, requests of the inbox, requests of the folder
		t.Fatalf("requests = %d, want 4", len(r.all))
	}
	for _, task := range r.all {
		if label := task.GetLabel(); label != "145" {
			t.Fatalf("listing sent task %s (%T), want only FetchThreadsTask (145)", label, task)
		}
	}
	// a known pending Marketplace thread is still not accepted by a second listing
	r.all = nil
	if _, _, err, done := listGap(t, s, r, context.Background(), 5*time.Second); !done || err != nil {
		t.Fatalf("second listing: done=%v err=%v", done, err)
	}
	for _, task := range r.all {
		if label := task.GetLabel(); label == "21" || label == "66" {
			t.Fatalf("second listing sent task %s", label)
		}
	}
}

// The scan is bounded: at most scopeParentCap folders, the first page of each scope only (even when a scope says
// there is more), every request from the top, and each scope asked once even when every one of them fails.
func TestScopeScanIsBoundedToFirstPages(t *testing.T) {
	keys := []int64{-1, 0}
	for k := int64(1); k <= 10; k++ {
		keys = append(keys, 1000+k)
	}
	more := &table.LSTable{LSUpsertSyncGroupThreadsRange: []*table.LSUpsertSyncGroupThreadsRange{{SyncGroup: 1, ParentThreadKey: 1001, HasMoreBefore: true, MinThreadKey: 5, MinLastActivityTimestampMS: 5}}}
	r := &pager{dump: dumpWithParents(keys...), scope: map[scopeKey]*table.LSTable{{1, 1001}: more}}
	_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 20*time.Second)
	if !done || err != nil || res.Gap != "" {
		t.Fatalf("done=%v err=%v gap=%q", done, err, res.Gap)
	}
	if len(r.scopes) != 2*scopeParentCap+1 {
		t.Fatalf("scope requests = %d, want %d", len(r.scopes), 2*scopeParentCap+1)
	}
	seen := map[scopeKey]bool{}
	for _, task := range r.scopes {
		ft := task.(*socket.FetchThreadsTask)
		k := scopeKey{int64(ft.SyncGroup), ft.ParentThreadKey}
		if seen[k] || ft.ReferenceThreadKey != 0 || ft.ReferenceActivityTimestamp != 9999999999999 || ft.ParentThreadKey == 0 || ft.ParentThreadKey > 1000+scopeParentCap {
			t.Fatalf("scope request %+v (repeat=%v)", ft, seen[k])
		}
		seen[k] = true
	}

	failing := &failingScopes{fail: func(socket.FetchThreadsTask) bool { return true }}
	_, res, err, done = listGap(t, newTestSession(), failing, context.Background(), 5*time.Second)
	if !done || err != nil || res.Stats == nil {
		t.Fatalf("a failed scope request must not fail the listing: done=%v err=%v res=%+v", done, err, res)
	}
	if len(failing.scopes) != 3 || res.Stats["scope_failed"] != 3 || res.Gap != gapRequestsIncomplete {
		t.Fatalf("every scope asked once, each failure counted: requests=%d stats=%v gap=%q", len(failing.scopes), res.Stats, res.Gap)
	}
}

// failingScopes answers the inbox with one complete page, fails the scopes fail picks, and answers the others from
// scope (or empty).
type failingScopes struct {
	fail   func(socket.FetchThreadsTask) bool
	scope  map[scopeKey]*table.LSTable
	scopes []socket.FetchThreadsTask
}

func (f *failingScopes) GetCursor(int64) string { return "" }
func (f *failingScopes) DumpState() (json.RawMessage, error) {
	return dumpWithParents(marketFolder), nil
}
func (f *failingScopes) ExecuteTasks(_ context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if !isInboxTask(tasks[0]) {
		task := *tasks[0].(*socket.FetchThreadsTask)
		f.scopes = append(f.scopes, task)
		if f.fail(task) {
			return nil, errors.New("boom")
		}
		if tbl := f.scope[scopeKey{int64(task.SyncGroup), task.ParentThreadKey}]; tbl != nil {
			return tbl, nil
		}
		return &table.LSTable{}, nil
	}
	return page(101, rangeRow(false, -1, 1, 1)), nil
}

// Sol P2a: a folder whose request always fails does not hide the other scopes. The message requests of the inbox
// and of that folder are still asked, a buyer there still certifies, and the listing says it is incomplete
// (requests_incomplete, scope_failed 1) instead of reporting a clean, complete listing.
func TestFailingFolderDoesNotStarveRequestsAndMarksTheListingIncomplete(t *testing.T) {
	failing := &failingScopes{
		fail: func(task socket.FetchThreadsTask) bool {
			return task.SyncGroup == 1 && task.ParentThreadKey == marketFolder
		},
		scope: map[scopeKey]*table.LSTable{{requestSyncGroup, -1}: marketThread(3005, -1, folderPending)},
	}
	s := newTestSession()
	frames, res, err, done := listGap(t, s, failing, context.Background(), 5*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	asked := map[scopeKey]bool{}
	for _, task := range failing.scopes {
		asked[scopeKey{int64(task.SyncGroup), task.ParentThreadKey}] = true
	}
	if !asked[scopeKey{requestSyncGroup, -1}] || !asked[scopeKey{requestSyncGroup, marketFolder}] || !asked[scopeKey{1, marketFolder}] {
		t.Fatalf("scopes asked = %v, want the failing folder and both request scopes", asked)
	}
	if res.Gap != gapRequestsIncomplete || res.Stats["scope_failed"] != 1 || res.Stats["scope_pages"] != 2 || res.Stats["scope_parents"] != 1 {
		t.Fatalf("gap=%q stats=%v, want requests_incomplete with one failed scope", res.Gap, res.Stats)
	}
	certifiable(t, s, frames, "3005", "3006")
}

// Sol P1: a live row that changes a pending Marketplace thread's type (here to a plain group, folder unchanged)
// while a scope request is in flight is a certification input: the reply issued before it, still saying type 5
// with a two-member roster, must neither restore Marketplace eligibility nor certify the request.
func TestStaleScopeReplyDoesNotUndoLiveTypeChange(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(marketThread(3005, -1, folderPending))
	mustComplete(t, s, "3005", true, "premise: a pending Marketplace thread certifies")
	s.mu.Lock()
	mark := s.issueMarkLocked()
	s.mu.Unlock()
	s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.GROUP_THREAD}}})
	mustComplete(t, s, "3005", false, "a pending plain group")
	s.handleReplyEmit(marketThread(3005, -1, folderPending), mark, func(Frame) int64 { return 0 })
	mustComplete(t, s, "3005", false, "stale type-5 reply over a live type change")
	if err := expectErr(s, "3005", "3006"); err == nil {
		t.Fatal("send allowed on a pending non-Marketplace thread")
	}
	s.mu.Lock()
	raw, unaccepted := s.threads["3005"].RawType, s.unacceptedLocked(3005)
	s.mu.Unlock()
	if raw != int64(table.GROUP_THREAD) || !unaccepted {
		t.Fatalf("type = %d unaccepted = %v, want the live plain group, still a request", raw, unaccepted)
	}
	// a fresh reply, issued after the live change, still applies
	s.mu.Lock()
	mark = s.issueMarkLocked()
	s.mu.Unlock()
	s.handleReplyEmit(marketThread(3005, -1, folderPending), mark, func(Frame) int64 { return 0 })
	mustComplete(t, s, "3005", true, "a reply issued after the live change")
}

// Sol P2b: a buyer under a folder named only by a range row (not by the page config) is re-certified by the reconnect
// refresh, which collects the parents its own inbox walk's range rows name.
func TestRefreshRecertifiesARangeOnlyFolderTarget(t *testing.T) {
	h := newRefreshHarness(t, &refreshFake{})
	r := &pager{
		pages: []*table.LSTable{page(101, rangeRow(false, -1, 90, 5000), rangeRow(true, 888, 1, 1))},
		scope: map[scopeKey]*table.LSTable{{1, 888}: marketThread(3003, 888, "inbox")},
	}
	h.s.refreshPager = r
	h.s.handleTableFrames(marketThread(3003, 888, "inbox"))
	mustComplete(t, h.s, "3003", true, "premise")
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	mustComplete(t, h.s, "3003", true, "after the refresh")
	asked := false
	for _, task := range r.scopes {
		if ft := task.(*socket.FetchThreadsTask); ft.SyncGroup == 1 && ft.ParentThreadKey == 888 {
			asked = true
		}
	}
	if !asked {
		t.Fatalf("the refresh never asked the range-only folder: %+v", r.scopes)
	}
}

// Meta side effect: History never reads a thread that carries a request mark. A pending Marketplace thread (typed
// and imported by the listing) gets "unavailable" and no task at all, in particular no CreateThreadTask (209), with
// or without a cursor. Once a row clears the mark, History reads it with the usual first-page task.
func TestPendingThreadHistorySendsNoTask(t *testing.T) {
	if socket.TaskLabels["CreateThreadTask"] != "209" || socket.TaskLabels["FetchMessagesTask"] != "228" {
		t.Fatalf("task labels moved: %v", socket.TaskLabels)
	}
	for name, pending := range map[string]*table.LSTable{
		"pending folder": marketThread(3005, -1, folderPending),
		"status 1": func() *table.LSTable {
			tbl := marketThread(3005, -1, "")
			tbl.LSDeleteThenInsertMessageRequest = []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 3005, MessageRequestStatus: 1}}
			return tbl
		}(),
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			rec := &recordingPager{}
			s.historyPager = rec
			s.handleTableFrames(pending)
			mustComplete(t, s, "3005", true, "premise: listed typed and certified")
			for _, cursor := range []string{"", "77:mid.1"} {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				_, err := s.History(ctx, "3005", cursor, 20)
				cancel()
				if codeOf(err) != "unavailable" || len(rec.all) != 0 {
					t.Fatalf("cursor %q: err=%v tasks=%d, want unavailable and no task", cursor, err, len(rec.all))
				}
			}
			// accepted: the first page is read with the usual task
			s.handleTableFrames(&table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 3005, MessageRequestStatus: 0}}})
			s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.MARKETPLACE, FolderName: "inbox"}}})
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, _ = s.History(ctx, "3005", "", 20)
			cancel()
			if len(rec.all) == 0 || rec.all[0].GetLabel() != "209" {
				t.Fatalf("after acceptance History must read the thread: tasks=%v", rec.all)
			}
		})
	}
}

// The stale-reply fence covers scope replies: a request reply issued before a live move to spam carries the old
// pending folder and roster, and must neither clear the spam mark nor certify the Marketplace thread.
func TestStaleScopeReplyDoesNotUndoLiveSpamMove(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(marketThread(3005, -1, folderPending))
	mustComplete(t, s, "3005", true, "premise: a pending Marketplace thread certifies")
	s.mu.Lock()
	mark := s.issueMarkLocked()
	s.mu.Unlock()
	s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.MARKETPLACE, FolderName: folderSpam}}})
	s.handleReplyEmit(marketThread(3005, -1, folderPending), mark, func(Frame) int64 { return 0 })
	mustComplete(t, s, "3005", false, "stale pending reply over a live spam move")
	if err := expectErr(s, "3005", "3006"); err == nil {
		t.Fatal("send allowed on a Marketplace thread moved to spam")
	}
	s.mu.Lock()
	spam := s.spamMark[3005]
	s.mu.Unlock()
	if !spam {
		t.Fatal("the spam mark was cleared by a stale reply")
	}
}

// After a reconnect, a Marketplace target that lives outside the main inbox is re-certified by the refresh from
// its folder's first page; the inbox walk alone never re-sees it.
func TestRefreshRecertifiesAMarketplaceFolderTarget(t *testing.T) {
	h := newRefreshHarness(t, &refreshFake{})
	r := &pager{dump: dumpWithParents(marketFolder), scope: map[scopeKey]*table.LSTable{
		{1, marketFolder}: marketThread(3003, marketFolder, "inbox"),
	}}
	h.s.refreshPager = r
	h.s.handleTableFrames(marketThread(3003, marketFolder, "inbox"))
	mustComplete(t, h.s, "3003", true, "premise")
	h.drop()
	h.reconnect()
	mustComplete(t, h.s, "3003", false, "reconnected, not yet refreshed")
	h.waitRefresh(t)
	mustComplete(t, h.s, "3003", true, "after the refresh")
	if len(r.tasks) != 1 || len(r.scopes) == 0 {
		t.Fatalf("inbox requests = %d, scope requests = %d", len(r.tasks), len(r.scopes))
	}
}

// One counts-only diagnostic per listing: threads by request scope and folder, and Marketplace (type 5) counts.
// It names no thread or parent key, and is not repeated while it does not change.
func TestScopeDiagnosticIsCountsOnlyAndLoggedOnChange(t *testing.T) {
	var buf bytes.Buffer
	old := stderr
	stderr = &buf
	defer func() { stderr = old }()
	r := &pager{dump: dumpWithParents(-1, marketFolder), scope: map[scopeKey]*table.LSTable{
		{1, marketFolder}:      marketThread(3003, -1, "inbox"), // the row says it is under another parent
		{requestSyncGroup, -1}: marketThread(3005, -1, folderPending),
	}}
	s := newTestSession()
	for i := 0; i < 2; i++ {
		if _, _, err, done := listGap(t, s, r, context.Background(), 5*time.Second); !done || err != nil {
			t.Fatalf("done=%v err=%v", done, err)
		}
	}
	const want = `{"code":"thread_scopes.k1.f1_i1_m1_q1.in_m0.r0_p1_m1.r1_m0","level":"info"}`
	var got []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if strings.Contains(line, "thread_scopes") {
			got = append(got, line)
		}
	}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("diagnostic lines = %q, want exactly %s", got, want)
	}
	for _, secret := range []string{"777", "3003", "3005", "Bike"} {
		if strings.Contains(got[0], secret) {
			t.Fatalf("diagnostic names %s: %s", secret, got[0])
		}
	}
}

// hookPager is a History client that records every task and runs onTask while task n is in flight.
type hookPager struct {
	recordingPager
	onTask func(n int)
}

func (h *hookPager) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	tbl, err := h.recordingPager.ExecuteTasks(ctx, tasks...)
	if h.onTask != nil {
		h.onTask(len(h.all))
	}
	return tbl, err
}

// Sol P1-2: History rechecks the request marks before every send. A request row that arrives while the call is
// waiting stops it: neither the re-request of the first page (another 209) nor a further page (228) goes out.
func TestRequestMarkArrivingMidHistoryStopsEverySend(t *testing.T) {
	mark := &table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 3005, MessageRequestStatus: 1}}}
	partial := &table.LSTable{
		LSInsertNewMessageRange: []*table.LSInsertNewMessageRange{{ThreadKey: 3005, MinTimestampMs: 50, MinMessageId: "mid.1", MaxTimestampMs: 50, HasMoreBefore: true}},
		LSUpsertMessage:         []*table.LSUpsertMessage{{ThreadKey: 3005, MessageId: "mid.1", TimestampMs: 50, SenderId: 3006, Text: "is this available?"}},
	}
	for name, tc := range map[string]struct {
		rows  []*table.LSTable // applied while the first send is in flight
		bound time.Duration
	}{
		"further page":            {[]*table.LSTable{mark, partial}, 3 * time.Second},
		"re-request of the first": {[]*table.LSTable{mark}, historyReRequest + 3*time.Second},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			s.handleTableFrames(marketThread(3005, -1, "inbox")) // accepted: History may read it
			h := &hookPager{}
			h.onTask = func(n int) {
				if n == 1 {
					for _, tbl := range tc.rows {
						s.handleTableFrames(tbl)
					}
				}
			}
			s.historyPager = h
			ctx, cancel := context.WithTimeout(context.Background(), tc.bound)
			defer cancel()
			_, err := s.History(ctx, "3005", "", 20)
			if codeOf(err) != "unavailable" {
				t.Fatalf("History = %v, want unavailable", err)
			}
			if len(h.all) != 1 || h.all[0].GetLabel() != "209" {
				labels := []string{}
				for _, task := range h.all {
					labels = append(labels, task.GetLabel())
				}
				t.Fatalf("tasks = %v, want only the first 209 sent before the mark", labels)
			}
		})
	}
}

// fact is the wire request fact of a frame: "true", "false", or "" when absent.
func fact(th *Thread) string {
	switch {
	case th == nil || th.Request == nil:
		return ""
	case *th.Request:
		return "true"
	}
	return "false"
}

// The thread frame says when a thread carries a request mark, typed (a pending Marketplace thread) or inert, so the
// connector can keep it out of history across restarts. A thread with no request seen says nothing (unknown); one
// whose request a later row cleared says false.
func TestThreadFrameCarriesTheRequestFact(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(joinTables(marketThread(3005, -1, folderPending), marketThread(3009, -1, folderSpam), marketThread(3003, -1, "inbox")))
	if th := lastThread(frames, "3005"); fact(th) != "true" || !th.Typed {
		t.Fatalf("pending Marketplace frame = %+v, want typed with request true", th)
	}
	if th := lastThread(frames, "3009"); fact(th) != "true" || th.Typed {
		t.Fatalf("spam frame = %+v, want inert with request true", th)
	}
	if th := lastThread(frames, "3003"); fact(th) != "" {
		t.Fatalf("a thread with no request seen = %q, want unknown (absent)", fact(th))
	}
	b, _ := json.Marshal(lastThread(frames, "3005"))
	if !strings.Contains(string(b), `"request":true`) {
		t.Fatalf("wire = %s", b)
	}
	b, _ = json.Marshal(lastThread(frames, "3003"))
	if strings.Contains(string(b), `"request"`) {
		t.Fatalf("an unknown request fact must be absent on the wire: %s", b)
	}
	frames = s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.MARKETPLACE, FolderName: "inbox"}}})
	if th := lastThread(frames, "3005"); fact(th) != "false" {
		t.Fatalf("accepted later = %q, want an explicit false", fact(th))
	}
	b, _ = json.Marshal(lastThread(frames, "3005"))
	if !strings.Contains(string(b), `"request":false`) {
		t.Fatalf("wire = %s", b)
	}
}

// Round 4, item 1: a status-only request row on an imported Marketplace thread (which stays typed and accepted for
// listing) still re-emits the thread with the new request fact: true on status 1, then an explicit false on status 0.
func TestStatusOnlyRowsReemitAMarketplaceThreadsRequestFact(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(marketThread(3005, -1, "inbox"))
	if th := lastThread(frames, "3005"); th == nil || fact(th) != "" {
		t.Fatalf("premise: imported with an unknown request fact: %+v", th)
	}
	status := func(v int64) *table.LSTable {
		return &table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 3005, MessageRequestStatus: v}}}
	}
	frames = s.handleTableFrames(status(1))
	if th := lastThread(frames, "3005"); th == nil || fact(th) != "true" || !th.Typed {
		t.Fatalf("status 1 alone: frame = %+v, want a typed frame with request true", th)
	}
	frames = s.handleTableFrames(status(1))
	if th := lastThread(frames, "3005"); th != nil {
		t.Fatalf("an unchanged fact re-emitted: %+v", th)
	}
	frames = s.handleTableFrames(status(0))
	if th := lastThread(frames, "3005"); th == nil || fact(th) != "false" {
		t.Fatalf("status 0 alone: frame = %+v, want request false", th)
	}
}
