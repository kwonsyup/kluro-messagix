// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// liveTestSession is a test session whose connected events do not start the
// encrypted client.
func liveTestSession() *messagixSession {
	s := newTestSession()
	s.e2eeStarting = true
	return s
}

func mustComplete(t *testing.T, s *messagixSession, id string, want bool, why string) {
	t.Helper()
	if got := wireOf(t, s, id).MembersComplete; got != want {
		t.Fatalf("%s: members_complete = %v, want %v", why, got, want)
	}
}

// Finding 1 (documents why the send does not hold s.mu across the provider
// call): table rows are applied under s.mu on the caller's goroutine, which in
// messagix is the socket's frame goroutine that also delivers a send's reply.
// While s.mu is held, table handling waits.
func TestRosterTableHandlingWaitsOnSessionLock(t *testing.T) {
	s := liveTestSession()
	s.mu.Lock()
	done := make(chan struct{})
	go func() {
		s.handleMetaEvent(context.Background(), groupRows(2, 7))
		close(done)
	}()
	select {
	case <-done:
		s.mu.Unlock()
		t.Fatal("table applied while s.mu was held")
	case <-time.After(100 * time.Millisecond):
	}
	s.mu.Unlock()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("table not applied after s.mu was released")
	}
}

// Finding 2: a disconnect ends the roster at once, and nothing certifies until
// the next connected event plus rows after it.
func TestMembersInvalidatedAtDisconnectUntilReconnected(t *testing.T) {
	ctx := context.Background()
	s := liveTestSession()
	s.handleTableFrames(groupRows(2, 7))
	mustComplete(t, s, "2002", true, "premise")
	s.handleMetaEvent(ctx, &messagix.TransientDisconnectEvent{Err: errors.New("closed")})
	mustComplete(t, s, "2002", false, "after disconnect")
	if err := expectErr(s, "2002", "7"); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect while disconnected: %v", err)
	}
	// Rows of the new socket's sync arrive before its connected event.
	s.handleMetaEvent(ctx, groupRows(2, 7))
	mustComplete(t, s, "2002", false, "resync rows while down")
	s.handleMetaEvent(ctx, &messagix.ReconnectedEvent{})
	mustComplete(t, s, "2002", false, "resync rows after reconnected")
	s.handleMetaEvent(ctx, groupRows(2, 7))
	mustComplete(t, s, "2002", true, "rows after reconnected")
	if err := expectErr(s, "2002", "7"); err != nil {
		t.Fatalf("expect after reconnect: %v", err)
	}
	s.handleMetaEvent(ctx, &messagix.PermanentErrorEvent{Err: errors.New("gone")})
	mustComplete(t, s, "2002", false, "after permanent error")
}

// reconnectingPager answers one listing page; during runs while the request
// is in flight.
type reconnectingPager struct {
	reply  *table.LSTable
	during func()
}

func (r *reconnectingPager) GetCursor(int64) string { return "" }

func (r *reconnectingPager) ExecuteTasks(context.Context, ...socket.Task) (*table.LSTable, error) {
	if r.during != nil {
		r.during()
	}
	tbl := r.reply
	tbl.LSUpsertSyncGroupThreadsRange = append(tbl.LSUpsertSyncGroupThreadsRange, &table.LSUpsertSyncGroupThreadsRange{SyncGroup: 1, ParentThreadKey: -1})
	return tbl, nil
}

// Finding 3: a listing reply to a request issued before a reconnect is applied
// after it; its rows date from the request and certify nothing.
func TestListingReplyIssuedBeforeReconnectNotFresh(t *testing.T) {
	s := liveTestSession()
	if _, _, err, done := listGap(t, s, &reconnectingPager{reply: groupRows(2, 7)}, context.Background(), 5*time.Second); !done || err != nil {
		t.Fatalf("control listing: done=%v err=%v", done, err)
	}
	mustComplete(t, s, "2002", true, "control: a listing in the current epoch certifies")

	s = liveTestSession()
	pg := &reconnectingPager{reply: groupRows(2, 7), during: s.bumpConnEpoch}
	if _, _, err, done := listGap(t, s, pg, context.Background(), 5*time.Second); !done || err != nil {
		t.Fatalf("listing: done=%v err=%v", done, err)
	}
	mustComplete(t, s, "2002", false, "reply issued before the reconnect")
	if err := expectErr(s, "2002", "7"); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect on a stale reply: %v", err)
	}
}

// Finding 4: a remove-all row empties the roster, its freshness and buffered
// rows before the same table's participant rows apply.
func TestMembersBulkResetClearsRoster(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(2, 7))
	mustComplete(t, s, "2002", true, "premise")
	reset := groupRows(2)
	reset.LSRemoveAllParticipantsForThread = []*table.LSRemoveAllParticipantsForThread{{ThreadKey: 2002}}
	s.handleTableFrames(reset)
	mustComplete(t, s, "2002", false, "reset then a count with no rows")
	if got := othersOf(wireOf(t, s, "2002")); len(got) != 0 {
		t.Fatalf("others after reset = %v", got)
	}
	if err := expectErr(s, "2002", "7"); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect [7] after reset: %v", err)
	}
	// Replacement rows in the same table as the reset apply after it.
	repl := groupRows(2, 9)
	repl.LSRemoveAllParticipantsForThread = []*table.LSRemoveAllParticipantsForThread{{ThreadKey: 2002}}
	s.handleTableFrames(repl)
	th := wireOf(t, s, "2002")
	if !th.MembersComplete || strings.Join(othersOf(th), ",") != "9" {
		t.Fatalf("after replacement complete=%v others=%v", th.MembersComplete, othersOf(th))
	}
	// A buffered row for a thread not yet known is dropped too.
	s.handleTableFrames(&table.LSTable{LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 3003, ContactId: 5}}})
	s.handleTableFrames(&table.LSTable{LSRemoveAllParticipantsForThread: []*table.LSRemoveAllParticipantsForThread{{ThreadKey: 3003}}})
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 3003, ThreadType: table.GROUP_THREAD, MemberCount: 2}}})
	mustComplete(t, s, "3003", false, "buffered row before reset")
	// A one-to-one thread keeps its other person.
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 42, ThreadType: table.ONE_TO_ONE}}})
	s.handleTableFrames(&table.LSTable{LSRemoveAllParticipantsForThread: []*table.LSRemoveAllParticipantsForThread{{ThreadKey: 42}}})
	if got := othersOf(wireOf(t, s, "42")); strings.Join(got, ",") != "42" {
		t.Fatalf("one-to-one others after reset = %v", got)
	}
}

// Finding 5: once an open thread is continued by an encrypted record, its own
// record never certifies, and a frame says so.
func TestMembersSupersededPlainThreadNotCertified(t *testing.T) {
	for name, mapping := range map[string]*table.LSTable{
		"authority": {LSUpdateThreadAuthorityAndMappingWithOTIDFromJID: []*table.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID{{ThreadKey: 2002, ThreadJID: 5005}}},
		"hybrid":    {LSVerifyHybridThreadExists: []*table.LSVerifyHybridThreadExists{{ThreadKey: 2002, ThreadJID: 5005}}},
		"cutover":   {LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{ArmadilloThreadID: 6006, OpenThreadID: 2002}}},
	} {
		s := newTestSession()
		s.handleTableFrames(groupRows(2, 7))
		mustComplete(t, s, "2002", true, name+": premise")
		frames := s.handleTableFrames(mapping)
		mustComplete(t, s, "2002", false, name+": superseded")
		if err := expectErr(s, "2002", "7"); codeOf(err) != "roster_unconfirmed" {
			t.Fatalf("%s: expect on a superseded thread: %v", name, err)
		}
		reemitted := false
		for _, f := range frames {
			if f.T == TThread && f.ThreadEvt.ID == "2002" && !f.ThreadEvt.MembersComplete {
				reemitted = true
			}
		}
		if !reemitted {
			t.Fatalf("%s: no frame withdrew members_complete: %+v", name, frames)
		}
		// A fresh snapshot of the old key does not certify it again.
		s.handleTableFrames(groupRows(2, 7))
		mustComplete(t, s, "2002", false, name+": snapshot after mapping")
	}
}

// Finding 6: a present empty expect survives the wire and is refused; an
// absent one stays absent.
func TestSendExpectEmptyStaysPresentAndIsRefused(t *testing.T) {
	var f Frame
	if err := json.Unmarshal([]byte(`{"t":"send","id":2,"thread":"2002","otid":"1","text":"hi","expect":[]}`), &f); err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(f)
	if !strings.Contains(string(out), `"expect":[]`) {
		t.Fatalf("present empty expect lost on re-encode: %s", out)
	}
	var g Frame
	if err := json.Unmarshal([]byte(`{"t":"send","id":2,"thread":"2002","otid":"1","text":"hi"}`), &g); err != nil {
		t.Fatal(err)
	}
	if out, _ := json.Marshal(g); strings.Contains(string(out), "expect") {
		t.Fatalf("absent expect appeared: %s", out)
	}
	// An empty expect names no recipient: a bad request, refused as
	// unconfirmed even on a complete roster.
	s := newTestSession()
	s.handleTableFrames(groupRows(2, 7))
	mustComplete(t, s, "2002", true, "premise")
	s.mu.Lock()
	err := s.expectLocked("2002", []string{})
	s.mu.Unlock()
	if codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("empty expect: %v", err)
	}
	fb := &fakeBackend{}
	runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"send","id":2,"thread":"2002","otid":"1","text":"hi","expect":[]}`,
		`{"t":"stop"}`)
	if fb.sentExpect == nil || len(fb.sentExpect) != 0 {
		t.Fatalf("present empty expect reached the backend as %#v", fb.sentExpect)
	}
}

// The two refusal codes: roster_unconfirmed while the roster is not known
// complete now (stale epoch, link down), roster_changed only when a complete
// roster differs from expect. Both refuse before any provider call: the zero
// client would panic on a write.
func TestSendExpectUnconfirmedVersusChanged(t *testing.T) {
	ctx := context.Background()
	s := liveTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(groupRows(2, 7))
	if err := sendWith(s, "2002", []string{"9"}); codeOf(err) != "roster_changed" {
		t.Fatalf("complete but mismatched: %v", err)
	}
	if err := expectErr(s, "2002", "7"); err != nil {
		t.Fatalf("two-member happy path: %v", err)
	}
	s.bumpConnEpoch()
	if err := sendWith(s, "2002", []string{"7"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("stale epoch: %v", err)
	}
	s.handleTableFrames(groupRows(2, 7))
	s.handleMetaEvent(ctx, &messagix.TransientDisconnectEvent{Err: errors.New("closed")})
	for _, expect := range []string{"7", "9"} {
		if err := sendWith(s, "2002", []string{expect}); codeOf(err) != "roster_unconfirmed" {
			t.Fatalf("link down, expect [%s]: %v", expect, err)
		}
	}
}

// refreshFake is the client of the roster refresh and of a concurrent Threads walk. It
// records whether two ExecuteTasks calls overlapped, which the helper assumes never happens.
type refreshFake struct {
	mu          sync.Mutex
	inflight    int
	overlapped  bool
	tasks       []socket.FetchThreadsTask
	startedAt   []time.Time
	reply       func(n int) *table.LSTable // page n, 1-based; nil means an empty page
	more        func(n int) bool           // whether page n says there is more before it
	during      func(n int)                // runs while request n is in flight
	gate        chan struct{}              // if set, request 1 blocks until it is closed
	firstServed chan struct{}              // closed when request 1 has started
}

func (r *refreshFake) GetCursor(int64) string { return "" }

func (r *refreshFake) ExecuteTasks(_ context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	r.mu.Lock()
	r.inflight++
	if r.inflight > 1 {
		r.overlapped = true
	}
	r.tasks = append(r.tasks, *tasks[0].(*socket.FetchThreadsTask))
	r.startedAt = append(r.startedAt, time.Now())
	n := len(r.tasks)
	r.mu.Unlock()
	defer func() { r.mu.Lock(); r.inflight--; r.mu.Unlock() }()
	if n == 1 && r.firstServed != nil {
		close(r.firstServed)
	}
	if n == 1 && r.gate != nil {
		<-r.gate
	}
	if r.during != nil {
		r.during(n)
	}
	tbl := &table.LSTable{}
	if r.reply != nil {
		tbl = r.reply(n)
	}
	more := r.more != nil && r.more(n)
	tbl.LSUpsertSyncGroupThreadsRange = append(tbl.LSUpsertSyncGroupThreadsRange,
		&table.LSUpsertSyncGroupThreadsRange{SyncGroup: 1, ParentThreadKey: -1, HasMoreBefore: more, MinThreadKey: int64(1000 - n), MinLastActivityTimestampMS: int64(5000 - n)})
	return tbl, nil
}

func (r *refreshFake) taskCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.tasks)
}

// twoGroups is marketplace two-person groups, each with one other member (key+1).
func twoGroups(keys ...int64) *table.LSTable {
	tbl := &table.LSTable{}
	for _, k := range keys {
		tbl.LSDeleteThenInsertThread = append(tbl.LSDeleteThenInsertThread, &table.LSDeleteThenInsertThread{ThreadKey: k, ThreadType: table.MARKETPLACE, MemberCount: 2})
		tbl.LSAddParticipantIdToGroupThread = append(tbl.LSAddParticipantIdToGroupThread, &table.LSAddParticipantIdToGroupThread{ThreadKey: k, ContactId: k + 1})
	}
	return tbl
}

// refreshHarness is a live session whose refresh uses fake, with every emitted
// frame and every finished refresh recorded.
type refreshHarness struct {
	s      *messagixSession
	fake   *refreshFake
	done   chan struct{}
	fmu    sync.Mutex
	frames []Frame
}

func newRefreshHarness(t *testing.T, fake *refreshFake) *refreshHarness {
	t.Helper()
	oldDebounce, oldPages, oldDelay := rosterRefreshDebounce, rosterRefreshPages, threadBatchDelay
	rosterRefreshDebounce, threadBatchDelay = 0, time.Millisecond
	t.Cleanup(func() { rosterRefreshDebounce, rosterRefreshPages, threadBatchDelay = oldDebounce, oldPages, oldDelay })
	h := &refreshHarness{s: liveTestSession(), fake: fake, done: make(chan struct{}, 16)}
	h.s.refreshPager = fake
	h.s.emit = func(f Frame) int64 { h.fmu.Lock(); h.frames = append(h.frames, f); h.fmu.Unlock(); return 0 }
	h.s.afterRefresh = func() { h.done <- struct{}{} }
	return h
}

// waitRefresh waits for one refresh goroutine to end.
func (h *refreshHarness) waitRefresh(t *testing.T) {
	t.Helper()
	select {
	case <-h.done:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh did not finish")
	}
}

// noRefresh fails if a refresh goroutine ends within d.
func (h *refreshHarness) noRefresh(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-h.done:
		t.Fatal("unexpected refresh")
	case <-time.After(d):
	}
}

func (h *refreshHarness) threadFrame(id string, complete bool) bool {
	h.fmu.Lock()
	defer h.fmu.Unlock()
	for _, f := range h.frames {
		if f.T == TThread && f.ThreadEvt.ID == id && f.ThreadEvt.MembersComplete == complete {
			return true
		}
	}
	return false
}

func (h *refreshHarness) drop() {
	h.s.handleMetaEvent(context.Background(), &messagix.TransientDisconnectEvent{Err: errors.New("closed")})
}

func (h *refreshHarness) reconnect() {
	h.s.handleMetaEvent(context.Background(), &messagix.ReconnectedEvent{})
}

// After a reconnect one refresh walk is issued from the top of the inbox, certifies
// the group again, and its c1 frame goes out through the normal path.
func TestReconnectRefreshCertifiesGroup(t *testing.T) {
	fake := &refreshFake{reply: func(int) *table.LSTable { return groupRows(2, 7) }, more: func(int) bool { return true }}
	h := newRefreshHarness(t, fake)
	h.s.handleTableFrames(groupRows(2, 7))
	mustComplete(t, h.s, "2002", true, "premise")
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	mustComplete(t, h.s, "2002", true, "after the refresh")
	if err := expectErr(h.s, "2002", "7"); err != nil {
		t.Fatalf("send after the refresh: %v", err)
	}
	h.noRefresh(t, 100*time.Millisecond)
	// The page certified the only target, so the walk stopped though it said there was more.
	if got := fake.taskCount(); got != 1 {
		t.Fatalf("walk requests = %d, want 1", got)
	}
	if task := fake.tasks[0]; task.ParentThreadKey != -1 || task.ReferenceThreadKey != 0 || task.ReferenceActivityTimestamp != 9999999999999 || task.SyncGroup != 1 {
		t.Fatalf("first request = %+v, want the top of the main inbox", task)
	}
	if !h.threadFrame("2002", true) {
		t.Fatal("no certified thread frame was emitted")
	}
}

// The first Connected of a run triggers nothing; a Connected after a disconnect does.
func TestInitialConnectedDoesNotRefresh(t *testing.T) {
	fake := &refreshFake{reply: func(int) *table.LSTable { return groupRows(2, 7) }}
	h := newRefreshHarness(t, fake)
	h.s.handleTableFrames(groupRows(2, 7))
	h.s.handleMetaEvent(context.Background(), &messagix.ConnectedEvent{})
	h.noRefresh(t, 150*time.Millisecond)
	if got := fake.taskCount(); got != 0 {
		t.Fatalf("initial Connected issued %d walk requests", got)
	}
	h.drop()
	h.s.handleMetaEvent(context.Background(), &messagix.ConnectedEvent{})
	h.waitRefresh(t)
	if got := fake.taskCount(); got != 1 {
		t.Fatalf("Connected after a disconnect issued %d walk requests, want 1", got)
	}
	// A further Connected without a disconnect in between is not a reconnect.
	h.s.handleMetaEvent(context.Background(), &messagix.ConnectedEvent{})
	h.noRefresh(t, 100*time.Millisecond)
}

// A refresh reply issued before a further disconnect certifies nothing, and
// neither does anything after it until fresh rows arrive.
func TestRefreshReplyIssuedBeforeSecondDisconnectCertifiesNothing(t *testing.T) {
	var h *refreshHarness
	fake := &refreshFake{reply: func(int) *table.LSTable { return groupRows(2, 7) }}
	fake.during = func(int) { h.s.setLinkDown(true) }
	h = newRefreshHarness(t, fake)
	h.s.handleTableFrames(groupRows(2, 7))
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	mustComplete(t, h.s, "2002", false, "reply issued before the second disconnect")
	if h.threadFrame("2002", true) {
		t.Fatal("a certified frame was emitted from a stale reply")
	}
	h.s.setLinkDown(false) // the link is back up, with no new rows yet
	mustComplete(t, h.s, "2002", false, "link back, no rows")
	if err := expectErr(h.s, "2002", "7"); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("send on a stale refresh: %v", err)
	}
}

// The refresh keeps its own start and never reads, clears or saves inboxResume,
// including when it ends at its page cap.
func TestRefreshLeavesInboxResumeAlone(t *testing.T) {
	fake := &refreshFake{more: func(int) bool { return true }} // never reaches the group
	h := newRefreshHarness(t, fake)
	rosterRefreshPages = 3
	h.s.handleTableFrames(groupRows(2, 7))
	resume := &inboxResumeRef{threadKey: 77, activityMS: 8888}
	h.s.inboxResume = resume
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	h.s.mu.Lock()
	got := h.s.inboxResume
	h.s.mu.Unlock()
	if got != resume || *got != (inboxResumeRef{77, 8888}) {
		t.Fatalf("inboxResume = %+v, want it untouched", got)
	}
	if n := fake.taskCount(); n != 3 {
		t.Fatalf("requests = %d, want the cap 3", n)
	}
	if task := fake.tasks[0]; task.ReferenceThreadKey != 0 || task.ReferenceActivityTimestamp != 9999999999999 {
		t.Fatalf("the refresh started at the resume position: %+v", task)
	}
	mustComplete(t, h.s, "2002", false, "not reached")
}

// A refresh and a Threads walk never run ExecuteTasks at the same time; the
// refresh waits for the walk and then runs.
func TestRefreshDoesNotOverlapThreadsWalk(t *testing.T) {
	fake := &refreshFake{reply: func(int) *table.LSTable { return groupRows(2, 7) }, gate: make(chan struct{}), firstServed: make(chan struct{})}
	h := newRefreshHarness(t, fake)
	h.s.handleTableFrames(groupRows(2, 7))
	walked := make(chan error, 1)
	go func() {
		_, err := h.s.listThreads(context.Background(), fake, func(Frame) int64 { return 0 })
		walked <- err
	}()
	<-fake.firstServed // the Threads walk's request is in flight
	h.drop()
	h.reconnect()
	h.noRefresh(t, 150*time.Millisecond)
	if got := fake.taskCount(); got != 1 {
		t.Fatalf("the refresh issued a request during the Threads walk (%d)", got)
	}
	close(fake.gate)
	if err := <-walked; err != nil {
		t.Fatal(err)
	}
	h.waitRefresh(t)
	fake.mu.Lock()
	overlapped, n := fake.overlapped, len(fake.tasks)
	fake.mu.Unlock()
	if overlapped {
		t.Fatal("ExecuteTasks calls overlapped")
	}
	if n < 2 {
		t.Fatalf("the refresh never ran after the walk (%d requests)", n)
	}
	mustComplete(t, h.s, "2002", true, "refresh after the walk")
}

// A group the refresh's pages do not reach stays unconfirmed, and Send refuses it.
func TestRefreshUnreachedThreadStaysUnconfirmed(t *testing.T) {
	fake := &refreshFake{reply: func(int) *table.LSTable { return twoGroups(2002) }}
	h := newRefreshHarness(t, fake)
	h.s.handleTableFrames(twoGroups(2002, 3003))
	mustComplete(t, h.s, "2002", true, "premise")
	mustComplete(t, h.s, "3003", true, "premise")
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	mustComplete(t, h.s, "2002", true, "reached")
	mustComplete(t, h.s, "3003", false, "not reached")
	if err := expectErr(h.s, "3003", "3004"); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("send to the unreached group: %v", err)
	}
	if err := expectErr(h.s, "2002", "2003"); err != nil {
		t.Fatalf("send to the reached group: %v", err)
	}
}

// A reconnect storm costs one refresh: the latest reconnect wins and starts are
// at least the debounce apart.
func TestRefreshDebouncesReconnectStorm(t *testing.T) {
	fake := &refreshFake{reply: func(int) *table.LSTable { return groupRows(2, 7) }}
	h := newRefreshHarness(t, fake)
	const debounce = 300 * time.Millisecond
	rosterRefreshDebounce = debounce
	h.s.handleTableFrames(groupRows(2, 7))
	h.drop()
	h.reconnect()
	h.waitRefresh(t)
	for i := 0; i < 4; i++ {
		h.drop()
		h.reconnect()
	}
	h.waitRefresh(t)
	h.noRefresh(t, 2*debounce)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.tasks) != 2 {
		t.Fatalf("requests = %d, want 2 (one per debounce window)", len(fake.tasks))
	}
	if gap := fake.startedAt[1].Sub(fake.startedAt[0]); gap < debounce {
		t.Fatalf("refresh starts %v apart, want at least %v", gap, debounce)
	}
	mustComplete(t, h.s, "2002", true, "after the last reconnect")
}
