// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
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
