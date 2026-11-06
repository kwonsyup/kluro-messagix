// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// groupRows is a typed group thread 2002 with a member count and others 7, 8.
func groupRows(count int64, others ...int64) *table.LSTable {
	tbl := &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 2002, ThreadType: table.GROUP_THREAD, MemberCount: count}}}
	for _, o := range others {
		tbl.LSAddParticipantIdToGroupThread = append(tbl.LSAddParticipantIdToGroupThread,
			&table.LSAddParticipantIdToGroupThread{ThreadKey: 2002, ContactId: o})
	}
	return tbl
}

func wireOf(t *testing.T, s *messagixSession, id string) Thread {
	t.Helper()
	th := s.threads[id]
	if th == nil {
		t.Fatalf("no thread %s", id)
	}
	return s.wireThread(th)
}

func TestMembersCompleteGroup(t *testing.T) {
	s := newTestSession()
	// A two-person group: self (listed too) and one other.
	s.handleTableFrames(groupRows(2, 1, 7))
	if !wireOf(t, s, "2002").MembersComplete {
		t.Fatal("two-person group not complete")
	}
}

// Only self plus exactly one other is ever certified: a fully fresh larger
// group stays copy-only and its expect is unconfirmed, not changed.
func TestMembersLargerGroupNeverComplete(t *testing.T) {
	s := newTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(groupRows(3, 1, 7, 8))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("fresh three-member group complete")
	}
	if err := sendWith(s, "2002", []string{"7", "8"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect on a three-member group: %v", err)
	}
}

func TestMembersIncompleteOnPartialRowsOrNoCount(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(3, 7))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("partial participant rows complete")
	}
	s.handleTableFrames(groupRows(0, 7, 8))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("no member count complete")
	}
}

func TestMembersIncompleteAfterDelta(t *testing.T) {
	for name, delta := range map[string]*table.LSTable{
		"add":    {LSAddToMemberCount: []*table.LSAddToMemberCount{{ThreadKey: 2002, IncrementCount: 1}}},
		"remove": {LSRemoveParticipantFromThread: []*table.LSRemoveParticipantFromThread{{ThreadKey: 2002, ParticipantId: 8}}},
	} {
		s := newTestSession()
		s.handleTableFrames(groupRows(2, 8))
		s.handleTableFrames(delta)
		if wireOf(t, s, "2002").MembersComplete {
			t.Fatalf("%s delta left the roster complete", name)
		}
		s.handleTableFrames(groupRows(2, 8))
		if !wireOf(t, s, "2002").MembersComplete {
			t.Fatalf("%s: a new snapshot did not restore completeness", name)
		}
	}
}

func TestMembersIncompleteAfterReconnectUntilNewSnapshot(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(2, 7))
	s.bumpConnEpoch()
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("complete after reconnect")
	}
	s.handleTableFrames(groupRows(2, 7))
	if !wireOf(t, s, "2002").MembersComplete {
		t.Fatal("not complete after a new DeleteThenInsert")
	}
}

func TestMembersNeverCompleteForOneToOneOrEncrypted(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 5, ThreadType: table.ONE_TO_ONE, MemberCount: 2},
		{ThreadKey: 6, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, MemberCount: 2}}})
	for _, id := range []string{"5", "e:6"} {
		if wireOf(t, s, id).MembersComplete {
			t.Fatalf("%s complete", id)
		}
	}
}

func sendWith(s *messagixSession, thread string, expect []string) error {
	_, err := s.Send(context.Background(), thread, "1", "hi", expect)
	return err
}

// A refused send must not reach the provider: the zero client would panic on
// any write. A confirmed roster that differs is "roster_changed"; anything
// that leaves the roster unconfirmed, or a malformed expect, is
// "roster_unconfirmed". The encrypted case is refused before its own
// "unavailable" check, so no encrypted send is attempted either.
func TestSendExpectRefusals(t *testing.T) {
	s := newTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(groupRows(2, 7))
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 6, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, MemberCount: 2}}})
	for name, c := range map[string]struct {
		thread string
		expect []string
		code   string
	}{
		"mismatch":  {"2002", []string{"9"}, "roster_changed"},
		"superset":  {"2002", []string{"7", "9"}, "roster_changed"},
		"empty":     {"2002", []string{}, "roster_unconfirmed"},
		"duplicate": {"2002", []string{"7", "7"}, "roster_unconfirmed"},
		"e2ee":      {"e:6", []string{"9"}, "roster_unconfirmed"},
		"unknown":   {"4004", []string{"9"}, "roster_unconfirmed"},
	} {
		if err := sendWith(s, c.thread, c.expect); codeOf(err) != c.code {
			t.Fatalf("%s: err %v, want %s", name, err, c.code)
		}
	}
	// Incomplete after a delta.
	s.handleTableFrames(&table.LSTable{LSAddToMemberCount: []*table.LSAddToMemberCount{{ThreadKey: 2002, IncrementCount: 1}}})
	if err := sendWith(s, "2002", []string{"7"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("incomplete roster: %v", err)
	}
}

func TestSendExpectMatchPassesCheck(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(2, 7))
	s.mu.Lock()
	err := s.expectLocked("2002", []string{"7"})
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("match refused: %v", err)
	}
}

// Without expect the loop passes nil through and a one-to-one send is as before;
// ready advertises send_expect.
func TestSendWithoutExpectUnchanged(t *testing.T) {
	fb := &fakeBackend{}
	frames := runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"send","id":2,"thread":"42","otid":"1","text":"hi"}`,
		`{"t":"stop"}`)
	if fb.sentExpect != nil || fb.sentText != "hi" {
		t.Fatalf("expect=%v text=%q", fb.sentExpect, fb.sentText)
	}
	var ready Frame
	for _, f := range frames {
		if f.T == TReady {
			ready = f
		}
	}
	if len(ready.Features) != 1 || ready.Features[0] != "send_expect" {
		t.Fatalf("ready = %+v", frames)
	}
	fb = &fakeBackend{}
	runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"send","id":2,"thread":"2002","otid":"1","text":"hi","expect":["7"]}`,
		`{"t":"stop"}`)
	if len(fb.sentExpect) != 1 || fb.sentExpect[0] != "7" {
		t.Fatalf("expect not passed: %v", fb.sentExpect)
	}
}

func TestRosterDiagnosticCodeOnlyOncePerEpoch(t *testing.T) {
	var buf bytes.Buffer
	old := stderr
	stderr = &buf
	defer func() { stderr = old }()
	s := newTestSession()
	s.handleTableFrames(groupRows(30, 7))
	wireOf(t, s, "2002")
	wireOf(t, s, "2002")
	if got := buf.String(); strings.Count(got, "roster.") != 1 || !strings.Contains(got, `"roster.t2.mc9.p1.c0"`) {
		t.Fatalf("log = %q", got)
	}
	s.bumpConnEpoch()
	wireOf(t, s, "2002")
	if strings.Count(buf.String(), "roster.") != 2 {
		t.Fatalf("no new epoch line: %q", buf.String())
	}
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		var m map[string]string
		if json.Unmarshal([]byte(line), &m) != nil || !regexp.MustCompile(`^[a-z0-9_.]{1,64}$`).MatchString(m["code"]) {
			t.Fatalf("bad line %q", line)
		}
	}
}

// othersOf is the non-self participant ids on the wire frame.
func othersOf(th Thread) []string {
	var out []string
	for _, p := range th.Participants {
		if !p.Self {
			out = append(out, p.ID)
		}
	}
	return out
}

func expectErr(s *messagixSession, thread string, expect ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expectLocked(thread, expect)
}

// Codex P1: self plus 7 and 8, reconnect after 8 is replaced by 9, then a
// count-3 snapshot naming only 7. The cached 8 is from an older epoch and must
// not fill the count; and a three-member group is never certified anyway,
// even once every member is fresh.
func TestMembersStaleIdentityAfterReconnectNotComplete(t *testing.T) {
	s := newTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(groupRows(3, 7, 8))
	s.bumpConnEpoch()
	s.handleTableFrames(groupRows(3, 7))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("stale participant 8 certified a new snapshot")
	}
	for _, expect := range [][]string{{"7", "8"}, {"7", "9"}, {"7"}} {
		if err := sendWith(s, "2002", expect); codeOf(err) != "roster_unconfirmed" {
			t.Fatalf("expect %v: err %v", expect, err)
		}
	}
	s.handleTableFrames(groupRows(3, 9))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("fresh three-member group complete")
	}
	if err := sendWith(s, "2002", []string{"9", "7"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect on a fresh three-member group: %v", err)
	}
}

// The Marketplace shape: self plus 7, reconnect after 7 is replaced by 9.
func TestMembersTwoPersonReplacedAcrossReconnect(t *testing.T) {
	s := newTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:        []*table.LSDeleteThenInsertThread{{ThreadKey: 2002, ThreadType: table.MARKETPLACE, MemberCount: 2}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 2002, ContactId: 7}},
	})
	if !wireOf(t, s, "2002").MembersComplete {
		t.Fatal("premise: two-person roster not complete")
	}
	s.bumpConnEpoch()
	// A count-2 snapshot with no participant row: only the stale 7 is known.
	snap := &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 2002, ThreadType: table.MARKETPLACE, MemberCount: 2}}}
	s.handleTableFrames(snap)
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("stale participant 7 certified a count-2 snapshot")
	}
	if err := sendWith(s, "2002", []string{"7"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("stale expect: %v", err)
	}
	// 9 is seen fresh in this epoch.
	s.handleTableFrames(&table.LSTable{LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 2002, ContactId: 9}}})
	th := wireOf(t, s, "2002")
	if !th.MembersComplete || strings.Join(othersOf(th), ",") != "9" {
		t.Fatalf("complete=%v others=%v", th.MembersComplete, othersOf(th))
	}
	if err := sendWith(s, "2002", []string{"7"}); codeOf(err) != "roster_changed" {
		t.Fatalf("expect [7] after replacement: %v", err)
	}
	if err := expectErr(s, "2002", "9"); err != nil {
		t.Fatalf("expect [9] refused: %v", err)
	}
	// Another reconnect makes 9 stale too until it is seen again.
	s.bumpConnEpoch()
	s.handleTableFrames(snap)
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("9 from an older epoch certified the roster")
	}
	if err := sendWith(s, "2002", []string{"9"}); codeOf(err) != "roster_unconfirmed" {
		t.Fatalf("expect [9] with a stale roster: %v", err)
	}
}

// A participant row buffered before its thread keeps the epoch it arrived in.
func TestMembersBufferedParticipantKeepsItsEpoch(t *testing.T) {
	early := &table.LSTable{LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 2002, ContactId: 7}}}
	s := newTestSession()
	s.handleTableFrames(early)
	s.bumpConnEpoch()
	s.handleTableFrames(groupRows(2))
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("buffered row from an older epoch certified the roster")
	}
	s = newTestSession()
	s.handleTableFrames(early)
	s.handleTableFrames(groupRows(2))
	if !wireOf(t, s, "2002").MembersComplete {
		t.Fatal("buffered row from this epoch not counted")
	}
}

func TestMembersRemovalRowRemovesParticipant(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(3, 7, 8))
	s.handleTableFrames(&table.LSTable{LSRemoveParticipantFromThread: []*table.LSRemoveParticipantFromThread{{ThreadKey: 2002, ParticipantId: 8}}})
	th := wireOf(t, s, "2002")
	if th.MembersComplete || strings.Join(othersOf(th), ",") != "7" {
		t.Fatalf("after removal complete=%v others=%v", th.MembersComplete, othersOf(th))
	}
	// A new count-2 snapshot in the same epoch is complete with 7 alone.
	s.handleTableFrames(groupRows(2))
	th = wireOf(t, s, "2002")
	if !th.MembersComplete || strings.Join(othersOf(th), ",") != "7" {
		t.Fatalf("after snapshot complete=%v others=%v", th.MembersComplete, othersOf(th))
	}
	if err := expectErr(s, "2002", "7", "8"); codeOf(err) != "roster_changed" {
		t.Fatalf("removed participant still expected: %v", err)
	}
	if err := expectErr(s, "2002", "7"); err != nil {
		t.Fatalf("remaining participant refused: %v", err)
	}
	// A removal for a thread not yet known drops the buffered row.
	s.handleTableFrames(&table.LSTable{LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 3003, ContactId: 5}}})
	s.handleTableFrames(&table.LSTable{LSRemoveParticipantFromThread: []*table.LSRemoveParticipantFromThread{{ThreadKey: 3003, ParticipantId: 5}}})
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 3003, ThreadType: table.GROUP_THREAD, MemberCount: 2}}})
	if th := wireOf(t, s, "3003"); th.MembersComplete || len(othersOf(th)) != 0 {
		t.Fatalf("removed buffered row survived: %+v", th)
	}
}

func TestMembersFreshTwoPersonHappyPath(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:        []*table.LSDeleteThenInsertThread{{ThreadKey: 2002, ThreadType: table.MARKETPLACE, MemberCount: 2}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 2002, ContactId: 1}, {ThreadKey: 2002, ContactId: 9}},
	})
	th := wireOf(t, s, "2002")
	if !th.MembersComplete || strings.Join(othersOf(th), ",") != "9" {
		t.Fatalf("complete=%v others=%v", th.MembersComplete, othersOf(th))
	}
	if err := expectErr(s, "2002", "9"); err != nil {
		t.Fatalf("expect [9] refused: %v", err)
	}
}

// The diagnostic's p<n> counts fresh others only.
func TestRosterDiagnosticCountsFreshOthers(t *testing.T) {
	var buf bytes.Buffer
	old := stderr
	stderr = &buf
	defer func() { stderr = old }()
	s := newTestSession()
	s.handleTableFrames(groupRows(2, 7))
	wireOf(t, s, "2002")
	s.bumpConnEpoch()
	s.handleTableFrames(groupRows(2))
	wireOf(t, s, "2002")
	if got := buf.String(); !strings.Contains(got, `"roster.t2.mc2.p1.c1"`) || !strings.Contains(got, `"roster.t2.mc2.p0.c0"`) {
		t.Fatalf("log = %q", got)
	}
}
