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
	s.handleTableFrames(groupRows(3, 1, 7, 8))
	if !wireOf(t, s, "2002").MembersComplete {
		t.Fatal("full roster not complete")
	}
	// A two-person group: self and one other.
	s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:        []*table.LSDeleteThenInsertThread{{ThreadKey: 3003, ThreadType: table.GROUP_THREAD, MemberCount: 2}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 3003, ContactId: 9}},
	})
	if !wireOf(t, s, "3003").MembersComplete {
		t.Fatal("two-person group not complete")
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
		s.handleTableFrames(groupRows(3, 7, 8))
		s.handleTableFrames(delta)
		if wireOf(t, s, "2002").MembersComplete {
			t.Fatalf("%s delta left the roster complete", name)
		}
		s.handleTableFrames(groupRows(3, 7, 8))
		if !wireOf(t, s, "2002").MembersComplete {
			t.Fatalf("%s: a new snapshot did not restore completeness", name)
		}
	}
}

func TestMembersIncompleteAfterReconnectUntilNewSnapshot(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(3, 7, 8))
	s.bumpConnEpoch()
	if wireOf(t, s, "2002").MembersComplete {
		t.Fatal("complete after reconnect")
	}
	s.handleTableFrames(groupRows(3, 7, 8))
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
// any write.
func TestSendExpectRefusals(t *testing.T) {
	s := newTestSession()
	s.client = &messagix.Client{}
	s.handleTableFrames(groupRows(3, 7, 8))
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 6, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, MemberCount: 2}}})
	for name, c := range map[string]struct {
		thread string
		expect []string
	}{
		"mismatch": {"2002", []string{"7", "9"}},
		"subset":   {"2002", []string{"7"}},
		"superset": {"2002", []string{"7", "8", "9"}},
		"empty":    {"2002", []string{}},
		"e2ee":     {"e:6", []string{"9"}},
		"unknown":  {"4004", []string{"9"}},
	} {
		if err := sendWith(s, c.thread, c.expect); codeOf(err) != "roster_changed" {
			t.Fatalf("%s: err %v", name, err)
		}
	}
	// Incomplete after a delta.
	s.handleTableFrames(&table.LSTable{LSAddToMemberCount: []*table.LSAddToMemberCount{{ThreadKey: 2002, IncrementCount: 1}}})
	if err := sendWith(s, "2002", []string{"8", "7"}); codeOf(err) != "roster_changed" {
		t.Fatalf("incomplete roster: %v", err)
	}
}

func TestSendExpectMatchPassesCheck(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(groupRows(3, 7, 8))
	s.mu.Lock()
	err := s.expectLocked("2002", []string{"8", "7"})
	s.mu.Unlock()
	if err != nil {
		t.Fatalf("order-insensitive match refused: %v", err)
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
