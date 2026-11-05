package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

func e2eeEvent(chat, alt waTypes.JID, id string) *events.FBMessage {
	info := waTypes.MessageInfo{
		MessageSource: waTypes.MessageSource{Chat: chat, Sender: chat, RecipientAlt: alt, SenderAlt: alt},
		ID:            id, Timestamp: time.UnixMilli(5000),
	}
	return &events.FBMessage{Info: info, Message: consumerText("hello")}
}

func TestNumericChatIDsStillParse(t *testing.T) {
	for _, jid := range []waTypes.JID{
		waTypes.NewJID("4242", waTypes.MessengerServer),
		{User: "4242", Device: 7, Server: waTypes.MessengerServer},
		waTypes.NewJID("4242", waTypes.GroupServer),
	} {
		s := newTestSession()
		var thread, msg bool
		for _, f := range s.waMessageFrames(e2eeEvent(jid, waTypes.EmptyJID, "ID1")) {
			thread = thread || (f.T == TThread && f.ThreadEvt.ID == "e:4242")
			msg = msg || (f.T == TMessage && f.Message.Thread == "e:4242" && f.Message.ID == "wa:ID1" && f.Message.Encrypted)
		}
		if !thread || !msg {
			t.Errorf("%v: thread=%v message=%v", jid, thread, msg)
		}
	}
}

// Forms whose peer cannot be established from the id itself are not guessed
// at: a wrong guess routes a reply to the wrong person.
func TestMalformedAndAmbiguousChatIDsAreNotGuessed(t *testing.T) {
	for _, jid := range []waTypes.JID{
		{User: "4242.invalid", Server: waTypes.GroupServer},
		{User: "4242:7", Server: waTypes.MessengerServer},
		{User: "4242.0:7", Server: waTypes.MessengerServer},
		{User: "4242abc", Server: waTypes.HiddenUserServer},
		{User: "-4242", Server: waTypes.MessengerServer},
		{User: "0", Server: waTypes.MessengerServer},
	} {
		s := newTestSession()
		alt := waTypes.NewJID("9999", waTypes.MessengerServer) // an alternate address is not used either
		if frames := s.waMessageFrames(e2eeEvent(jid, alt, "IDX")); len(frames) != 0 {
			t.Errorf("%q@%s produced %d frames", jid.User, jid.Server, len(frames))
		}
		if s.deliverEncrypted(e2eeEvent(jid, alt, "IDY")) {
			t.Errorf("%q@%s was acknowledged on first delivery", jid.User, jid.Server)
		}
	}
}

func unparsedEvent(id string) *events.FBMessage {
	return e2eeEvent(waTypes.NewJID("not-a-number-SECRET", waTypes.MessengerServer), waTypes.EmptyJID, id)
}

func TestUnparsedChatIsCountedLoggedAndNotAcked(t *testing.T) {
	testStderr.Reset()
	s := newTestSession()
	var emitted int
	s.emit = func(Frame) int64 { emitted++; return 0 }
	if s.deliverEncrypted(unparsedEvent("M1")) {
		t.Fatal("an undelivered message was acknowledged to the server")
	}
	st := s.listingStats(0, 0, 0)
	if st["e2ee_received"] != 1 || st["e2ee_emitted"] != 0 || st["e2ee_dropped_unparsed_chat"] != 1 {
		t.Fatalf("stats = %v", st)
	}
	log := testStderr.String()
	if !strings.Contains(log, `"code":"e2ee_unparsed_chat_msgr"`) {
		t.Fatalf("log = %q", log)
	}
	if strings.Contains(log, "SECRET") || strings.Contains(log, "not-a-number") {
		t.Fatalf("the log carried the id: %q", log)
	}
}

func TestUnparsedReplayOfManyIDsEachGivesUpAfterExactlyThreeDeliveries(t *testing.T) {
	s := newTestSession()
	const n = 600 // replayed round-robin, more than a small cap
	acked := map[int]int{}
	for round := 1; round <= maxUnparsedRedeliveries; round++ {
		for i := 0; i < n; i++ {
			if s.deliverEncrypted(unparsedEvent(fmt.Sprintf("R%d", i))) {
				acked[i] = round
			}
		}
	}
	for i := 0; i < n; i++ {
		if acked[i] != maxUnparsedRedeliveries {
			t.Fatalf("id %d acknowledged on delivery %d, want %d", i, acked[i], maxUnparsedRedeliveries)
		}
	}
	if st := s.listingStats(0, 0, 0); st["e2ee_unparsed_given_up"] != n {
		t.Fatalf("given up = %d", st["e2ee_unparsed_given_up"])
	}
}

func TestUnparsedTrackingEvictsOnlyTheOldestAndNeverResetsACount(t *testing.T) {
	s := newTestSession()
	s.unparsedCap = 8
	for i := 0; i < 8; i++ {
		s.deliverEncrypted(unparsedEvent(fmt.Sprintf("E%d", i))) // count 1 each
	}
	s.deliverEncrypted(unparsedEvent("E8")) // full: evicts only the oldest (E0)
	if got := s.unparsedCount("E1"); got != 1 {
		t.Fatalf("E1 count = %d, want 1 (a younger entry survives the eviction)", got)
	}
	s.deliverEncrypted(unparsedEvent("E1")) // 2
	if !s.deliverEncrypted(unparsedEvent("E1")) {
		t.Fatal("E1 was not given up on its third delivery")
	}
}

func TestUnparsedChatRedeliveryIsBounded(t *testing.T) {
	s := newTestSession()
	acked := 0
	for i := 0; i < maxUnparsedRedeliveries+3; i++ {
		if s.deliverEncrypted(unparsedEvent("LOOP")) {
			acked++
			break
		}
	}
	if acked != 1 {
		t.Fatalf("the same unparsable message was refused forever")
	}
	st := s.listingStats(0, 0, 0)
	if st["e2ee_dropped_unparsed_chat"] != maxUnparsedRedeliveries || st["e2ee_unparsed_given_up"] != 1 {
		t.Fatalf("stats = %v", st)
	}
}

func TestEncryptedOneToOneMessageIsEmittedAndCounted(t *testing.T) {
	s := newTestSession()
	var got []Frame
	s.emit = func(f Frame) int64 { got = append(got, f); return 0 }
	chat := waTypes.NewJID("4242", waTypes.MessengerServer)
	if !s.deliverEncrypted(e2eeEvent(chat, waTypes.EmptyJID, "OK1")) {
		t.Fatal("a delivered message was not acknowledged")
	}
	var thread, msg bool
	for _, f := range got {
		thread = thread || (f.T == TThread && f.ThreadEvt.ID == "e:4242")
		msg = msg || (f.T == TMessage && f.Message.ID == "wa:OK1" && f.Message.Thread == "e:4242")
	}
	if !thread || !msg {
		t.Fatalf("frames = %+v", got)
	}
	st := s.listingStats(0, 0, 0)
	if st["e2ee_received"] != 1 || st["e2ee_emitted"] != 1 || st["e2ee_dropped_unparsed_chat"] != 0 {
		t.Fatalf("stats = %v", st)
	}
}

// Send reads a thread's kind while the receive path writes it.
func TestSendReadsTheThreadKindUnderTheSessionLock(t *testing.T) {
	s := newTestSession()
	s.waMessageFrames(e2eeEvent(waTypes.NewJID("4242", waTypes.MessengerServer), waTypes.EmptyJID, "K0"))
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			s.waMessageFrames(e2eeEvent(waTypes.NewJID("4242", waTypes.GroupServer), waTypes.EmptyJID, fmt.Sprintf("K%d", i)))
		}
	}()
	for i := 0; i < 500; i++ {
		_ = s.encryptedServer("e:4242")
	}
	<-done
	if got := s.encryptedServer("e:4242"); got != waTypes.GroupServer {
		t.Fatalf("server = %q", got)
	}
	if got := s.encryptedServer("e:9"); got != waTypes.MessengerServer {
		t.Fatalf("unknown thread server = %q", got)
	}
}
