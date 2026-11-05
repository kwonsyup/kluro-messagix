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

func unparsedEvent(id string) *events.FBMessage {
	return e2eeEvent(waTypes.NewJID("not-a-number-SECRET", waTypes.MessengerServer), waTypes.EmptyJID, id)
}

func TestUnparsedChatIsCountedLoggedAndStillAcked(t *testing.T) {
	testStderr.Reset()
	s := newTestSession()
	var emitted int
	s.emit = func(Frame) int64 { emitted++; return 0 }
	if !s.deliverEncrypted(unparsedEvent("M1")) {
		t.Fatal("behaviour changed: the message is no longer acknowledged as in the shipped helper")
	}
	if emitted != 0 {
		t.Fatalf("emitted %d frames for an unreadable chat", emitted)
	}
	st := s.listingStats(0, 0, 0)
	if st["e2ee_received"] != 1 || st["e2ee_emitted"] != 0 || st["e2ee_dropped_unparsed_chat"] != 1 {
		t.Fatalf("stats = %v", st)
	}
	if _, ok := st["e2ee_unparsed_given_up"]; ok {
		t.Fatal("the redelivery counter should be gone")
	}
	log := testStderr.String()
	if !strings.Contains(log, `"code":"e2ee_unparsed_chat_msgr"`) {
		t.Fatalf("log = %q", log)
	}
	if strings.Contains(log, "SECRET") || strings.Contains(log, "not-a-number") {
		t.Fatalf("the log carried the id: %q", log)
	}
}

// The parse is the shipped one, byte for byte in behaviour.
func TestChatParseMatchesTheShippedRule(t *testing.T) {
	for user, want := range map[string]bool{
		"4242": true, "-4242": true, "0": false, "": false, "4242:7": false, "4242.0": false, "4242abc": false, "abc": false,
	} {
		if _, ok := e2eeChatKey(waTypes.NewJID(user, waTypes.MessengerServer)); ok != want {
			t.Errorf("%q: ok=%v want %v", user, ok, want)
		}
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
