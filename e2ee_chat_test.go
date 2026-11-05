package main

import (
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

func TestEveryRealChatFormParsesToItsFBID(t *testing.T) {
	cases := []struct {
		name string
		jid  waTypes.JID
		want string
	}{
		{"msgr user", waTypes.NewJID("4242", waTypes.MessengerServer), "e:4242"},
		{"msgr device form", waTypes.JID{User: "4242", Device: 7, Server: waTypes.MessengerServer}, "e:4242"},
		{"msgr agent form", waTypes.JID{User: "4242", RawAgent: 3, Server: waTypes.MessengerServer}, "e:4242"},
		{"msgr user text with device suffix", waTypes.JID{User: "4242:7", Server: waTypes.MessengerServer}, "e:4242"},
		{"msgr user text with agent and device", waTypes.JID{User: "4242.0:7", Server: waTypes.MessengerServer}, "e:4242"},
		{"lid", waTypes.NewJID("4242", waTypes.HiddenUserServer), "e:4242"},
		{"group", waTypes.NewJID("4242", waTypes.GroupServer), "e:4242"},
	}
	for _, c := range cases {
		s := newTestSession()
		frames := s.waMessageFrames(e2eeEvent(c.jid, waTypes.EmptyJID, "ID1"))
		var thread, msg bool
		for _, f := range frames {
			if f.T == TThread && f.ThreadEvt.ID == c.want {
				thread = true
			}
			if f.T == TMessage && f.Message.Thread == c.want && f.Message.ID == "wa:ID1" && f.Message.Encrypted {
				msg = true
			}
		}
		if !thread || !msg {
			t.Errorf("%s: thread=%v message=%v frames=%d", c.name, thread, msg, len(frames))
		}
	}
}

func TestLIDChatWithoutANumericIDUsesItsMessengerAlternate(t *testing.T) {
	s := newTestSession()
	chat := waTypes.NewJID("abc-not-numeric", waTypes.HiddenUserServer)
	alt := waTypes.NewJID("4242", waTypes.MessengerServer)
	frames := s.waMessageFrames(e2eeEvent(chat, alt, "ID2"))
	if len(frames) == 0 || frames[0].T != TThread || frames[0].ThreadEvt.ID != "e:4242" {
		t.Fatalf("frames = %+v", frames)
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
