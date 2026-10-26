// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"strconv"
	"sync"

	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const msgIndexCap = 50000

// pageCollector gathers the table rows answering one history request.
type pageCollector struct {
	mu     sync.Mutex
	upsert *table.UpsertMessages
	max    int
	done   chan struct{}
	once   sync.Once
}

func (c *pageCollector) finish() { c.once.Do(func() { close(c.done) }) }

// handleTable turns a live table into frames: thread facts first, then
// messages, edits and unsends. History pages requested through a collector are
// routed to it instead.
func (s *messagixSession) handleTable(tbl *table.LSTable) {
	for _, f := range s.handleTableFrames(tbl) {
		s.emit(f)
	}
}

func (s *messagixSession) handleTableFrames(tbl *table.LSTable) (frames []Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dirty := false
	for _, c := range tbl.LSCreateOpenToE2EEThreadLink {
		if s.cutover[c.ArmadilloThreadID] != c.OpenThreadID {
			s.cutover[c.ArmadilloThreadID] = c.OpenThreadID
			dirty = true
		}
	}
	for _, j := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			dirty = true
		}
	}
	for _, j := range tbl.LSVerifyHybridThreadExists {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			dirty = true
		}
	}
	for _, c := range tbl.LSDeleteThenInsertContact {
		if c.Name != "" {
			s.names[c.GetFBID()] = c.Name
		}
	}
	for _, c := range tbl.LSVerifyContactRowExists {
		if c.Name != "" {
			s.names[c.GetFBID()] = c.Name
		}
	}

	touched := map[string]bool{}
	upsertThread := func(key int64, typ table.ThreadType, name string, last int64) {
		originalKey := key
		enc := typ.IsWhatsApp()
		if mapped, ok := s.fbToWA[key]; ok {
			key, enc = mapped, true
		}
		if key == 0 {
			return
		}
		t := s.thread(enc, key)
		for _, id := range s.participants[originalKey] {
			t.addParticipant(id)
		}
		delete(s.participants, originalKey)
		if typ != table.UNKNOWN_THREAD_TYPE {
			t.Typed = true
			if typ.IsOneToOne() {
				t.Kind = "user"
				t.addParticipant(key)
			} else {
				t.Kind = "group"
			}
		}
		if name != "" {
			t.Title = name
		}
		t.LastActivity = max(t.LastActivity, last)
		touched[wireID(enc, key)] = true
	}
	for _, th := range tbl.LSDeleteThenInsertThread {
		upsertThread(th.ThreadKey, th.ThreadType, th.ThreadName, th.LastActivityTimestampMs)
	}
	for _, th := range tbl.LSUpdateOrInsertThread {
		upsertThread(th.ThreadKey, th.ThreadType, th.ThreadName, th.LastActivityTimestampMs)
	}
	for _, p := range tbl.LSAddParticipantIdToGroupThread {
		key, enc := s.mapKey(p.ThreadKey)
		if t := s.threads[wireID(enc, key)]; t != nil {
			t.addParticipant(p.ContactId)
			touched[wireID(enc, key)] = true
		} else {
			s.participants[p.ThreadKey] = append(s.participants[p.ThreadKey], p.ContactId)
		}
	}
	for _, n := range tbl.LSSyncUpdateThreadName {
		key, enc := s.mapKey(n.ThreadKey)
		if t := s.threads[wireID(enc, key)]; t != nil && n.ThreadName != "" {
			t.Title = n.ThreadName
			touched[wireID(enc, key)] = true
		}
	}
	for id := range touched {
		frames = append(frames, threadFrame(s.wireThreadLocked(s.threads[id])))
	}

	upserts, inserts := tbl.WrapMessages()
	for key, up := range upserts {
		if c := s.collectors[key]; c != nil {
			c.mu.Lock()
			c.upsert = c.upsert.Join(up)
			c.mu.Unlock()
			continue
		}
		for _, m := range up.Messages {
			frames = append(frames, s.messageFrameLocked(m)...)
		}
	}
	for _, m := range inserts {
		frames = append(frames, s.messageFrameLocked(m)...)
	}
	for _, rng := range tbl.LSUpdateExistingMessageRange {
		if c := s.collectors[rng.ThreadKey]; c != nil {
			c.mu.Lock()
			if c.upsert != nil && c.upsert.Range.MinTimestampMs == rng.TimestampMS {
				if len(c.upsert.Messages) == 0 || (rng.UnknownBool2 && !rng.UnknownBool3) {
					c.upsert.Range.HasMoreBefore = false
				} else {
					c.upsert.Range.HasMoreAfter = false
				}
				c.finish()
			}
			c.mu.Unlock()
		}
	}
	for _, e := range tbl.LSEditMessage {
		thread, ok := s.msgIndex[e.MessageID]
		if !ok {
			continue
		}
		// The edit row names no editor, and the msgIndex holds only the thread:
		// By comes solely from the event's own sender, so here it stays empty.
		frames = append(frames, Frame{T: TEdit, Edit: &Edit{Thread: thread, ID: e.MessageID, Text: e.Text, AtMS: nowMS()}})
	}
	for _, d := range tbl.LSDeleteMessage {
		key, enc := s.mapKey(d.ThreadKey)
		thread := wireID(enc, key)
		if indexed := s.msgIndex[d.MessageId]; indexed != "" {
			thread = indexed
		}
		// By may name only the actor from the unsend event itself; the delete
		// row carries none (LSDeleteMessage is just thread key and message id),
		// so By stays empty rather than crediting the original author.
		frames = append(frames, Frame{T: TUnsend, Unsend: &Unsend{Thread: thread, ID: d.MessageId}})
	}
	if dirty {
		s.stateDirty = true
	}
	return frames
}

// mapKey resolves a facebook thread key through a hybrid mapping or an already
// known encrypted thread. Participant and name rows carry no encryption type.
func (s *messagixSession) mapKey(key int64) (int64, bool) {
	if mapped, ok := s.fbToWA[key]; ok {
		return mapped, true
	}
	if s.threads[wireID(true, key)] != nil {
		return key, true
	}
	return key, false
}

// thread returns (creating) the merged thread record. The caller holds s.mu.
func (s *messagixSession) thread(enc bool, key int64) *threadInfo {
	id := wireID(enc, key)
	t := s.threads[id]
	if t == nil {
		t = &threadInfo{Key: key, Enc: enc, Kind: "group"}
		s.threads[id] = t
	}
	for _, p := range s.participants[key] {
		t.addParticipant(p)
	}
	delete(s.participants, key)
	return t
}

func (t *threadInfo) addParticipant(id int64) {
	if id == 0 {
		return
	}
	for _, p := range t.Participants {
		if p == id {
			return
		}
	}
	t.Participants = append(t.Participants, id)
}

func threadFrame(t Thread) Frame { return Frame{T: TThread, ThreadEvt: &t} }

// wireThreadLocked builds the protocol thread. A one-to-one thread names the
// other person; an encrypted thread names the open thread it continues.
func (s *messagixSession) wireThreadLocked(t *threadInfo) Thread {
	out := Thread{ID: wireID(t.Enc, t.Key), Kind: t.Kind, Typed: t.Typed, Title: t.Title, Encrypted: t.Enc, LastActivity: t.LastActivity}
	if t.Enc {
		if open, ok := s.cutover[t.Key]; ok {
			out.Predecessor = wireID(false, open)
		}
	}
	self := strconv.FormatInt(s.selfID, 10)
	out.Participants = append(out.Participants, Person{ID: self, Name: s.selfName, Self: true})
	for _, p := range t.Participants {
		if p == s.selfID {
			continue
		}
		out.Participants = append(out.Participants, Person{ID: strconv.FormatInt(p, 10), Name: s.names[p]})
	}
	return out
}

func (s *messagixSession) wireThread(t *threadInfo) Thread {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wireThreadLocked(t)
}

func (s *messagixSession) sortedThreadFrames() []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []Frame
	for _, t := range s.threads {
		out = append(out, threadFrame(s.wireThreadLocked(t)))
	}
	return out
}

// messageFrameLocked converts one lightspeed message. The caller holds s.mu.
func (s *messagixSession) messageFrameLocked(m *table.WrappedMessage) []Frame {
	if m == nil || m.LSInsertMessage == nil || m.MessageId == "" || m.IsUnsent {
		return nil
	}
	key, enc := s.mapKey(m.ThreadKey)
	thread := wireID(enc, key)
	msg := convertLSMessage(m)
	msg.Thread = thread
	msg.Out = m.SenderId == s.selfID
	s.indexMessageLocked(msg.ID, thread)
	// The thread must be known before its first message.
	var frames []Frame
	if s.threads[thread] == nil {
		t := s.thread(enc, key)
		frames = append(frames, threadFrame(s.wireThreadLocked(t)))
	}
	return append(frames, Frame{T: TMessage, Message: &msg})
}

func convertLSMessage(m *table.WrappedMessage) Message {
	out := Message{
		ID: m.MessageId, Sender: strconv.FormatInt(m.SenderId, 10), Text: m.Text,
		ReplyTo: m.ReplySourceId, SentMS: m.TimestampMs, OTID: m.OfflineThreadingId,
	}
	switch {
	case len(m.Stickers) > 0 || m.StickerId != 0:
		out.Media = "sticker"
	case len(m.XMAAttachments) > 0:
		out.Media = "link"
	case len(m.BlobAttachments) > 0 || len(m.Attachments) > 0:
		out.Media = "attachment"
	}
	return out
}

// indexMessageLocked records a message's thread so later edit and unsend rows,
// which carry only the message id, can name the right thread. The sender is
// deliberately not recorded: it is the author, never the actor.
func (s *messagixSession) indexMessageLocked(id, thread string) {
	if _, ok := s.msgIndex[id]; !ok {
		s.msgOrder = append(s.msgOrder, id)
		if len(s.msgOrder) > msgIndexCap {
			drop := len(s.msgOrder) - msgIndexCap/2
			for _, old := range s.msgOrder[:drop] {
				delete(s.msgIndex, old)
			}
			s.msgOrder = append([]string(nil), s.msgOrder[drop:]...)
		}
	}
	s.msgIndex[id] = thread
}

// handleWAEvent handles the encrypted-chat client's events. It returns whether
// the event was handled, which acknowledges the message to the server.
func (s *messagixSession) handleWAEvent(raw any) bool {
	switch evt := raw.(type) {
	case *events.FBMessage:
		return s.deliverEncrypted(evt)
	case *events.Connected, *events.OfflineSyncCompleted:
		s.markStateDirty()
	case *events.Disconnected:
		logCode("info", "e2ee_disconnected")
	case *events.LoggedOut:
		logCode("warn", "e2ee_logged_out")
		s.mu.Lock()
		s.waDeviceJID = ""
		s.waDevice = nil
		s.mu.Unlock()
		s.markStateDirty()
	}
	return true
}

// deliverEncrypted emits one encrypted-chat event and only acknowledges it to
// the server once the connector has durably committed it. whatsmeow keeps an
// event whose handler returns false: it sends no receipt, so the server
// redelivers, and the decrypted plaintext stays in the event buffer, so the
// redelivered copy reaches this handler again instead of being dropped as
// already processed. Blocking here is the backpressure: the server's next
// messages queue behind one the connector has not committed.
func (s *messagixSession) deliverEncrypted(evt *events.FBMessage) bool {
	s.mu.Lock()
	if s.stopping || s.logoutOnly {
		s.mu.Unlock()
		return false
	}
	s.workWG.Add(1)
	s.mu.Unlock()
	defer s.workWG.Done()
	frames := s.waMessageFrames(evt)
	if len(frames) == 0 {
		return true
	}
	// Seal first: this message's decryption advanced Signal sessions in the
	// device store, and the connector must seal that state before it commits
	// and acks the message.
	if !s.sealState() {
		logCode("warn", "state_seal_failed")
		return false
	}
	var seqs []int64
	for _, f := range frames {
		f.waitAck = true
		seq := s.emit(f)
		if seq < 0 {
			return false
		}
		if seq > 0 {
			seqs = append(seqs, seq)
		}
	}
	if len(seqs) == 0 {
		return true // a bare thread or reaction row: nothing is committed
	}
	for _, seq := range seqs {
		if !s.acks.wait(seq) {
			return false
		}
	}
	return true
}

func (s *messagixSession) waMessageFrames(evt *events.FBMessage) []Frame {
	chat := evt.Info.Chat
	chatID, err := strconv.ParseInt(chat.User, 10, 64)
	if err != nil || chatID == 0 {
		return nil
	}
	senderID, _ := strconv.ParseInt(evt.Info.Sender.User, 10, 64)
	app, _ := evt.Message.(*waConsumerApplication.ConsumerApplication)

	s.mu.Lock()
	defer s.mu.Unlock()
	t := s.thread(true, chatID)
	var frames []Frame
	known := t.Typed
	oldKind := t.Kind
	participants := len(t.Participants)
	if chat.Server == waTypes.GroupServer {
		t.Kind = "group"
		if senderID != s.selfID {
			t.addParticipant(senderID)
		}
	} else {
		t.Kind = "user"
		t.addParticipant(chatID)
	}
	t.Typed = true // the Messenger JID distinguishes groups from one-to-one chats
	if !known || oldKind != t.Kind || len(t.Participants) != participants {
		frames = append(frames, threadFrame(s.wireThreadLocked(t)))
	}
	thread := wireID(true, chatID)
	waID := "wa:" + evt.Info.ID

	if app != nil {
		// Both edit and unsend name the actor: senderID is the event's own
		// sender, never the original message's author from the msgIndex.
		switch content := app.GetPayload().GetContent().GetContent().(type) {
		case *waConsumerApplication.ConsumerApplication_Content_EditMessage:
			target := "wa:" + content.EditMessage.GetKey().GetID()
			text := content.EditMessage.GetMessage().GetText()
			return append(frames, Frame{T: TEdit, Edit: &Edit{Thread: thread, ID: target, Text: text, By: strconv.FormatInt(senderID, 10), AtMS: content.EditMessage.GetTimestampMS()}})
		case *waConsumerApplication.ConsumerApplication_Content_ReactionMessage:
			return frames
		}
		if rev := app.GetPayload().GetApplicationData().GetRevoke(); rev != nil {
			target := "wa:" + rev.GetKey().GetID()
			return append(frames, Frame{T: TUnsend, Unsend: &Unsend{Thread: thread, ID: target, By: strconv.FormatInt(senderID, 10)}})
		}
	}
	text, media, reply := consumerContent(app)
	if app == nil {
		media = "attachment"
	}
	msg := Message{
		Thread: thread, ID: waID, Sender: strconv.FormatInt(senderID, 10), Out: evt.Info.IsFromMe || senderID == s.selfID,
		Text: text, Media: media, ReplyTo: reply, SentMS: evt.Info.Timestamp.UnixMilli(), Encrypted: true,
	}
	s.indexMessageLocked(waID, thread)
	return append(frames, Frame{T: TMessage, Message: &msg})
}

// consumerContent reads the text, the media kind and the replied-to id of a
// decrypted consumer message. Media bytes are never fetched.
func consumerContent(app *waConsumerApplication.ConsumerApplication) (text, media, reply string) {
	content := app.GetPayload().GetContent()
	switch c := content.GetContent().(type) {
	case *waConsumerApplication.ConsumerApplication_Content_MessageText:
		text = c.MessageText.GetText()
	case *waConsumerApplication.ConsumerApplication_Content_ExtendedTextMessage:
		text = c.ExtendedTextMessage.GetText().GetText()
		media = "link"
	case *waConsumerApplication.ConsumerApplication_Content_ImageMessage:
		text, media = c.ImageMessage.GetCaption().GetText(), "photo"
	case *waConsumerApplication.ConsumerApplication_Content_VideoMessage:
		text, media = c.VideoMessage.GetCaption().GetText(), "video"
	case *waConsumerApplication.ConsumerApplication_Content_AudioMessage:
		media = "audio"
	case *waConsumerApplication.ConsumerApplication_Content_DocumentMessage:
		media = "file"
	case *waConsumerApplication.ConsumerApplication_Content_StickerMessage:
		media = "sticker"
	case nil:
	default:
		media = "attachment"
	}
	return text, media, ""
}
