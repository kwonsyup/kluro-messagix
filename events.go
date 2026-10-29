// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"strconv"
	"sync"

	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"

	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// Thread folders that mean "not an accepted chat": a message request, or spam.
const (
	folderPending = "pending"
	folderSpam    = "spam"
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
	s.handleTableEmit(tbl, s.emit)
}

// handleTableEmit builds the frames of a table and emits them inside one hold of
// s.mu, thread frames ahead of their messages as built, so a later row (a live
// rejection, say) cannot land between a frame's build and its emit.
func (s *messagixSession) handleTableEmit(tbl *table.LSTable, emit emitFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, f := range s.buildTableFramesLocked(tbl, fromRequestsDatabase(tbl)) {
		emit(f)
	}
}

// handleRequestTableFrames is handleTableFrames for a table known to come from the
// message-requests database (95): every thread row in it takes the request-source
// mark before its frame is built, so it goes out minimal whether or not the row names
// a folder. Only a later accepted signal clears the mark (a thread row in a folder
// other than pending or spam, or a request row with a status other than 1).
func (s *messagixSession) handleRequestTableFrames(tbl *table.LSTable) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildTableFramesLocked(tbl, true)
}

// requestsDatabaseID is the sync database of message requests.
const requestsDatabaseID = 95

// fromRequestsDatabase reports whether a table is a database-95 sync batch: its sync
// transaction blocks all name database 95. A live push carries no block and is not
// identified here; the folder rule guards those.
func fromRequestsDatabase(tbl *table.LSTable) bool {
	blocks := tbl.GetLSExecuteFirstBlockForSyncTransactionV4()
	if len(blocks) == 0 {
		return false
	}
	for _, b := range blocks {
		if b.DatabaseID != requestsDatabaseID {
			return false
		}
	}
	return true
}

// handleTableFrames builds the frames of a table and returns them for the caller
// to emit; use handleTableEmit where emit order against later rows matters.
func (s *messagixSession) handleTableFrames(tbl *table.LSTable) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildTableFramesLocked(tbl, false)
}

// buildTableFramesLocked applies a table to the session and returns its frames.
// fromRequests says the table is from the message-requests database. The caller
// holds s.mu.
func (s *messagixSession) buildTableFramesLocked(tbl *table.LSTable, fromRequests bool) (frames []Frame) {
	dirty := false
	touched := map[string]bool{}
	for _, c := range tbl.LSCreateOpenToE2EEThreadLink {
		if s.cutover[c.ArmadilloThreadID] != c.OpenThreadID {
			s.cutover[c.ArmadilloThreadID] = c.OpenThreadID
			s.touchUnacceptedLocked(touched, c.ArmadilloThreadID)
			dirty = true
		}
	}
	for _, j := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			s.touchUnacceptedLocked(touched, j.ThreadKey)
			dirty = true
		}
	}
	for _, j := range tbl.LSVerifyHybridThreadExists {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			s.touchUnacceptedLocked(touched, j.ThreadKey)
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

	// A thread row in the "pending" or "spam" folder, or a message request row with
	// status 1, marks the thread unaccepted: it is emitted with Typed=false whatever
	// its type, so the connector qualifies nothing from it. Any other non-empty
	// folder, or a request status other than 1, clears the mark and re-emits the
	// thread typed; an empty folder does neither. The mark is memory only: after a
	// restart a request the listing does not type stays untyped.
	// Two sources are tracked apart: the latest non-empty folder (pending or spam)
	// and the latest request status row. A status row only clears what a status row
	// set, and a folder only clears what a folder set, so a spam folder stays
	// unaccepted next to a status-0 row, as in the library.
	// A mark belongs to the chat, not to one key: it is set on the row's key, read
	// through every related key (the same number as a plaintext and an encrypted
	// thread, a hybrid mapping, a cutover link), and cleared on all of them, so an
	// accepted signal on either form accepts the chat. A change re-emits every
	// existing thread of the chat.
	set := func(marks map[int64]bool, key int64, on bool) {
		changed := false
		if on {
			if !marks[key] {
				marks[key] = true
				changed = true
			}
		} else {
			for _, k := range s.relatedKeysLocked(key) {
				if marks[k] {
					delete(marks, k)
					changed = true
				}
			}
		}
		if changed {
			s.touchRelatedLocked(touched, key)
		}
	}
	if fromRequests {
		for _, th := range tbl.LSDeleteThenInsertThread {
			set(s.requestSource, th.ThreadKey, true)
		}
		for _, th := range tbl.LSUpdateOrInsertThread {
			set(s.requestSource, th.ThreadKey, true)
		}
		for _, th := range tbl.LSVerifyThreadExists {
			set(s.requestSource, th.ThreadKey, true)
		}
	}
	markFolder := func(key int64, folder string) {
		switch folder {
		case "":
		case folderPending, folderSpam:
			set(s.folderMark, key, true)
		default:
			set(s.folderMark, key, false)
			set(s.requestSource, key, false)
		}
	}
	for _, th := range tbl.LSDeleteThenInsertThread {
		markFolder(th.ThreadKey, th.FolderName)
	}
	for _, th := range tbl.LSUpdateOrInsertThread {
		markFolder(th.ThreadKey, th.FolderName)
	}
	for _, th := range tbl.LSVerifyThreadExists {
		markFolder(th.ThreadKey, th.FolderName)
	}
	for _, r := range tbl.LSDeleteThenInsertMessageRequest {
		// MessageRequestStatus 1 means the thread is in the pending folder.
		set(s.statusMark, r.ThreadKey, r.MessageRequestStatus == 1)
		if r.MessageRequestStatus != 1 {
			set(s.requestSource, r.ThreadKey, false)
		}
	}
	upsertThread := func(key int64, typ table.ThreadType, name string, last int64) {
		originalKey := key
		s.rowKeys[originalKey] = true
		if typ.IsWhatsApp() {
			s.encRowKeys[originalKey] = true
		}
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
	// A bare existence row names an encrypted chat the inbox rows may not have.
	for _, th := range tbl.LSVerifyThreadExists {
		if th.ThreadType.IsWhatsApp() {
			upsertThread(th.ThreadKey, th.ThreadType, "", 0)
		}
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
	if s.unacceptedLocked(t.Key) {
		// A message request qualifies for nothing: the connector imports any
		// one-to-one thread that names another person, typed or not, so the frame
		// carries no kind, no type, no participants and no title (usually the
		// stranger's name). The real ones stay in
		// memory and go out when a later row accepts the thread. An encrypted thread
		// is held the same way: any mark of its chat applies to it.
		return Thread{ID: wireID(t.Enc, t.Key), LastActivity: t.LastActivity}
	}
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

// unacceptedLocked reports whether a facebook thread is a message request or
// spam by either source. The caller holds s.mu.
func (s *messagixSession) unacceptedLocked(key int64) bool {
	for _, k := range s.relatedKeysLocked(key) {
		if s.folderMark[k] || s.statusMark[k] || s.requestSource[k] {
			return true
		}
	}
	return false
}

// relatedKeysLocked is the keys of the same chat as key, key included: the whole
// connected set under the same number (a plaintext thread and its encrypted form
// share it), the hybrid mapping either way and the cutover link either way, followed
// to a fixed point (a visited set bounds it). The caller holds s.mu.
func (s *messagixSession) relatedKeysLocked(key int64) []int64 {
	out := []int64{key}
	seen := map[int64]bool{key: true}
	for i := 0; i < len(out); i++ {
		k := out[i]
		add := func(n int64) {
			if !seen[n] {
				seen[n] = true
				out = append(out, n)
			}
		}
		if w, ok := s.fbToWA[k]; ok {
			add(w)
		}
		for fb, w := range s.fbToWA {
			if w == k {
				add(fb)
			}
		}
		if open, ok := s.cutover[k]; ok {
			add(open)
		}
		for armadillo, open := range s.cutover {
			if open == k {
				add(armadillo)
			}
		}
	}
	return out
}

// touchRelatedLocked marks every existing thread of key's chat, plaintext or
// encrypted, for re-emission.
func (s *messagixSession) touchRelatedLocked(touched map[string]bool, key int64) {
	for _, k := range s.relatedKeysLocked(key) {
		for _, enc := range []bool{false, true} {
			if id := wireID(enc, k); s.threads[id] != nil {
				touched[id] = true
			}
		}
	}
}

// touchUnacceptedLocked is touchRelatedLocked for a chat that is unaccepted, used
// when a mapping or link changes which threads belong to it.
func (s *messagixSession) touchUnacceptedLocked(touched map[string]bool, key int64) {
	if s.unacceptedLocked(key) {
		s.touchRelatedLocked(touched, key)
	}
}

// emitKnownThreads emits a thread frame for every thread the helper knows. Each
// frame is built from the current state inside the lock that also covers its
// emit, so a listing never emits an older mark than a live frame already did.
func (s *messagixSession) emitKnownThreads(ctx context.Context, emit emitFunc) error {
	s.mu.Lock()
	ids := make([]string, 0, len(s.threads))
	for id := range s.threads {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	return s.emitThreadIDs(ctx, ids, emit)
}

// initialEmitter turns the frames Start built from the initial table into an
// emitter that runs after ready: the thread frames are rebuilt from current state
// at emit time (so a live rejection in between is not undone), then the other
// frames follow in their built order.
func (s *messagixSession) initialEmitter(frames []Frame) initialEmit {
	var ids []string
	var rest []Frame
	seen := map[string]bool{}
	for _, f := range frames {
		if f.T != TThread {
			rest = append(rest, f)
		} else if !seen[f.ThreadEvt.ID] {
			seen[f.ThreadEvt.ID] = true
			ids = append(ids, f.ThreadEvt.ID)
		}
	}
	if len(ids) == 0 && len(rest) == 0 {
		return nil
	}
	return func(emit emitFunc) {
		ctx := s.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		_ = s.emitThreadIDs(ctx, ids, emit)
		for _, f := range rest {
			emit(f)
		}
	}
}

// emitThreadIDs emits a thread frame for each id, built from the current state
// inside the lock that covers its emit.
func (s *messagixSession) emitThreadIDs(ctx context.Context, ids []string, emit emitFunc) error {
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return err
		}
		if s.beforeListEmit != nil {
			s.beforeListEmit(id)
		}
		s.mu.Lock()
		if t := s.threads[id]; t != nil {
			emit(threadFrame(s.wireThreadLocked(t)))
		}
		s.mu.Unlock()
	}
	return nil
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
	case *events.OfflineSyncPreview:
		s.mu.Lock()
		s.e2eeOffline = evt.Messages
		s.mu.Unlock()
	case *events.OfflineSyncCompleted:
		s.mu.Lock()
		s.e2eeOfflineEnd = true
		s.mu.Unlock()
		s.markStateDirty()
	case *events.Connected:
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

// encryptedCounts reports how many encrypted chats the helper knows of (a thread
// record, a cutover link or a hybrid mapping) and how many of those no table
// has named, so they have no thread record and were not listed.
func (s *messagixSession) encryptedCounts() (known, unlisted int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := map[int64]bool{}
	for k := range s.cutover {
		keys[k] = true
	}
	for _, k := range s.fbToWA {
		keys[k] = true
	}
	for _, t := range s.threads {
		if t.Enc {
			keys[t.Key] = true
		}
	}
	for k := range keys {
		if s.threads[wireID(true, k)] == nil {
			unlisted++
		}
	}
	return len(keys), unlisted
}

// listingStats is the counts-only diagnostic on threads_done.
func (s *messagixSession) listingStats(known, emitted, unlisted int) map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()
	connected := 0
	if s.e2ee != nil && s.e2ee.IsConnected() {
		connected = 1
	}
	offlineDone := 0
	if s.e2eeOfflineEnd {
		offlineDone = 1
	}
	return map[string]int{
		"group1_threads":        len(s.rowKeys),
		"group1_encrypted_rows": len(s.encRowKeys),
		"hybrid_mappings":       len(s.fbToWA),
		"cutover_links":         len(s.cutover),
		"encrypted_known":       known,
		"encrypted_emitted":     emitted,
		"e2ee_connected":        connected,
		"e2ee_offline_events":   s.e2eeOffline,
		"e2ee_offline_done":     offlineDone,
		"resume_sync_rows":      s.resumeRows,
		"group1_range_wait_ms":  s.group1WaitMS,
		"encrypted_unlisted":    unlisted,
	}
}
