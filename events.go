// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"fmt"
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
	s.handleTableEmitAt(tbl, 0, emit)
}

// handleTableEmitAt is handleTableEmit for the reply to a request issued in
// connection epoch issued: its roster rows count as seen in that epoch, so a
// reply issued before a reconnect never certifies a roster after it. Zero
// means the current epoch.
func (s *messagixSession) handleTableEmitAt(tbl *table.LSTable, issued int64, emit emitFunc) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if issued == 0 {
		issued = s.connEpoch
	}
	for _, f := range s.buildTableFramesLocked(tbl, issued) {
		emit(f)
	}
}

// handleTableFrames builds the frames of a table and returns them for the caller
// to emit; use handleTableEmit where emit order against later rows matters.
func (s *messagixSession) handleTableFrames(tbl *table.LSTable) []Frame {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buildTableFramesLocked(tbl, s.connEpoch)
}

// buildTableFramesLocked applies a table to the session and returns its frames.
// Its roster rows count as seen in epoch. The caller holds s.mu.
func (s *messagixSession) buildTableFramesLocked(tbl *table.LSTable, epoch int64) (frames []Frame) {
	dirty := false
	touched := map[string]bool{}
	// supersede re-emits an open thread that an encrypted record now continues;
	// membersCompleteLocked never certifies it again.
	supersede := func(key int64) {
		if t := s.threads[wireID(false, key)]; t != nil {
			t.RosterEpoch = 0
			touched[wireID(false, key)] = true
		}
	}
	for _, c := range tbl.LSCreateOpenToE2EEThreadLink {
		if s.cutover[c.ArmadilloThreadID] != c.OpenThreadID {
			s.cutover[c.ArmadilloThreadID] = c.OpenThreadID
			dirty = true
			supersede(c.OpenThreadID)
		}
	}
	for _, j := range tbl.LSUpdateThreadAuthorityAndMappingWithOTIDFromJID {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			dirty = true
			supersede(j.ThreadKey)
		}
	}
	for _, j := range tbl.LSVerifyHybridThreadExists {
		if s.fbToWA[j.ThreadKey] != j.ThreadJID {
			s.fbToWA[j.ThreadKey] = j.ThreadJID
			dirty = true
			supersede(j.ThreadKey)
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
	set := func(marks map[int64]bool, key int64, on bool) {
		before := s.unacceptedLocked(key)
		if on {
			marks[key] = true
		} else {
			delete(marks, key)
		}
		if s.unacceptedLocked(key) != before && s.threads[wireID(false, key)] != nil {
			touched[wireID(false, key)] = true
		}
	}
	markFolder := func(key int64, folder string) {
		switch folder {
		case "":
		case folderPending, folderSpam:
			set(s.folderMark, key, true)
		default:
			set(s.folderMark, key, false)
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
		s.takePendingLocked(t, originalKey)
		if typ != table.UNKNOWN_THREAD_TYPE {
			t.Typed = true
			t.RawType = int64(typ)
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
	// A bulk reset empties the roster before this table's participant rows.
	for _, r := range tbl.LSRemoveAllParticipantsForThread {
		s.resetParticipantsLocked(r.ThreadKey, touched)
	}
	for _, p := range tbl.LSAddParticipantIdToGroupThread {
		key, enc := s.mapKey(p.ThreadKey)
		if t := s.threads[wireID(enc, key)]; t != nil {
			t.seeParticipant(p.ContactId, epoch)
			touched[wireID(enc, key)] = true
		} else {
			s.participants[p.ThreadKey] = append(s.participants[p.ThreadKey], pendingParticipant{p.ContactId, epoch})
		}
	}
	// A snapshot row with a count marks the roster as taken in this epoch; a
	// delta after it (same table or later) leaves the roster unknown.
	for _, th := range tbl.LSDeleteThenInsertThread {
		key, enc := s.mapKey(th.ThreadKey)
		if t := s.threads[wireID(enc, key)]; t != nil {
			// A snapshot without a count replaces the old one: unknown.
			t.MemberCount, t.RosterEpoch = th.MemberCount, 0
			if th.MemberCount > 0 {
				t.RosterEpoch = epoch
			}
		}
	}
	for _, d := range tbl.LSAddToMemberCount {
		s.clearRosterLocked(d.ThreadKey, touched)
	}
	for _, d := range tbl.LSRemoveParticipantFromThread {
		s.removeParticipantLocked(d.ThreadKey, d.ParticipantId, touched)
		s.clearRosterLocked(d.ThreadKey, touched)
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
	s.takePendingLocked(t, key)
	return t
}

// takePendingLocked moves the participant rows buffered under key onto t,
// keeping the epoch each row arrived in.
func (s *messagixSession) takePendingLocked(t *threadInfo, key int64) {
	for _, p := range s.participants[key] {
		t.seeParticipant(p.ID, p.Epoch)
	}
	delete(s.participants, key)
}

// seeParticipant records a participant row seen in epoch.
func (t *threadInfo) seeParticipant(id, epoch int64) {
	if id == 0 {
		return
	}
	t.addParticipant(id)
	if t.Seen == nil {
		t.Seen = map[int64]int64{}
	}
	t.Seen[id] = max(t.Seen[id], epoch)
}

// removeParticipantLocked applies a removal row: the participant leaves the
// thread and any buffered row for it is dropped.
func (s *messagixSession) removeParticipantLocked(threadKey, id int64, touched map[string]bool) {
	if pend := s.participants[threadKey]; pend != nil {
		kept := pend[:0]
		for _, p := range pend {
			if p.ID != id {
				kept = append(kept, p)
			}
		}
		s.participants[threadKey] = kept
	}
	key, enc := s.mapKey(threadKey)
	t := s.threads[wireID(enc, key)]
	if t == nil {
		return
	}
	delete(t.Seen, id)
	for i, p := range t.Participants {
		if p == id {
			t.Participants = append(t.Participants[:i], t.Participants[i+1:]...)
			touched[wireID(enc, key)] = true
			return
		}
	}
}

// resetParticipantsLocked applies a remove-all row: the thread's participants,
// their freshness, its buffered rows and its snapshot are all dropped. A typed
// one-to-one thread keeps the other person, who is its key.
func (s *messagixSession) resetParticipantsLocked(threadKey int64, touched map[string]bool) {
	delete(s.participants, threadKey)
	key, enc := s.mapKey(threadKey)
	t := s.threads[wireID(enc, key)]
	if t == nil {
		return
	}
	t.Participants, t.Seen, t.RosterEpoch = nil, nil, 0
	if t.Typed && t.Kind == "user" {
		t.addParticipant(t.Key)
	}
	touched[wireID(enc, key)] = true
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

// clearRosterLocked marks a thread's roster unknown after a member delta.
func (s *messagixSession) clearRosterLocked(threadKey int64, touched map[string]bool) {
	key, enc := s.mapKey(threadKey)
	if t := s.threads[wireID(enc, key)]; t != nil && t.RosterEpoch != 0 {
		t.RosterEpoch = 0
		touched[wireID(enc, key)] = true
	}
}

const rosterLogCap = 200

// logRosterLocked logs counts only, once per typed group thread per epoch and
// at most rosterLogCap lines per epoch: roster.t<type>.mc<count>.p<others>.c<0|1>,
// counts capped at 9.
func (s *messagixSession) logRosterLocked(t *threadInfo, complete bool) {
	if t.Enc || !t.Typed || t.Kind != "group" {
		return
	}
	if s.rosterLogEpoch != s.connEpoch {
		s.rosterLogEpoch, s.rosterLogN = s.connEpoch, 0
	}
	if s.rosterLogged == nil {
		s.rosterLogged = map[string]int64{}
	}
	id := wireID(false, t.Key)
	if s.rosterLogged[id] == s.connEpoch || s.rosterLogN >= rosterLogCap {
		return
	}
	s.rosterLogged[id] = s.connEpoch
	s.rosterLogN++
	c := 0
	if complete {
		c = 1
	}
	logCode("info", fmt.Sprintf("roster.t%d.mc%d.p%d.c%d", min(t.RawType, 99), min(t.MemberCount, 9), min(len(s.freshOthersLocked(t)), 9), c))
}

// bumpConnEpoch ends every roster snapshot taken so far.
func (s *messagixSession) bumpConnEpoch() {
	s.mu.Lock()
	s.connEpoch++
	s.mu.Unlock()
}

// setLinkDown records a socket going down or coming up. Either way every roster
// snapshot taken so far ends; while down, no roster is complete.
func (s *messagixSession) setLinkDown(down bool) {
	s.mu.Lock()
	s.connEpoch++
	s.linkDown = down
	s.mu.Unlock()
}

// freshOthersLocked is the thread's participants other than self whose latest
// participant row arrived in the current connection epoch, as wire ids. A
// participant last seen in an older epoch is left out: it may have been
// replaced while the stream was down.
func (s *messagixSession) freshOthersLocked(t *threadInfo) []string {
	var out []string
	for _, p := range t.Participants {
		if p != s.selfID && t.Seen[p] == s.connEpoch {
			out = append(out, strconv.FormatInt(p, 10))
		}
	}
	return out
}

// membersCompleteLocked reports whether the fresh others are known to be the
// whole roster: an open typed group, not continued by an encrypted record,
// whose snapshot count, taken in this connection epoch with no delta since,
// equals self plus the others seen in this epoch, while the socket is up.
// Identities from an older epoch never count.
func (s *messagixSession) membersCompleteLocked(t *threadInfo) bool {
	return !t.Enc && !s.linkDown && !s.supersededLocked(t) && t.Typed && t.Kind == "group" && !s.unacceptedLocked(t.Key) &&
		t.MemberCount > 0 && t.RosterEpoch != 0 && t.RosterEpoch == s.connEpoch &&
		t.MemberCount == int64(1+len(s.freshOthersLocked(t)))
}

// supersededLocked reports whether an open thread's record is continued by an
// encrypted one: a hybrid mapping or encrypted record under its key, or a
// cutover naming it as the predecessor. Its roster then lives elsewhere.
func (s *messagixSession) supersededLocked(t *threadInfo) bool {
	if _, enc := s.mapKey(t.Key); enc {
		return true
	}
	for _, open := range s.cutover {
		if open == t.Key {
			return true
		}
	}
	return false
}

func threadFrame(t Thread) Frame { return Frame{T: TThread, ThreadEvt: &t} }

// wireThreadLocked builds the protocol thread. A one-to-one thread names the
// other person; an encrypted thread names the open thread it continues.
func (s *messagixSession) wireThreadLocked(t *threadInfo) Thread {
	if !t.Enc && s.unacceptedLocked(t.Key) {
		// A message request qualifies for nothing: the connector imports any
		// one-to-one thread that names another person, typed or not, so the frame
		// carries no kind, no type, no participants and no title (usually the
		// stranger's name). The real ones stay in
		// memory and go out when a later row accepts the thread.
		return Thread{ID: wireID(false, t.Key), LastActivity: t.LastActivity}
	}
	out := Thread{ID: wireID(t.Enc, t.Key), Kind: t.Kind, Typed: t.Typed, Title: t.Title, Encrypted: t.Enc, LastActivity: t.LastActivity}
	if t.Enc {
		if open, ok := s.cutover[t.Key]; ok {
			out.Predecessor = wireID(false, open)
		}
	}
	out.MembersComplete = s.membersCompleteLocked(t)
	s.logRosterLocked(t, out.MembersComplete)
	self := strconv.FormatInt(s.selfID, 10)
	out.Participants = append(out.Participants, Person{ID: self, Name: s.selfName, Self: true})
	for _, p := range t.Participants {
		if p == s.selfID {
			continue
		}
		if out.MembersComplete && t.Seen[p] != s.connEpoch {
			// A complete roster lists exactly the fresh others; an identity
			// from an older epoch is not a member the count vouches for.
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
	return s.folderMark[key] || s.statusMark[key]
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
		s.connEpoch++
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
	s.mu.Lock()
	s.e2eeReceived++
	s.mu.Unlock()
	if _, ok := e2eeChatKey(evt.Info.Chat); !ok {
		// Visibility only: as before, the message is acknowledged. The class of
		// the id's server is logged, never the id.
		logCode("warn", "e2ee_unparsed_chat_"+e2eeServerClass(evt.Info.Chat.Server))
		s.mu.Lock()
		s.e2eeUnparsed++
		s.mu.Unlock()
		return true
	}
	frames := s.waMessageFrames(evt)
	if len(frames) == 0 {
		return true // a reaction that changes nothing here
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
	if len(seqs) > 0 {
		for _, seq := range seqs {
			if !s.acks.wait(seq) {
				return false
			}
		}
	}
	// A bare thread or reaction row commits nothing but was still emitted.
	s.mu.Lock()
	s.e2eeEmitted++
	s.mu.Unlock()
	return true
}

// e2eeServerClass names a JID server for the log: a fixed set, never anything
// the message chose.
func e2eeServerClass(server string) string {
	switch server {
	case waTypes.MessengerServer:
		return "msgr"
	case waTypes.HiddenUserServer:
		return "lid"
	case waTypes.HostedLIDServer:
		return "hosted_lid"
	case waTypes.GroupServer:
		return "g_us"
	case waTypes.DefaultUserServer, waTypes.LegacyUserServer:
		return "pn"
	}
	return "other"
}

// e2eeChatKey is the chat's numeric Messenger id, read from the id itself and
// nothing else (unchanged from the shipped parse): any other form is left
// unparsed, since guessing at the peer could route a reply to the wrong person.
func e2eeChatKey(chat waTypes.JID) (int64, bool) {
	id, err := strconv.ParseInt(chat.User, 10, 64)
	if err != nil || id == 0 {
		return 0, false
	}
	return id, true
}

func (s *messagixSession) waMessageFrames(evt *events.FBMessage) []Frame {
	chat := evt.Info.Chat
	chatID, ok := e2eeChatKey(chat)
	if !ok {
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
		"encrypted_unlisted":    unlisted,
		// Every encrypted message that reached deliverEncrypted, how many of
		// them were emitted, how many had a chat id that maps to no Messenger id.
		"e2ee_received":              s.e2eeReceived,
		"e2ee_emitted":               s.e2eeEmitted,
		"e2ee_dropped_unparsed_chat": s.e2eeUnparsed,
	}
}

// encryptedServer is the JID server an encrypted thread is addressed on. The
// thread's kind is read under the session lock: the receive path writes it.
func (s *messagixSession) encryptedServer(thread string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t := s.threads[thread]; t != nil && t.Kind == "group" {
		return waTypes.GroupServer
	}
	return waTypes.MessengerServer
}
