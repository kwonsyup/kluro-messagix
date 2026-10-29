// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupCommon"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	"go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// testStderr captures everything the sidecar writes to stderr while the tests
// run; TestMain validates every captured line before the binary exits.
var testStderr bytes.Buffer

func TestMain(m *testing.M) {
	stderr = &testStderr
	code := m.Run()
	if code == 0 && !stderrLinesAreLevelAndCode(&testStderr) {
		fmt.Fprintln(os.Stderr, "FAIL: stderr carried a line that is not {level, code} JSON")
		code = 1
	}
	os.Exit(code)
}

// stderrLinesAreLevelAndCode reports whether every line is one JSON object
// with exactly the keys level and code, level being info, warn or error.
func stderrLinesAreLevelAndCode(buf *bytes.Buffer) bool {
	sc := bufio.NewScanner(buf)
	for sc.Scan() {
		var obj struct {
			Level string `json:"level"`
			Code  string `json:"code"`
		}
		if json.Unmarshal(sc.Bytes(), &obj) != nil || obj.Level == "" || obj.Code == "" {
			return false
		}
		var keys map[string]any
		if json.Unmarshal(sc.Bytes(), &keys) != nil || len(keys) != 2 {
			return false
		}
		switch obj.Level {
		case "info", "warn", "error":
		default:
			return false
		}
	}
	return true
}

// fakeBackend scripts the loop's backend.
type fakeBackend struct {
	sentText      string
	sendErr       error
	histories     map[string]*Page
	stopped       bool
	loggedOut     bool
	stopState     bool
	acks          *ackHub
	emit          emitFunc
	startMode     string
	startPlatform string
	threadStats   map[string]int // returned by Threads
	threadGap     string
}

func (f *fakeBackend) Start(_ context.Context, emit emitFunc, acks *ackHub, cookies map[string]string, state []byte, mode, platform string) (Ready, initialEmit, error) {
	f.acks, f.emit = acks, emit
	f.startMode = mode
	f.startPlatform = platform
	th := Thread{ID: "e:42", Kind: "user", Encrypted: true, Predecessor: "42", Participants: []Person{{ID: "1", Self: true}, {ID: "42", Name: "P"}}}
	emit(Frame{T: TState, State: []byte("s1")})
	return Ready{Self: "1", Name: "Me", History: HistoryNeedsPIN}, func(emit emitFunc) { emit(threadFrame(th)) }, nil
}
func (f *fakeBackend) HistoryPIN(_ context.Context, pin []byte) (HistoryState, error) {
	if string(pin) == "000000" {
		return HistoryNone, &authErr{AuthPINWrong}
	}
	return HistoryEnrolled, nil
}
func (f *fakeBackend) Threads(_ context.Context, emit emitFunc) (listingDone, error) {
	if f.acks != nil && f.stopState {
		seq := emit(Frame{T: TMessage, Message: &Message{ID: "wa:thread-event"}, waitAck: true})
		if !f.acks.wait(seq) {
			return listingDone{}, errCode("ack_failed")
		}
	}
	emit(threadFrame(Thread{ID: "42", Kind: "user"}))
	return listingDone{Stats: f.threadStats, Gap: f.threadGap}, nil
}
func (f *fakeBackend) History(_ context.Context, thread, cursor string, count int) (*Page, error) {
	if p, ok := f.histories[thread+"|"+cursor]; ok {
		return p, nil
	}
	return nil, errCode("not_enrolled")
}
func (f *fakeBackend) Send(_ context.Context, thread, otid, text string) (*Sent, error) {
	f.sentText = text
	if f.sendErr != nil {
		return nil, f.sendErr
	}
	return &Sent{Thread: thread, MessageID: "m1", OTID: otid}, nil
}
func (f *fakeBackend) Logout(_ context.Context) error {
	f.loggedOut = true
	return nil
}
func (f *fakeBackend) Stop() {
	f.stopped = true
	if f.stopState && f.emit != nil {
		f.emit(Frame{T: TState, State: []byte("final")})
	}
}

func runLoop(t *testing.T, fb *fakeBackend, lines ...string) []Frame {
	t.Helper()
	var out bytes.Buffer
	in := strings.NewReader(strings.Join(lines, "\n") + "\n")
	code := run(in, &out, func() backend { return fb })
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	var frames []Frame
	sc := bufio.NewScanner(&out)
	sc.Buffer(make([]byte, 1<<20), maxLine)
	for sc.Scan() {
		var f Frame
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatalf("stdout carries a non-frame line: %q", sc.Text())
		}
		frames = append(frames, f)
	}
	return frames
}

func TestProtocolLoopStartHistorySendAndPIN(t *testing.T) {
	fb := &fakeBackend{histories: map[string]*Page{
		"e:42|": {Thread: "e:42", Next: "9", Continue: "42", Messages: []Message{{ID: "wa:1", Sender: "42", SentMS: 5}}},
	}}
	frames := runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"threads","id":2}`,
		`{"t":"history","id":3,"thread":"e:42","count":50}`,
		`{"t":"history","id":4,"thread":"e:99","count":50}`,
		`{"t":"history_pin","id":5,"pin":"000000"}`,
		`{"t":"history_pin","id":6,"pin":"123456"}`,
		`{"t":"send","id":7,"thread":"e:42","otid":"77","text":"hello"}`,
	)
	byID := map[int64]Frame{}
	for _, f := range frames {
		if f.ID != 0 {
			byID[f.ID] = f
		}
	}
	if byID[1].T != TReady || byID[1].Self != "1" || byID[1].History != HistoryNeedsPIN {
		t.Fatalf("ready = %+v", byID[1])
	}
	if byID[2].T != TThreadsDone {
		t.Fatalf("threads answer = %+v", byID[2])
	}
	if p := byID[3].Page; byID[3].T != TPage || p == nil || p.Continue != "42" || len(p.Messages) != 1 {
		t.Fatalf("page = %+v", byID[3])
	}
	if byID[4].T != TError || byID[4].Err != "not_enrolled" {
		t.Fatalf("unenrolled history = %+v", byID[4])
	}
	if byID[5].T != TAuth || byID[5].Auth != AuthPINWrong {
		t.Fatalf("wrong PIN = %+v", byID[5])
	}
	if byID[6].T != TReady || byID[6].History != HistoryEnrolled {
		t.Fatalf("PIN restore = %+v", byID[6])
	}
	if byID[7].T != TSent || byID[7].Sent.MessageID != "m1" || fb.sentText != "hello" {
		t.Fatalf("send = %+v", byID[7])
	}
	if !fb.stopped {
		t.Fatal("stop did not stop the backend")
	}
}

func TestSendFailureIsACodeNeverAProviderText(t *testing.T) {
	fb := &fakeBackend{sendErr: errCode("unknown")}
	frames := runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"send","id":2,"thread":"42","otid":"7","text":"hi"}`,
		`{"t":"stop"}`,
	)
	for _, f := range frames {
		if f.ID == 2 && (f.T != TError || f.Err != "unknown") {
			t.Fatalf("send failure = %+v", f)
		}
	}
}

func newTestSession() *messagixSession {
	s := newSession()
	s.selfID = 1
	s.selfName = "Me"
	acks := newAckHub()
	s.acks = acks
	s.emit = func(Frame) int64 { return 0 }
	return s
}

func TestTableMapsThreadsCutoverMessagesEditsAndUnsends(t *testing.T) {
	s := newTestSession()
	tbl := &table.LSTable{
		LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 42, ArmadilloThreadID: 4242}},
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			{ThreadKey: 42, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 100},
			{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, LastActivityTimestampMs: 200},
		},
		LSInsertMessage: []*table.LSInsertMessage{
			{ThreadKey: 42, MessageId: "mid.A", SenderId: 42, Text: "hello", TimestampMs: 150, OfflineThreadingId: "55"},
			{ThreadKey: 42, MessageId: "mid.B", SenderId: 1, Text: "mine", TimestampMs: 160},
		},
		LSEditMessage:   []*table.LSEditMessage{{MessageID: "mid.A", Text: "hello!"}},
		LSDeleteMessage: []*table.LSDeleteMessage{{ThreadKey: 42, MessageId: "mid.A"}},
	}
	frames := s.handleTableFrames(tbl)
	var threads = map[string]Thread{}
	var msgs []Message
	var edit *Edit
	var unsend *Unsend
	for _, f := range frames {
		switch f.T {
		case TThread:
			threads[f.ThreadEvt.ID] = *f.ThreadEvt
		case TMessage:
			msgs = append(msgs, *f.Message)
		case TEdit:
			edit = f.Edit
		case TUnsend:
			unsend = f.Unsend
		}
	}
	enc := threads["e:4242"]
	if !enc.Encrypted || enc.Predecessor != "42" || enc.Kind != "user" {
		t.Fatalf("encrypted thread = %+v", enc)
	}
	if th := threads["42"]; th.Encrypted || th.Kind != "user" {
		t.Fatalf("legacy thread = %+v", th)
	}
	if len(msgs) != 2 || msgs[0].Out || !msgs[1].Out || msgs[0].Thread != "42" || msgs[0].OTID != "55" {
		t.Fatalf("messages = %+v", msgs)
	}
	if edit == nil || edit.By != "" || edit.Thread != "42" || edit.Text != "hello!" {
		t.Fatalf("edit = %+v", edit)
	}
	if unsend == nil || unsend.By != "" || unsend.ID != "mid.A" {
		t.Fatalf("unsend = %+v", unsend)
	}
	// The lightspeed edit and unsend rows name no actor, so By stays empty:
	// the msgIndex's sender is the original author, not the actor, and the
	// connector records a claim instead of trusting the author.
	frames = s.handleTableFrames(&table.LSTable{LSDeleteMessage: []*table.LSDeleteMessage{{ThreadKey: 42, MessageId: "mid.Z"}}})
	if frames[0].Unsend.By != "" {
		t.Fatalf("unknown message unsend claimed an author: %+v", frames[0].Unsend)
	}
}

func TestHybridThreadMessagesLandInTheEncryptedChat(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSVerifyHybridThreadExists: []*table.LSVerifyHybridThreadExists{{ThreadKey: 77, ThreadJID: 7777}},
		LSInsertMessage:            []*table.LSInsertMessage{{ThreadKey: 77, MessageId: "mid.H", SenderId: 9, Text: "x", TimestampMs: 1}},
	})
	for _, f := range frames {
		if f.T == TMessage && f.Message.Thread != "e:7777" {
			t.Fatalf("hybrid message thread = %q", f.Message.Thread)
		}
	}
}

func consumerText(text string) *waConsumerApplication.ConsumerApplication {
	return &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: &waConsumerApplication.ConsumerApplication_Content{
			Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{MessageText: &waCommon.MessageText{Text: proto.String(text)}},
		}}}}
}

func TestEncryptedLiveMessageEditAndRevoke(t *testing.T) {
	s := newTestSession()
	info := waTypes.MessageInfo{MessageSource: waTypes.MessageSource{Chat: waTypes.NewJID("4242", waTypes.MessengerServer), Sender: waTypes.NewJID("4242", waTypes.MessengerServer)}, ID: "ABC", Timestamp: time.UnixMilli(5000)}
	frames := s.waMessageFrames(&events.FBMessage{Info: info, Message: consumerText("secret hello")})
	var msg *Message
	var thread *Thread
	for _, f := range frames {
		if f.T == TMessage {
			msg = f.Message
		}
		if f.T == TThread {
			thread = f.ThreadEvt
		}
	}
	if thread == nil || thread.ID != "e:4242" || thread.Kind != "user" {
		t.Fatalf("thread = %+v", thread)
	}
	if msg == nil || msg.ID != "wa:ABC" || msg.Text != "secret hello" || !msg.Encrypted || msg.Sender != "4242" || msg.Out {
		t.Fatalf("message = %+v", msg)
	}
	edit := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: &waConsumerApplication.ConsumerApplication_Content{
			Content: &waConsumerApplication.ConsumerApplication_Content_EditMessage{EditMessage: &waConsumerApplication.ConsumerApplication_EditMessage{
				Key: &waCommon.MessageKey{ID: proto.String("ABC")}, Message: &waCommon.MessageText{Text: proto.String("edited")}, TimestampMS: proto.Int64(6000),
			}},
		}}}}
	info.ID = "EDIT1"
	frames = s.waMessageFrames(&events.FBMessage{Info: info, Message: edit})
	var got *Edit
	for _, f := range frames {
		if f.T == TEdit {
			got = f.Edit
		}
	}
	if got == nil || got.ID != "wa:ABC" || got.Text != "edited" || got.By != "4242" {
		t.Fatalf("edit = %+v", got)
	}
}

func TestStateSurvivesRestartWithDeviceStoreAndLinks(t *testing.T) {
	ctx := context.Background()
	s := newTestSession()
	wdb, err := openWADB(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	var tempStore int
	if err := wdb.db.QueryRowContext(ctx, "PRAGMA temp_store").Scan(&tempStore); err != nil || tempStore != 2 {
		t.Fatalf("temp_store=%d err=%v, want MEMORY", tempStore, err)
	}
	rows, err := wdb.db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int
		var name, filename string
		if err := rows.Scan(&seq, &name, &filename); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if name == "main" && filename != "" {
			rows.Close()
			t.Fatalf("main SQLite store uses a file: %q", filename)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	s.waDB = wdb
	dev := wdb.container.NewDevice()
	dev.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{1}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	dev.ID = &waTypes.JID{User: "1", Device: 3, Server: waTypes.MessengerServer}
	if err := dev.Save(ctx); err != nil {
		t.Fatal(err)
	}
	if err := dev.Sessions.PutSession(ctx, "1:42.1", []byte("ratchet-state")); err != nil {
		t.Fatal(err)
	}
	s.cutover[4242] = 42
	s.fbToWA[77] = 7777
	blob := s.dump()
	if len(blob) == 0 {
		t.Fatal("empty state")
	}
	wdb.close()

	var st savedState
	if err := json.Unmarshal(blob, &st); err != nil {
		t.Fatal(err)
	}
	s2 := newTestSession()
	s2.restore(&st)
	if s2.cutover[4242] != 42 || s2.fbToWA[77] != 7777 {
		t.Fatalf("links lost: %v %v", s2.cutover, s2.fbToWA)
	}
	restored, err := openWADB(ctx, s2.waSnap, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restored.close)
	devices, err := restored.container.GetAllDevices(ctx)
	if err != nil || len(devices) != 1 || devices[0].ID.String() != dev.ID.String() {
		t.Fatalf("restored devices = %d err=%v", len(devices), err)
	}
	session, err := devices[0].Sessions.GetSession(ctx, "1:42.1")
	if err != nil || string(session) != "ratchet-state" {
		t.Fatalf("restored session = %q err=%v", session, err)
	}
}

func TestLogsAndFramesNeverCarrySecrets(t *testing.T) {
	fb := &fakeBackend{}
	var out bytes.Buffer
	// The malformed middle line forces a bad_frame warn onto stderr.
	in := io.NopCloser(strings.NewReader(`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"SECRETXS","datr":"d"}}` + "\n" +
		"not json\n" +
		`{"t":"history_pin","id":2,"pin":"123456"}` + "\n" + `{"t":"stop"}` + "\n"))
	run(in, &out, func() backend { return fb })
	if !strings.Contains(testStderr.String(), `"bad_frame"`) {
		t.Fatal("the malformed line logged no bad_frame")
	}
	for _, banned := range []string{"SECRETXS", "123456"} {
		if strings.Contains(testStderr.String(), banned) || strings.Contains(out.String(), banned) {
			t.Fatalf("%q leaked into logs or frames", banned)
		}
	}
}

func TestLogoutAnswersLoggedOutAndExits(t *testing.T) {
	fb := &fakeBackend{}
	frames := runLoop(t, fb,
		`{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`,
		`{"t":"logout","id":2}`,
	)
	if len(frames) == 0 || frames[len(frames)-1].T != TLoggedOut || frames[len(frames)-1].ID != 2 {
		t.Fatalf("logout answer = %+v", frames[len(frames)-1])
	}
	if !fb.loggedOut || !fb.stopped {
		t.Fatalf("logout loggedOut=%v stopped=%v", fb.loggedOut, fb.stopped)
	}
}

// TestEncryptedUnsendNamesTheActorNotTheAuthor: the encrypted-chat unsend
// carries its own sender, so By names the actor even when the original
// message was written by someone else.
func TestEncryptedUnsendNamesTheActorNotTheAuthor(t *testing.T) {
	s := newTestSession()
	chat := waTypes.NewJID("4242", waTypes.MessengerServer)
	info := waTypes.MessageInfo{MessageSource: waTypes.MessageSource{Chat: chat, Sender: waTypes.NewJID("4242", waTypes.MessengerServer)}, ID: "ABC", Timestamp: time.UnixMilli(5000)}
	var author string
	for _, f := range s.waMessageFrames(&events.FBMessage{Info: info, Message: consumerText("hello")}) {
		if f.T == TMessage {
			author = f.Message.Sender
		}
	}
	if author != "4242" {
		t.Fatalf("original author = %q", author)
	}
	revoke := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_ApplicationData{ApplicationData: &waConsumerApplication.ConsumerApplication_ApplicationData{
			ApplicationContent: &waConsumerApplication.ConsumerApplication_ApplicationData_Revoke{Revoke: &waConsumerApplication.ConsumerApplication_RevokeMessage{
				Key: &waCommon.MessageKey{ID: proto.String("ABC")},
			}},
		}},
	}}
	info.ID = "REVOKE1"
	info.Sender = waTypes.NewJID("1", waTypes.MessengerServer)
	var unsend *Unsend
	for _, f := range s.waMessageFrames(&events.FBMessage{Info: info, Message: revoke}) {
		if f.T == TUnsend {
			unsend = f.Unsend
		}
	}
	if unsend == nil || unsend.ID != "wa:ABC" || unsend.By != "1" {
		t.Fatalf("unsend = %+v", unsend)
	}
}

// archiveBackup builds one archive backup message carrying app, signed by
// sender, in the shape archiveMessage decodes.
func archiveBackup(t *testing.T, sender string, app *waConsumerApplication.ConsumerApplication) *waArmadilloBackupMessage.BackupMessage {
	t.Helper()
	sub := &waMsgApplication.MessageApplication_SubProtocolPayload_ConsumerMessage{}
	if err := sub.Set(app); err != nil {
		t.Fatal(err)
	}
	encoded, err := proto.Marshal(&waMsgApplication.MessageApplication{Payload: &waMsgApplication.MessageApplication_Payload{
		Content: &waMsgApplication.MessageApplication_Payload_SubProtocol{SubProtocol: &waMsgApplication.MessageApplication_SubProtocolPayload{SubProtocol: sub}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return &waArmadilloBackupMessage.BackupMessage{
		Metadata: &waArmadilloBackupCommon.Metadata{SenderID: proto.String(sender), MessageID: proto.String("M1")},
		Payload:  &waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage{EncryptedTransportMessage: encoded},
	}
}

func archiveEdit(t *testing.T, sender, text string) *securehistory.DecryptedProtobuf {
	t.Helper()
	app := &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
		Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: &waConsumerApplication.ConsumerApplication_Content{
			Content: &waConsumerApplication.ConsumerApplication_Content_EditMessage{EditMessage: &waConsumerApplication.ConsumerApplication_EditMessage{
				Key: &waCommon.MessageKey{ID: proto.String("M1")}, Message: &waCommon.MessageText{Text: proto.String(text)},
			}},
		}},
	}}
	return &securehistory.DecryptedProtobuf{Message: archiveBackup(t, sender, app), TimestampMS: 2}
}

// TestArchiveEditAppliesOnlyFromTheOriginalSender: a supplemental edit signed
// by anyone but the original sender leaves the text; the sender's own applies.
func TestArchiveEditAppliesOnlyFromTheOriginalSender(t *testing.T) {
	s := newTestSession()
	page := func(sup *securehistory.DecryptedProtobuf) Message {
		d := &securehistory.DecryptedMessage{
			ID:            "M1",
			Message:       &securehistory.DecryptedProtobuf{Message: archiveBackup(t, "4242", consumerText("original")), TimestampMS: 1},
			Supplementals: map[string]*securehistory.DecryptedProtobuf{"edit:1": sup},
		}
		m, ok := s.archiveMessage("e:4242", "4242", d)
		if !ok {
			t.Fatal("archive message dropped")
		}
		return m
	}
	if m := page(archiveEdit(t, "1", "tampered")); m.Text != "original" {
		t.Fatalf("foreign edit applied: %+v", m)
	}
	if m := page(archiveEdit(t, "4242", "edited")); m.Text != "edited" {
		t.Fatalf("author's edit not applied: %+v", m)
	}
}

// newSavedDevice creates a device with a complete row and saves it, the way
// RegisterE2EE leaves the store.
func newSavedDevice(t *testing.T, wdb *waDB, user string) *store.Device {
	t.Helper()
	dev := wdb.container.NewDevice()
	dev.Account = &waAdv.ADVSignedDeviceIdentity{Details: []byte{1}, AccountSignature: make([]byte, 64), AccountSignatureKey: make([]byte, 32), DeviceSignature: make([]byte, 64)}
	dev.ID = &waTypes.JID{User: user, Device: 3, Server: waTypes.MessengerServer}
	if err := dev.Save(t.Context()); err != nil {
		t.Fatal(err)
	}
	return dev
}

// emitRecorder wires a session like run does: frames are stamped with the
// hub's seq in emission order and recorded.
type emitRecorder struct {
	mu        sync.Mutex
	frames    []Frame
	hub       *ackHub
	ackOnEmit bool
}

func (r *emitRecorder) emit(f Frame) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := r.hub.stamp(&f)
	if r.ackOnEmit && seq > 0 {
		r.hub.ack(seq)
	}
	r.frames = append(r.frames, f)
	return seq
}

func TestAckArrivingBeforeWaitIsRetained(t *testing.T) {
	hub := newAckHub()
	rec := &emitRecorder{hub: hub, ackOnEmit: true}
	s := newTestSession()
	s.acks, s.emit = hub, rec.emit
	if !s.handleWAEvent(fbMessage("4242", "4242", "M1", "early-ack")) {
		t.Fatal("handler failed when ack arrived during frame emission")
	}
}

func TestMessageEditAndUnsendFramesAllReceiveIncreasingSequences(t *testing.T) {
	hub := newAckHub()
	w := newEmitter(io.Discard, hub)
	frames := []Frame{
		{T: TMessage, Message: &Message{ID: "m"}},
		{T: TEdit, Edit: &Edit{ID: "m"}},
		{T: TUnsend, Unsend: &Unsend{ID: "m"}},
	}
	for i, frame := range frames {
		if seq := w.emit(frame); seq != int64(i+1) {
			t.Fatalf("%s seq=%d, want %d", frame.T, seq, i+1)
		}
	}
}

func TestClosedAckHubCannotAcknowledgeEncryptedEvent(t *testing.T) {
	hub := newAckHub()
	hub.close()
	rec := &emitRecorder{hub: hub}
	s := newTestSession()
	s.acks, s.emit = hub, rec.emit
	if s.handleWAEvent(fbMessage("4242", "4242", "M1", "closed")) {
		t.Fatal("handler reported success after connector close")
	}
	frames := rec.all()
	for _, frame := range frames {
		if frame.T == TMessage && frame.Seq > 0 {
			return
		}
	}
	t.Fatalf("closed-connection event has no seq: %+v", frames)
}

func (r *emitRecorder) all() []Frame {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.frames)
}

func (r *emitRecorder) last() Frame {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.frames) == 0 {
		return Frame{}
	}
	return r.frames[len(r.frames)-1]
}

func (r *emitRecorder) reset() {
	r.mu.Lock()
	r.frames = nil
	r.mu.Unlock()
}

func fbMessage(chatUser, sender, id, text string) *events.FBMessage {
	return &events.FBMessage{
		Info: waTypes.MessageInfo{
			MessageSource: waTypes.MessageSource{
				Chat:   waTypes.NewJID(chatUser, waTypes.MessengerServer),
				Sender: waTypes.NewJID(sender, waTypes.MessengerServer),
			},
			ID: id, Timestamp: time.UnixMilli(5000),
		},
		Message: consumerText(text),
	}
}

// TestEncryptedMessageCarriesSeqAndWaitsForAck: the message frame carries a
// strictly increasing seq, and the handler does not report success to whatsmeow
// until the connector's ack for that seq arrives.
func TestEncryptedMessageCarriesSeqAndWaitsForAck(t *testing.T) {
	hub := newAckHub()
	rec := &emitRecorder{hub: hub}
	s := newTestSession()
	s.acks = hub
	s.emit = rec.emit

	done := make(chan bool, 1)
	go func() { done <- s.handleWAEvent(fbMessage("4242", "4242", "M1", "gated")) }()

	deadline := time.Now().Add(2 * time.Second)
	var seq int64
	for seq == 0 && time.Now().Before(deadline) {
		for _, f := range rec.all() {
			if f.T == TMessage {
				if f.Seq == 0 {
					t.Fatalf("message frame carries no seq: %+v", f)
				}
				seq = f.Seq
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if seq == 0 {
		t.Fatal("the message frame never arrived")
	}
	select {
	case r := <-done:
		t.Fatalf("handler returned %v before the ack", r)
	default:
	}
	hub.ack(seq)
	select {
	case r := <-done:
		if !r {
			t.Fatal("acked event reported failure")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not return after the ack")
	}
	// A second event gets a strictly greater seq.
	go func() { done <- s.handleWAEvent(fbMessage("4242", "4242", "M2", "second")) }()
	deadline = time.Now().Add(2 * time.Second)
	var seq2 int64
	for seq2 == 0 && time.Now().Before(deadline) {
		for _, f := range rec.all() {
			if f.T == TMessage && f.Message != nil && f.Message.ID == "wa:M2" {
				seq2 = f.Seq
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if seq2 <= seq {
		t.Fatalf("second seq %d not greater than %d", seq2, seq)
	}
	hub.ack(seq2)
	<-done
}

// TestAckTimeoutHandsTheEventBack: without an ack the handler reports failure
// so whatsmeow keeps the event for redelivery, and a closed hub does the same.
func TestAckTimeoutHandsTheEventBack(t *testing.T) {
	old := ackWait
	ackWait = 30 * time.Millisecond
	t.Cleanup(func() { ackWait = old })

	hub := newAckHub()
	rec := &emitRecorder{hub: hub}
	s := newTestSession()
	s.acks = hub
	s.emit = rec.emit

	start := time.Now()
	if s.handleWAEvent(fbMessage("4242", "4242", "M1", "unacked")) {
		t.Fatal("unacked event reported success")
	}
	if elapsed := time.Since(start); elapsed < 20*time.Millisecond {
		t.Fatalf("handler returned before the ack wait: %s", elapsed)
	}
	if !strings.Contains(testStderr.String(), `"ack_timeout"`) {
		t.Fatal("the ack timeout was not logged")
	}

	closed := newAckHub()
	rec2 := &emitRecorder{hub: closed}
	s2 := newTestSession()
	s2.acks = closed
	s2.emit = rec2.emit
	seq := closed.stamp(&Frame{T: TMessage, waitAck: true})
	closed.close()
	if closed.wait(seq) {
		t.Fatal("wait on a closed hub reported success")
	}
}

// TestDeviceStoreMutationSealsStateBeforeTheMessage: a whatsmeow store write
// (the Signal session change an ordinary encrypted message makes) marks the
// state dirty, and the state frame is emitted before the message frame.
func TestDeviceStoreMutationSealsStateBeforeTheMessage(t *testing.T) {
	ctx := context.Background()
	s := newTestSession()
	wdb, err := openWADB(ctx, nil, s.markStateDirty)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	s.waDB = wdb
	dev := newSavedDevice(t, wdb, "1")

	hub := newAckHub()
	rec := &emitRecorder{hub: hub}
	s.acks = hub
	s.emit = rec.emit
	if !s.flushState(true) {
		t.Fatal("failed to emit the initial state")
	}
	rec.reset()
	if err := dev.EventBuffer.DoDecryptionTxn(ctx, func(ctx context.Context) error {
		return dev.Sessions.PutSession(ctx, "1:4242.1", []byte("ratchet-after-message"))
	}); err != nil {
		t.Fatal(err)
	}

	done := make(chan bool, 1)
	go func() { done <- s.handleWAEvent(fbMessage("4242", "4242", "M1", "sealed")) }()
	var seq int64
	deadline := time.Now().Add(2 * time.Second)
	for seq == 0 && time.Now().Before(deadline) {
		for _, f := range rec.all() {
			if f.T == TMessage {
				seq = f.Seq
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	if seq == 0 {
		t.Fatal("the message frame never arrived")
	}
	hub.ack(seq)
	if r := <-done; !r {
		t.Fatal("handler failed")
	}
	stateAt, messageAt := -1, -1
	for i, f := range rec.all() {
		switch {
		case f.T == TState && stateAt == -1:
			stateAt = i
			var st savedState
			if err := json.Unmarshal(f.State, &st); err != nil {
				t.Fatalf("state frame is not a saved state: %v", err)
			}
			if len(st.WADB) == 0 {
				t.Fatal("state frame sealed no device store")
			}
			restored, err := openWADB(ctx, st.WADB, nil)
			if err != nil {
				t.Fatalf("cannot restore emitted state: %v", err)
			}
			defer restored.close()
			devices, err := restored.container.GetAllDevices(ctx)
			if err != nil || len(devices) != 1 {
				t.Fatalf("emitted-state devices=%d err=%v", len(devices), err)
			}
			session, err := devices[0].Sessions.GetSession(ctx, "1:4242.1")
			if err != nil || string(session) != "ratchet-after-message" {
				t.Fatalf("emitted ratchet=%q err=%v", session, err)
			}
		case f.T == TMessage:
			messageAt = i
		}
	}
	if stateAt == -1 || messageAt == -1 || stateAt > messageAt {
		t.Fatalf("state frame (%d) does not precede the message frame (%d)", stateAt, messageAt)
	}
}

// TestStoreCreatesNoFiles: the memory device store and its snapshot leave
// nothing behind in the process's temporary directory.
func TestStoreCreatesNoFiles(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)
	ctx := context.Background()
	wdb, err := openWADB(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	var tempStore int
	if err := wdb.db.QueryRowContext(ctx, "PRAGMA temp_store").Scan(&tempStore); err != nil || tempStore != 2 {
		t.Fatalf("temp_store=%d err=%v, want MEMORY", tempStore, err)
	}
	rows, err := wdb.db.QueryContext(ctx, "PRAGMA database_list")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int
		var name, filename string
		if err := rows.Scan(&seq, &name, &filename); err != nil {
			rows.Close()
			t.Fatal(err)
		}
		if name == "main" && filename != "" {
			rows.Close()
			t.Fatalf("main SQLite store uses a file: %q", filename)
		}
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		t.Fatal(err)
	}
	rows.Close()
	dev := newSavedDevice(t, wdb, "1")
	if err := dev.Sessions.PutSession(ctx, "1:42.1", []byte("s")); err != nil {
		t.Fatal(err)
	}
	snap, err := wdb.snapshot(ctx)
	if err != nil || len(snap) == 0 {
		t.Fatalf("snapshot = %d err=%v", len(snap), err)
	}
	restored, err := openWADB(ctx, snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	restored.close()
	wdb.close()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		names := make([]string, len(entries))
		for i, e := range entries {
			names[i] = e.Name()
		}
		t.Fatalf("the store wrote files: %v", names)
	}
}

func TestReopenedMemoryStoreConnectionKeepsTempMemoryAndRows(t *testing.T) {
	ctx := context.Background()
	wdb, err := openWADB(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	dev := newSavedDevice(t, wdb, "1")
	if err := dev.Sessions.PutSession(ctx, "1:42.1", []byte("still-here")); err != nil {
		t.Fatal(err)
	}

	// Discard the pooled connection, then require the driver's open hook to
	// apply temp_store again on the replacement connection.
	wdb.db.SetMaxIdleConns(0)
	var tempStore int
	if err := wdb.db.QueryRowContext(ctx, "PRAGMA temp_store").Scan(&tempStore); err != nil || tempStore != 2 {
		t.Fatalf("reopened temp_store=%d err=%v, want MEMORY", tempStore, err)
	}
	wdb.db.SetMaxIdleConns(1)
	if err := wdb.db.QueryRowContext(ctx, "PRAGMA temp_store").Scan(&tempStore); err != nil || tempStore != 2 {
		t.Fatalf("pooled temp_store=%d err=%v, want MEMORY", tempStore, err)
	}

	devices, err := wdb.container.GetAllDevices(ctx)
	if err != nil || len(devices) != 1 {
		t.Fatalf("devices=%d err=%v after reconnect", len(devices), err)
	}
	session, err := devices[0].Sessions.GetSession(ctx, "1:42.1")
	if err != nil || string(session) != "still-here" {
		t.Fatalf("session=%q err=%v after reconnect", session, err)
	}
}

// TestStopExitsZeroWithoutStart: a stop command is answered by a clean exit.
func TestStopExitsZeroWithoutStart(t *testing.T) {
	fb := &fakeBackend{stopState: true}
	frames := runLoop(t, fb, `{"t":"start","id":1}`, `{"t":"stop"}`)
	if !fb.stopped {
		t.Fatal("stop did not stop the backend")
	}
	if frames[len(frames)-1].T != TState || string(frames[len(frames)-1].State) != "final" {
		t.Fatalf("stop final frame = %+v", frames[len(frames)-1])
	}
}

func TestRunRoutesAckDuringBlockingBackendCommand(t *testing.T) {
	fb := &fakeBackend{stopState: true}
	ackR, ackW := io.Pipe()
	defer ackR.Close()
	defer ackW.Close()
	in := io.MultiReader(strings.NewReader("{\"t\":\"start\",\"id\":1}\n{\"t\":\"threads\",\"id\":2}\n"), ackR)
	var out bytes.Buffer
	w := &ackAfterMessageWriter{out: &out, ackW: ackW}
	code := run(in, w, func() backend { return fb })
	if code != 0 {
		t.Fatalf("exit code %d", code)
	}
	var frames []Frame
	sc := bufio.NewScanner(&out)
	for sc.Scan() {
		var f Frame
		if err := json.Unmarshal(sc.Bytes(), &f); err != nil {
			t.Fatal(err)
		}
		frames = append(frames, f)
	}
	var done bool
	for _, f := range frames {
		if f.ID == 2 && f.T == TThreadsDone {
			done = true
		}
	}
	if !done {
		t.Fatalf("ack did not unblock the backend command: %+v", frames)
	}
}

type ackAfterMessageWriter struct {
	out  io.Writer
	ackW *io.PipeWriter
	once sync.Once
}

func (w *ackAfterMessageWriter) Write(p []byte) (int, error) {
	n, err := w.out.Write(p)
	if err != nil {
		return n, err
	}
	var f Frame
	if json.Unmarshal(bytes.TrimSpace(p), &f) == nil && f.T == TMessage {
		w.once.Do(func() {
			_, _ = fmt.Fprintf(w.ackW, `{"t":"ack","seq":%d}`+"\n", f.Seq)
		})
	}
	if f.T == TThreadsDone {
		_, _ = io.WriteString(w.ackW, "{\"t\":\"stop\"}\n")
		_ = w.ackW.Close()
	}
	return n, nil
}

// TestSessionStopFlushesFinalStateAndWipesStore: stop emits the final state
// while the store is still open, then closes the memory store for good.
func TestSessionStopFlushesFinalStateAndWipesStore(t *testing.T) {
	ctx := context.Background()
	s := newTestSession()
	wdb, err := openWADB(ctx, nil, s.markStateDirty)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	s.waDB = wdb
	newSavedDevice(t, wdb, "1")
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.stateLoopDone = make(chan struct{})
	go s.stateLoop()
	hub := newAckHub()
	rec := &emitRecorder{hub: hub}
	s.acks = hub
	s.emit = rec.emit

	s.Stop()
	select {
	case <-s.stateLoopDone:
	default:
		t.Fatal("state loop survived stop")
	}

	last := rec.last()
	if last.T != TState {
		t.Fatalf("final frame = %+v, want state", last)
	}
	var st savedState
	if err := json.Unmarshal(last.State, &st); err != nil || len(st.WADB) == 0 {
		t.Fatalf("final state sealed no device store: %v", err)
	}
	if err := wdb.db.Ping(); err == nil {
		t.Fatal("the memory store survived stop")
	}
}

// TestSnapshotRoundTripKeepsTextAndBlobColumns: restored rows keep their
// SQLite storage classes, so the library's lookups still match them.
func TestSnapshotRoundTripKeepsTextAndBlobColumns(t *testing.T) {
	ctx := context.Background()
	wdb, err := openWADB(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	dev := newSavedDevice(t, wdb, "1")
	jid := dev.ID.String()
	if _, err := wdb.db.ExecContext(ctx, "INSERT INTO whatsmeow_sessions (our_jid, their_id, session) VALUES (?, '1:42.1', x'001122')", jid); err != nil {
		t.Fatal(err)
	}
	snap, err := wdb.snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	restored, err := openWADB(ctx, snap, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.close()
	var session []byte
	if err := restored.db.QueryRowContext(ctx, "SELECT session FROM whatsmeow_sessions WHERE our_jid = ? AND their_id = ?", jid, "1:42.1").Scan(&session); err != nil {
		t.Fatalf("text-keyed lookup failed after restore: %v", err)
	}
	if !bytes.Equal(session, []byte{0x00, 0x11, 0x22}) {
		t.Fatalf("blob column changed: %x", session)
	}
	var their string
	if err := restored.db.QueryRowContext(ctx, "SELECT their_id FROM whatsmeow_sessions WHERE session = ?", []byte{0x00, 0x11, 0x22}).Scan(&their); err != nil {
		t.Fatalf("blob-keyed lookup failed after restore: %v", err)
	}
	if their != "1:42.1" {
		t.Fatalf("text column changed: %q", their)
	}
	large := bytes.Repeat([]byte{0x5a}, 2<<20)
	devices, err := restored.container.GetAllDevices(ctx)
	if err != nil || len(devices) != 1 {
		t.Fatalf("restored devices=%d err=%v", len(devices), err)
	}
	if err := devices[0].Sessions.PutSession(ctx, "1:42.1", large); err != nil {
		t.Fatalf("restored store could not grow: %v", err)
	}
}
