// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestEncryptedGroupFactsAndSenderShareOneThread(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:        []*table.LSDeleteThenInsertThread{{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_GROUP}},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 4242, ContactId: 42}},
		LSSyncUpdateThreadName:          []*table.LSSyncUpdateThreadName{{ThreadKey: 4242, ThreadName: "Group"}},
	})
	check := func(frames []Frame, title string, participants ...string) {
		t.Helper()
		if len(frames) == 0 {
			t.Fatal("no thread facts emitted")
		}
		for _, f := range frames {
			if f.T != TThread {
				continue
			}
			th := f.ThreadEvt
			if th.ID != "e:4242" || th.Kind != "group" || !th.Encrypted || th.Title != title {
				t.Fatalf("thread = %+v", th)
			}
			for _, id := range participants {
				if !slices.ContainsFunc(th.Participants, func(p Person) bool { return p.ID == id }) {
					t.Fatalf("missing participant %s: %+v", id, th)
				}
			}
		}
		if s.threads["4242"] != nil || len(s.threads) != 1 {
			t.Fatal("a phantom unencrypted thread was created")
		}
	}
	check(frames, "Group", "1", "42")
	// Later participant and name rows still address the existing encrypted row.
	check(s.handleTableFrames(&table.LSTable{
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 4242, ContactId: 43}},
		LSSyncUpdateThreadName:          []*table.LSSyncUpdateThreadName{{ThreadKey: 4242, ThreadName: "Renamed"}},
	}), "Renamed", "42", "43")
	msg := fbMessage("4242", "44", "group-message", "hello")
	msg.Info.Chat.Server = waTypes.GroupServer
	check(s.waMessageFrames(msg), "Renamed", "42", "43", "44")
	check(s.sortedThreadFrames(), "Renamed", "42", "43", "44")
	// Self sends must not add a duplicate self participant to the group record.
	msg.Info.Sender.User = "1"
	s.waMessageFrames(msg)
	if slices.Contains(s.threads["e:4242"].Participants, int64(1)) {
		t.Fatal("self was added as a peer")
	}
}

func encryptedMutation(kind string) *events.FBMessage {
	evt := fbMessage("4242", "42", "mutation", "")
	key := &waCommon.MessageKey{ID: proto.String("missing-target")}
	if kind == TEdit {
		evt.Message = &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{Content: &waConsumerApplication.ConsumerApplication_Content{
				Content: &waConsumerApplication.ConsumerApplication_Content_EditMessage{EditMessage: &waConsumerApplication.ConsumerApplication_EditMessage{
					Key: key, Message: &waCommon.MessageText{Text: proto.String("changed")},
				}},
			}},
		}}
	} else {
		evt.Message = &waConsumerApplication.ConsumerApplication{Payload: &waConsumerApplication.ConsumerApplication_Payload{
			Payload: &waConsumerApplication.ConsumerApplication_Payload_ApplicationData{ApplicationData: &waConsumerApplication.ConsumerApplication_ApplicationData{
				ApplicationContent: &waConsumerApplication.ConsumerApplication_ApplicationData_Revoke{Revoke: &waConsumerApplication.ConsumerApplication_RevokeMessage{Key: key}},
			}},
		}}
	}
	return evt
}

func TestEncryptedEditsAndUnsendsRetryUntilDurableAck(t *testing.T) {
	old := ackWait
	ackWait = 25 * time.Millisecond
	t.Cleanup(func() { ackWait = old })
	for _, kind := range []string{TEdit, TUnsend} {
		t.Run(kind, func(t *testing.T) {
			s := newTestSession()
			hub := newAckHub()
			rec := &emitRecorder{hub: hub}
			s.acks, s.emit = hub, rec.emit
			evt := encryptedMutation(kind)
			if s.handleWAEvent(evt) {
				t.Fatal("unacked mutation was acknowledged to Messenger")
			}
			first := rec.last()
			if first.T != kind || first.Seq <= 0 {
				t.Fatalf("mutation = %+v", first)
			}
			// A durable no-op (target absent) is still acked by the connector.
			rec.ackOnEmit = true
			if !s.handleWAEvent(evt) {
				t.Fatal("redelivered mutation did not succeed after commit ack")
			}
			if next := rec.last(); next.T != kind || next.Seq <= first.Seq {
				t.Fatalf("redelivery = %+v", next)
			}
			hub.mu.Lock()
			defer hub.mu.Unlock()
			if len(hub.waiting) != 0 || len(hub.acked) != 0 {
				t.Fatal("mutation ack waiters leaked")
			}
		})
	}
}

func TestLogoutOnlyLoadsWebStateWithoutRegisteringOrConnecting(t *testing.T) {
	s := newSession()
	hub := newAckHub()
	rec := &emitRecorder{hub: hub}
	// A valid cached web account avoids any network dependency. An invalid
	// device snapshot would fail if logout_only tried to open an E2EE store.
	state, err := json.Marshal(savedState{
		Messagix: json.RawMessage(`{"Configs":{"BrowserConfigTable":{"CurrentUserInitialData":{"ACCOUNT_ID":"1","USER_ID":"1","NAME":"Me"}},"Bitmap":{},"CSRBitmap":{}},"SyncStore":{}}`),
		WADB:     []byte("not a device database"), WADevice: "1:3@msgr",
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, initial, err := s.Start(t.Context(), rec.emit, hub, map[string]string{"c_user": "1", "xs": "test-cookie", "datr": "test-device"}, state, "logout_only", "")
	t.Cleanup(s.Stop)
	if err != nil || ready.Self != "1" || initial != nil {
		t.Fatalf("start: ready=%+v initial=%v err=%v", ready, initial != nil, err)
	}
	if s.client == nil || s.client.GetHTTP() == nil || s.client.IsConnected() {
		t.Fatal("logout_only needs only a web HTTP client")
	}
	// Even a spurious connection callback cannot admit registration work.
	s.handleMetaEvent(t.Context(), &messagix.ConnectedEvent{})
	s.scheduleE2EE()
	if err := s.connectE2EE(); codeOf(err) != "not_started" {
		t.Fatalf("E2EE admission = %v", err)
	}
	if s.e2eeStarting || s.e2eeTry || s.waDB != nil || s.waDevice != nil || s.histBak != nil {
		t.Fatal("logout_only admitted encrypted-device or history work")
	}
	if err := s.Logout(t.Context()); err != nil {
		t.Fatal(err)
	}
	last := rec.last()
	if last.T != TState || string(last.State) != "{}" || last.Seq <= 0 {
		t.Fatalf("logout did not seal empty state: %+v", last)
	}
	n := len(rec.all())
	s.Stop()
	if len(rec.all()) != n {
		t.Fatal("Stop after Logout emitted stale state")
	}
}

func TestLogoutOnlyRejectsEveryOtherCommand(t *testing.T) {
	fb := &fakeBackend{}
	frames := runLoop(t, fb,
		`{"t":"start","id":1,"mode":"logout_only"}`,
		`{"t":"threads","id":2}`, `{"t":"history","id":3}`,
		`{"t":"history_pin","id":4,"pin":"123456"}`, `{"t":"send","id":5,"text":"no"}`,
		`{"t":"start","id":6}`, `{"t":"logout","id":7}`,
	)
	if fb.startMode != "logout_only" || !fb.loggedOut || fb.sentText != "" {
		t.Fatal("logout_only was not routed to the backend")
	}
	for id := int64(2); id <= 6; id++ {
		if !slices.ContainsFunc(frames, func(f Frame) bool { return f.ID == id && f.T == TError && f.Err == "bad_request" }) {
			t.Fatalf("command %d was accepted", id)
		}
	}
}

func TestLogoutWaitsForExistingWorkAndClosesAdmission(t *testing.T) {
	s := newTestSession()
	s.ctx, s.cancel = context.WithCancel(t.Context())
	s.e2eeConnectMu.Lock()
	s.scheduleE2EE() // admitted, but not yet running connectE2EE
	done := make(chan error, 1)
	go func() { done <- s.Logout(t.Context()) }()
	deadline := time.Now().Add(time.Second)
	for {
		s.mu.Lock()
		stopping := s.stopping
		s.mu.Unlock()
		if stopping {
			break
		}
		if time.Now().After(deadline) {
			s.e2eeConnectMu.Unlock()
			t.Fatal("Logout never closed admission")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-done:
		s.e2eeConnectMu.Unlock()
		t.Fatal("Logout did not wait for its existing owner")
	default:
	}
	s.scheduleE2EE() // must not add another owner
	s.e2eeConnectMu.Unlock()
	select {
	case err := <-done:
		if err != nil || s.e2eeTry || s.waDB != nil {
			t.Fatalf("Logout started registration work: err=%v tried=%v", err, s.e2eeTry)
		}
	case <-time.After(time.Second):
		t.Fatal("Logout waited for work beyond its existing owner")
	}
}

type cancellableListingBackend struct{ fakeBackend }

func (b *cancellableListingBackend) Threads(ctx context.Context, emit emitFunc) (listingDone, error) {
	emit(threadFrame(Thread{ID: "first-batch", Kind: "group"}))
	<-ctx.Done()
	return listingDone{}, ctx.Err()
}

func TestStopAndLogoutCancelListingAfterFirstBatch(t *testing.T) {
	for _, terminal := range []string{TStop, TLogout} {
		t.Run(terminal, func(t *testing.T) {
			inR, inW := io.Pipe()
			outR, outW := io.Pipe()
			t.Cleanup(func() { inR.Close(); inW.Close(); outR.Close(); outW.Close() })
			fb := &cancellableListingBackend{}
			done := make(chan int, 1)
			go func() {
				done <- run(inR, outW, func() backend { return fb })
				outW.Close()
			}()
			events := make(chan Frame, 10)
			conn := NewConn(outR, inW, func(f Frame) { events <- f })
			ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
			defer cancel()
			if _, err := conn.Call(ctx, Frame{T: TStart}); err != nil {
				t.Fatal(err)
			}
			listing := make(chan error, 1)
			go func() { _, err := conn.Call(ctx, Frame{T: TThreads}); listing <- err }()
			for {
				select {
				case f := <-events:
					if f.T == TThread && f.ThreadEvt.ID == "first-batch" {
						goto batchReceived
					}
				case <-ctx.Done():
					t.Fatal("first batch was hidden behind threads_done")
				}
			}
		batchReceived:
			if terminal == TLogout {
				if ans, err := conn.Call(ctx, Frame{T: TLogout}); err != nil || ans.T != TLoggedOut {
					t.Fatalf("logout: %+v %v", ans, err)
				}
			} else if err := conn.Send(Frame{T: TStop}); err != nil {
				t.Fatal(err)
			}
			select {
			case code := <-done:
				if code != 0 || !fb.stopped || (terminal == TLogout && !fb.loggedOut) {
					t.Fatalf("termination failed: code=%d", code)
				}
			case <-ctx.Done():
				t.Fatal("termination stuck behind listing")
			}
			if err := <-listing; err == nil {
				t.Fatal("cancelled listing reported threads_done")
			}
		})
	}
}

func TestWaitingDeliveriesShareStateBeforeGatedEvents(t *testing.T) {
	s := newTestSession()
	s.ctx = t.Context()
	wdb, err := openWADB(t.Context(), nil, s.markStateDirty)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	s.waDB = wdb
	dev := newSavedDevice(t, wdb, "1")
	rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
	s.emit = rec.emit
	var wg sync.WaitGroup
	for i := range 8 {
		id := fmt.Sprintf("burst-%d", i)
		// The real decryption transaction also buffers plaintext. Every write
		// dirties the store; coalescing must cover the newest one, not skip it.
		if err := dev.EventBuffer.DoDecryptionTxn(t.Context(), func(ctx context.Context) error {
			if err := dev.Sessions.PutSession(ctx, id, []byte(id)); err != nil {
				return err
			}
			return dev.EventBuffer.PutBufferedEvent(ctx, sha256.Sum256([]byte(id)), []byte(id), time.Now())
		}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 8 {
		id := fmt.Sprintf("burst-%d", i)
		wg.Go(func() {
			if !s.handleWAEvent(fbMessage("4242", "42", id, id)) {
				t.Errorf("delivery %s failed", id)
			}
		})
	}
	wg.Wait()
	frames := rec.all()
	states, messages, stateAt := 0, 0, -1
	var state savedState
	var previous int64
	for i, f := range frames {
		if f.Seq > 0 {
			if f.Seq <= previous {
				t.Fatal("state and event sequences are not increasing")
			}
			previous = f.Seq
		}
		if f.T == TState {
			states++
			stateAt = i
			if err := json.Unmarshal(f.State, &state); err != nil {
				t.Fatal(err)
			}
		}
		if f.T == TMessage {
			messages++
			if stateAt < 0 {
				t.Fatal("event preceded its state")
			}
		}
	}
	if states != 1 || messages != 8 {
		t.Fatalf("burst emitted %d states and %d messages", states, messages)
	}
	restored, err := openWADB(t.Context(), state.WADB, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restored.close)
	devices, err := restored.container.GetAllDevices(t.Context())
	if err != nil || len(devices) != 1 {
		t.Fatalf("restored devices=%d err=%v", len(devices), err)
	}
	for i := range 8 {
		id := fmt.Sprintf("burst-%d", i)
		ratchet, err := devices[0].Sessions.GetSession(t.Context(), id)
		if err != nil || string(ratchet) != id {
			t.Fatalf("snapshot omitted %s's ratchet: %v", id, err)
		}
		buffer, err := devices[0].EventBuffer.GetBufferedEvent(t.Context(), sha256.Sum256([]byte(id)))
		if err != nil || buffer == nil || string(buffer.Plaintext) != id {
			t.Fatalf("snapshot omitted %s's buffered event: %v", id, err)
		}
	}
	// A newer mutation immediately after the burst requires another snapshot.
	// Reusing a recent state frame would lose this ratchet on restart.
	rec.reset()
	if err := dev.Sessions.PutSession(t.Context(), "newer", []byte("new-ratchet")); err != nil {
		t.Fatal(err)
	}
	if !s.handleWAEvent(fbMessage("4242", "42", "newer", "newer")) || rec.all()[0].T != TState {
		t.Fatal("a recent snapshot incorrectly covered a newer ratchet")
	}
}

func TestSnapshotVacuumRemovesDeletedPlaintext(t *testing.T) {
	wdb, err := openWADB(t.Context(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	// Deliberately recreate a pre-fix store with remnants on freelist pages.
	for _, query := range []string{"PRAGMA secure_delete=OFF", "CREATE TABLE remnants (content BLOB)"} {
		if _, err := wdb.db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	secret := bytes.Repeat([]byte("deleted-buffer-plaintext-"), 10000)
	if _, err := wdb.db.ExecContext(t.Context(), "INSERT INTO remnants VALUES (?)", secret); err != nil {
		t.Fatal(err)
	}
	if _, err := wdb.db.ExecContext(t.Context(), "DELETE FROM remnants"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := wdb.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := gunzip(snapshot)
	if err != nil || bytes.Contains(raw, []byte("deleted-buffer-plaintext-")) {
		t.Fatal("serialized state retained deleted plaintext")
	}
	for pragma, want := range map[string]int{"secure_delete": 1, "freelist_count": 0} {
		var got int
		if err := wdb.db.QueryRowContext(t.Context(), "PRAGMA "+pragma).Scan(&got); err != nil || got != want {
			t.Fatalf("%s=%d want=%d err=%v", pragma, got, want, err)
		}
	}
}

func TestHistoryEnrollmentRequiresStateAck(t *testing.T) {
	old := ackWait
	ackWait = 30 * time.Millisecond
	t.Cleanup(func() { ackWait = old })
	for _, outcome := range []string{"ack", "timeout", "write_failed", "cancelled"} {
		t.Run(outcome, func(t *testing.T) {
			s := newTestSession()
			previous := &securehistory.Device{StoragePrivateKey: []byte("previous-keys")}
			s.histDev = previous
			s.histBak = &securehistory.Backup{ID: "backup"}
			rec := &emitRecorder{hub: s.acks}
			emitted := make(chan Frame, 1)
			s.emit = func(f Frame) int64 {
				if outcome == "write_failed" {
					return -1
				}
				seq := rec.emit(f)
				emitted <- rec.last()
				return seq
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			added := make(chan struct{}, 1)
			done := make(chan error, 1)
			dev := &securehistory.Device{StoragePrivateKey: []byte("recoverable-key")}
			go func() {
				_, err := s.persistHistoryEnrollment(ctx, dev, func() (*securehistory.Device, error) {
					added <- struct{}{}
					return &securehistory.Device{ID: "confirmed"}, nil
				})
				done <- err
			}()
			if outcome != "write_failed" {
				f := <-emitted
				if f.T != TState || f.Seq <= 0 || !bytes.Contains(f.State, []byte("history_device")) {
					t.Fatalf("enrollment state = %+v", f)
				}
				select {
				case <-added:
					t.Fatal("AddDevice ran before state ack")
				default:
				}
				if outcome == "ack" {
					s.acks.ack(f.Seq)
				} else if outcome == "cancelled" {
					cancel()
				}
			}
			select {
			case err := <-done:
				if outcome == "ack" {
					if err != nil || len(added) != 1 {
						t.Fatalf("acked enrollment: %v", err)
					}
				} else if err == nil || len(added) != 0 {
					t.Fatal("enrollment confirmed without durable state")
				}
				if outcome != "ack" && (s.histDev != previous || !s.stateDirty || s.checkHistory(t.Context()) != HistoryNeedsPIN) {
					t.Fatal("failed state seal retained an unconfirmed enrollment")
				}
			case <-time.After(time.Second):
				t.Fatal("state ack wait did not finish")
			}
		})
	}
}

func TestOversizedStateEmitsHealthAndWithholdsEncryptedEvent(t *testing.T) {
	s := newTestSession()
	s.histDev = &securehistory.Device{StoragePrivateKey: bytes.Repeat([]byte{1}, maxLine)}
	s.stateDirty = true
	rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
	s.emit = rec.emit
	wantBytes := len(s.dump())
	if s.handleWAEvent(fbMessage("4242", "42", "oversized", "withheld")) {
		t.Fatal("event succeeded without a state snapshot")
	}
	frames := rec.all()
	if len(frames) != 1 || frames[0].T != THealth || frames[0].Code != "state_too_large" || frames[0].Bytes != wantBytes || frames[0].Seq != 0 {
		t.Fatalf("oversized state frames = %+v", frames)
	}
	if !strings.Contains(testStderr.String(), `"code":"state_too_large"`) {
		t.Fatal("missing fixed error log")
	}
	// Exercise the JSON-lines emitter and reader too: health is an event,
	// carries no ack waiter, and exposes only code and byte count.
	var wire bytes.Buffer
	em := newEmitter(&wire, s.acks)
	em.emit(frames[0])
	var obj map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(wire.Bytes()), &obj); err != nil || len(obj) != 3 {
		t.Fatalf("health wire shape = %s err=%v", wire.Bytes(), err)
	}
	got := make(chan Frame, 1)
	conn := NewConn(&wire, io.Discard, func(f Frame) { got <- f })
	<-conn.Done()
	if f := <-got; f.Code != "state_too_large" || f.Bytes != wantBytes {
		t.Fatalf("health round trip = %+v", f)
	}
}

func TestRestoredSnapshotTransfersOnceAndStopIsIdempotent(t *testing.T) {
	s := newTestSession()
	s.ctx = t.Context()
	original, err := openWADB(t.Context(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(original.close)
	newSavedDevice(t, original, "1")
	s.waSnap, err = original.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	wdb, err := s.deviceStore()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Stop)
	if s.waSnap != nil {
		t.Fatal("restored start snapshot still retained")
	}
	devices, err := wdb.container.GetAllDevices(t.Context())
	if err != nil || len(devices) != 1 {
		t.Fatalf("devices=%d err=%v", len(devices), err)
	}
	if err := devices[0].Sessions.PutSession(t.Context(), "latest", []byte("latest-ratchet")); err != nil {
		t.Fatal(err)
	}
	rec := &emitRecorder{hub: s.acks}
	s.emit = rec.emit
	s.Stop()
	final := rec.last()
	var state savedState
	if err := json.Unmarshal(final.State, &state); err != nil {
		t.Fatal(err)
	}
	restored, err := openWADB(t.Context(), state.WADB, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restored.close)
	devices, err = restored.container.GetAllDevices(t.Context())
	if err != nil || len(devices) != 1 {
		t.Fatalf("final devices=%d err=%v", len(devices), err)
	}
	if got, err := devices[0].Sessions.GetSession(t.Context(), "latest"); err != nil || string(got) != "latest-ratchet" {
		t.Fatalf("Stop rolled back live state: %q %v", got, err)
	}
	n := len(rec.all())
	s.Stop()
	if len(rec.all()) != n || s.waDB != nil {
		t.Fatal("second Stop emitted state or kept the live store")
	}
}

func TestProtocolDocumentsHeadCheckpointAndHealth(t *testing.T) {
	// The reconciliation promise is documentation: guard its user-visible
	// consequence rather than asserting a nonexistent backfill behavior.
	header, err := os.ReadFile("protocol.go")
	if err != nil {
		t.Fatal(err)
	}
	text := string(header)
	if !strings.Contains(text, "head checkpoint") || !strings.Contains(text, "newest page on start") || strings.Contains(text, "history backfill reconciles") || !strings.Contains(text, "health       Code, Bytes") {
		t.Fatal("protocol header misstates reconciliation or omits health")
	}
}

func TestStartPlatformSelection(t *testing.T) {
	for _, tc := range []struct {
		name, platform string
		want           types.Platform
	}{
		{"facebook", "facebook", types.Facebook},
		{"messenger", "messenger", types.Messenger},
		{"default", "", types.Messenger},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSession()
			hub := newAckHub()
			rec := &emitRecorder{hub: hub}
			state, err := json.Marshal(savedState{
				Platform: tc.platform,
				Messagix: json.RawMessage(`{"Configs":{"BrowserConfigTable":{"CurrentUserInitialData":{"ACCOUNT_ID":"1","USER_ID":"1","NAME":"Me"}},"Bitmap":{},"CSRBitmap":{}},"SyncStore":{}}`),
			})
			if err != nil {
				t.Fatal(err)
			}
			_, _, err = s.Start(t.Context(), rec.emit, hub, map[string]string{"c_user": "1", "xs": "x", "datr": "d"}, state, "logout_only", tc.platform)
			t.Cleanup(s.Stop)
			if err != nil {
				t.Fatal(err)
			}
			if got := s.client.GetPlatform(); got != tc.want {
				t.Fatalf("platform = %v, want %v", got, tc.want)
			}
			var saved savedState
			if err := json.Unmarshal(s.dump(), &saved); err != nil {
				t.Fatal(err)
			}
			if tc.platform == "facebook" && saved.Platform != "facebook" {
				t.Fatalf("saved platform = %q", saved.Platform)
			}
		})
	}
}

func TestStartPlatformRefusals(t *testing.T) {
	validCookies := map[string]string{"c_user": "1", "xs": "x", "datr": "d"}
	for _, tc := range []struct {
		name, platform, state string
		cookies               map[string]string
	}{
		{"unknown", "instagram", "", validCookies},
		{"facebook missing required cookie", "facebook", "", map[string]string{"c_user": "1", "xs": "x", "sb": "s", "fr": "f"}},
		{"legacy state on facebook", "facebook", `{}`, validCookies},
		{"messenger state on facebook", "facebook", `{"platform":"messenger"}`, validCookies},
		{"facebook state on messenger", "messenger", `{"platform":"facebook"}`, validCookies},
		{"unknown state platform", "messenger", `{"platform":"instagram"}`, validCookies},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newSession()
			hub := newAckHub()
			rec := &emitRecorder{hub: hub}
			_, _, err := s.Start(t.Context(), rec.emit, hub, tc.cookies, []byte(tc.state), "logout_only", tc.platform)
			t.Cleanup(s.Stop)
			if codeOf(err) != "bad_request" {
				t.Fatalf("error = %v, want bad_request", err)
			}
			if s.client != nil || s.waSnap != nil {
				t.Fatal("refused input restored a session")
			}
		})
	}
}

func TestStartFramePassesPlatform(t *testing.T) {
	fb := &fakeBackend{}
	runLoop(t, fb, `{"t":"start","id":1,"platform":"facebook"}`, `{"t":"stop","id":2}`)
	if fb.startPlatform != "facebook" {
		t.Fatalf("platform = %q", fb.startPlatform)
	}
}
