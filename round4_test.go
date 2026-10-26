// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	waTypes "go.mau.fi/whatsmeow/types"
)

func TestCancelRunningAndQueuedListingLetsNextCommandRun(t *testing.T) {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	t.Cleanup(func() { inR.Close(); inW.Close(); outR.Close(); outW.Close() })
	fb := &cancellableListingBackend{}
	done := make(chan int, 1)
	go func() {
		done <- run(inR, outW, func() backend { return fb })
		outW.Close()
	}()
	events := make(chan Frame, 20)
	conn := NewConn(outR, inW, func(f Frame) { events <- f })
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if _, err := conn.Call(ctx, Frame{T: TStart}); err != nil {
		t.Fatal(err)
	}
	next := func() Frame {
		t.Helper()
		select {
		case f := <-events:
			return f
		case <-ctx.Done():
			t.Fatal("listing blocked the serial loop")
			return Frame{}
		}
	}
	if err := conn.Send(Frame{T: TThreads, ID: 20}); err != nil {
		t.Fatal(err)
	}
	for f := next(); f.T != TThread || f.ThreadEvt.ID != "first-batch"; f = next() {
	}
	// The second listing is queued behind the running one. Neither cancel is
	// allowed to wait for that serial queue, and neither gets its own answer.
	for _, f := range []Frame{
		{T: TThreads, ID: 21}, {T: TCancel, ID: 90, Target: 21},
		{T: TCancel, ID: 91, Target: 20},
		{T: TSend, ID: 22, Thread: "42", OTID: "1", Text: "next"},
	} {
		if err := conn.Send(f); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int64{20, 21, 22} {
		f := next()
		if f.ID != id || (id != 22 && (f.T != TError || f.Err != "cancelled")) || (id == 22 && f.T != TSent) {
			t.Fatalf("command %d: %+v", id, f)
		}
	}
	// An unknown/completed target is a no-op; it cannot poison a future call.
	if err := conn.Send(Frame{T: TCancel, Target: 999}); err != nil {
		t.Fatal(err)
	}
	if err := conn.Send(Frame{T: TStop}); err != nil {
		t.Fatal(err)
	}
	select {
	case code := <-done:
		if code != 0 || fb.sentText != "next" {
			t.Fatalf("exit=%d send=%q", code, fb.sentText)
		}
	case <-ctx.Done():
		t.Fatal("cancel did not release the loop promptly")
	}
	<-conn.Done()
	if len(events) != 0 {
		t.Fatalf("cancel emitted its own answer: %+v", <-events)
	}
}

type fakeHTTP func(*http.Request) (*http.Response, error)

func (f fakeHTTP) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newWebTestClient() *messagix.Client {
	ck := &cookies.Cookies{Platform: types.Messenger}
	ck.UpdateValues(map[cookies.MetaCookieName]string{"c_user": "1", "xs": "test-cookie", "datr": "test-device"})
	return messagix.NewClient(ck, zerolog.Nop(), &messagix.Config{})
}

func httpResponse(r *http.Request, body string) *http.Response {
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: r}
}

func TestWebLogoutWithoutSyncedStateUsesLoadedPageToken(t *testing.T) {
	for _, state := range []string{"missing", "expired"} {
		t.Run(state, func(t *testing.T) {
			cli := newWebTestClient()
			if state == "expired" {
				blob := []byte(`{"Timestamp":"2020-01-01T00:00:00Z"}`)
				if !errors.Is(cli.LoadState(blob), messagix.ErrCachedStateTooOld) {
					t.Fatal("fixture was not expired")
				}
			}
			pageLoads, logouts := 0, 0
			cli.GetHTTP().SetTransportOverride(fakeHTTP(func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && r.URL.String() == cli.GetEndpoint("messages"):
					pageLoads++
					if c, err := r.Cookie("xs"); err != nil || c.Value != "test-cookie" {
						t.Fatal("page fetch omitted session cookies")
					}
					return httpResponse(r, `<html><script type="application/json">{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[["DTSGInitData",[],{"token":"page+token&value"},1],["CurrentUserInitialData",[],{"ACCOUNT_ID":"1","USER_ID":"1","NAME":"Me"},2]]}}]]]}</script><script src="https://static.invalid/version.js"></script></html>`), nil
				case r.URL.String() == "https://static.invalid/version.js":
					return httpResponse(r, `__d("LSVersion",[],function(a,b,c,d,e,f){e.exports="123"})`), nil
				case r.Method == http.MethodPost && r.URL.String() == cli.GetEndpoint("graphql"):
					return httpResponse(r, `{"data":{"viewer":{"lightspeed_web_request":{"payload":"{\"step\":[1]}","dependencies":[]}}}}`), nil
				case r.Method == http.MethodPost && r.URL.String() == logoutURL:
					logouts++
					if err := r.ParseForm(); err != nil || r.Form.Get("fb_dtsg") != "page+token&value" || r.Form.Get("ref") != "mb" {
						t.Fatal("logout did not post the page token")
					}
					if c, err := r.Cookie("xs"); err != nil || c.Value != "test-cookie" {
						t.Fatal("logout omitted session cookies")
					}
					return httpResponse(r, ""), nil
				default:
					t.Fatalf("unexpected HTTP request: %s %s", r.Method, r.URL)
					return nil, errors.New("unexpected request")
				}
			}))
			// This is the real no-cache page-loading path, including messagix's
			// config parser. No socket has been connected or synced.
			if user, _, err := cli.LoadMessagesPage(t.Context()); err != nil || user.GetFBID() != 1 {
				t.Fatalf("page load: user=%v err=%v", user, err)
			}
			if blob, err := cli.DumpState(); err != nil || len(blob) != 0 {
				t.Fatalf("fixture unexpectedly has synced state: %s %v", blob, err)
			}
			s := newTestSession()
			s.client, s.logoutOnly = cli, true
			if err := s.Logout(t.Context()); err != nil {
				t.Fatal(err)
			}
			if pageLoads != 1 || logouts != 1 || s.e2eeTry || s.waDB != nil {
				t.Fatalf("page loads=%d web logouts=%d E2EE=%v", pageLoads, logouts, s.e2eeTry)
			}
		})
	}
}

func TestSerialDeliveriesSealWithoutBurstWait(t *testing.T) {
	s := newTestSession()
	rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
	s.emit = rec.emit
	done := make(chan bool, 1)
	go func() {
		for i := range 3 {
			s.markStateDirty()
			if !s.handleWAEvent(fbMessage("4242", "42", string(rune('a'+i)), "serial")) {
				done <- false
				return
			}
		}
		done <- true
	}()
	select {
	case ok := <-done:
		if !ok {
			t.Fatal("serial delivery failed")
		}
	case <-time.After(500 * time.Millisecond): // three old 250ms waits cannot pass
		<-done
		t.Fatal("serial handler still waits for a burst that cannot arrive")
	}
	states, messages := 0, 0
	for _, f := range rec.all() {
		if f.T == TState {
			states++
		}
		if f.T == TMessage {
			messages++
			if states != messages {
				t.Fatal("serial event was not preceded by its current state")
			}
		}
	}
	if states != 3 || messages != 3 {
		t.Fatalf("states=%d messages=%d", states, messages)
	}
}

func TestSnapshotVacuumsOnlyAboveQuarterFree(t *testing.T) {
	wdb, err := openWADB(t.Context(), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(wdb.close)
	if _, err := wdb.db.ExecContext(t.Context(), "CREATE TABLE waste (content BLOB)"); err != nil {
		t.Fatal(err)
	}
	for range 128 {
		if _, err := wdb.db.ExecContext(t.Context(), "INSERT INTO waste VALUES (zeroblob(8192))"); err != nil {
			t.Fatal(err)
		}
	}
	counts := func() (free, total int) {
		t.Helper()
		if err := wdb.db.QueryRowContext(t.Context(), "PRAGMA freelist_count").Scan(&free); err != nil {
			t.Fatal(err)
		}
		if err := wdb.db.QueryRowContext(t.Context(), "PRAGMA page_count").Scan(&total); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := wdb.db.ExecContext(t.Context(), "DELETE FROM waste WHERE rowid <= 8"); err != nil {
		t.Fatal(err)
	}
	free, total := counts()
	if free == 0 || free*4 > total {
		t.Fatalf("small deletion fixture: free=%d total=%d", free, total)
	}
	if _, err := wdb.snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if afterFree, afterTotal := counts(); afterFree != free || afterTotal != total {
		t.Fatal("small freelist caused a full database rebuild")
	}
	if _, err := wdb.db.ExecContext(t.Context(), "DELETE FROM waste"); err != nil {
		t.Fatal(err)
	}
	if free, total = counts(); free*4 <= total {
		t.Fatalf("large deletion fixture: free=%d total=%d", free, total)
	}
	if _, err := wdb.snapshot(t.Context()); err != nil {
		t.Fatal(err)
	}
	if free, afterTotal := counts(); free != 0 || afterTotal >= total {
		t.Fatal("large freelist did not compact")
	}
}

func TestOversizedStateBacksOffAndRecovers(t *testing.T) {
	s := newTestSession()
	s.histDev = &securehistory.Device{StoragePrivateKey: bytes.Repeat([]byte{1}, maxLine)}
	s.stateDirty = true
	rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
	s.emit = rec.emit
	for _, delay := range []time.Duration{2, 4, 8, 16, 32, 64, 128, 256, 300, 300} {
		if s.sealState() || !s.stateDirty || s.stateRetryDelay != delay*time.Second {
			t.Fatalf("oversized state: delay=%v want=%vs dirty=%v", s.stateRetryDelay, delay, s.stateDirty)
		}
		before := len(rec.all())
		if s.flushState(false) || s.sealState() || len(rec.all()) != before {
			t.Fatal("oversized state retried before backoff expired")
		}
		s.stateRetryAt = time.Now().Add(-time.Second) // advance to the next retry
	}
	s.histDev = nil
	if !s.sealState() || rec.last().T != TState || s.stateRetryDelay != 0 || !s.stateRetryAt.IsZero() {
		t.Fatal("state did not recover after becoming small enough")
	}
	s.markStateDirty()
	if !s.sealState() {
		t.Fatal("recovered state retained the oversized backoff")
	}
}

func TestLogoutClearsOversizedStateBackoff(t *testing.T) {
	s := newTestSession()
	s.histDev = &securehistory.Device{StoragePrivateKey: bytes.Repeat([]byte{1}, maxLine)}
	s.stateDirty = true
	rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
	s.emit = rec.emit
	if s.sealState() {
		t.Fatal("oversized state was sealed")
	}
	if err := s.Logout(t.Context()); err != nil {
		t.Fatal(err)
	}
	if last := rec.last(); last.T != TState || string(last.State) != "{}" {
		t.Fatal("oversized backoff prevented logout from sealing empty state")
	}
}

func TestAddDeviceFailureRollsBackAndHistoryPINRetries(t *testing.T) {
	for _, outcome := range []string{"transport", "rejected", "missing_id"} {
		t.Run(outcome, func(t *testing.T) {
			s := newTestSession()
			previous := &securehistory.Device{StoragePrivateKey: []byte("previous-unconfirmed")}
			s.histDev = previous
			if err := json.Unmarshal([]byte(`{"id":"backup","virtual_devices":[{"device_type":"HSM","client_generated_id":"hsm"}]}`), &s.histBak); err != nil {
				t.Fatal(err)
			}
			rec := &emitRecorder{hub: s.acks, ackOnEmit: true}
			s.emit = rec.emit
			_, err := s.persistHistoryEnrollment(t.Context(), &securehistory.Device{ID: "not-confirmed", StoragePrivateKey: []byte("new-keys")}, func() (*securehistory.Device, error) {
				if state := s.checkHistory(t.Context()); state != HistoryNeedsPIN {
					t.Fatalf("unconfirmed device reported %s", state)
				}
				switch outcome {
				case "transport":
					return nil, errors.New("transport failed")
				case "rejected":
					return nil, &securehistory.EnrollmentError{Code: "rejected"}
				default:
					return &securehistory.Device{}, nil
				}
			})
			if err == nil || s.histDev != previous || !s.stateDirty || s.checkHistory(t.Context()) != HistoryNeedsPIN {
				t.Fatal("failed restore retained the new enrollment")
			}
			// A later history_pin must reach PIN recovery again, rather than
			// returning enrolled merely because prepared keys were retained.
			s.client = newWebTestClient()
			requests := 0
			s.client.GetHTTP().SetTransportOverride(fakeHTTP(func(r *http.Request) (*http.Response, error) {
				requests++
				if r.Header.Get("x-fb-friendly-name") != "VestaServerGraphQLLoginProviderInitQuery" {
					t.Fatal("history_pin did not retry recovery")
				}
				return httpResponse(r, `{"data":{"fbid_based_auth_layer_vesta_login_init":{"is_registered":true,"attempts_remaining":0}}}`), nil
			}))
			wdb, err := openWADB(t.Context(), nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(wdb.close)
			s.waDevice = newSavedDevice(t, wdb, "1")
			s.e2eeTry = true
			state, err := s.HistoryPIN(t.Context(), []byte("123456"))
			var auth *authErr
			if requests != 1 || state == HistoryEnrolled || !errors.As(err, &auth) || auth.state != AuthPINLocked {
				t.Fatalf("retry: requests=%d state=%s err=%v", requests, state, err)
			}
		})
	}
}

func TestThreadTypeRequiresMessengerFactsAndEarlyParticipantsWait(t *testing.T) {
	for _, typ := range []table.ThreadType{table.ONE_TO_ONE, table.GROUP_THREAD, table.ENCRYPTED_OVER_WA_GROUP} {
		s := newTestSession()
		if frames := s.handleTableFrames(&table.LSTable{LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 42, ContactId: 7}}}); len(frames) != 0 || len(s.threads) != 0 {
			t.Fatal("early participant created a phantom thread")
		}
		frames := s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 42, ThreadType: typ}}})
		th := frames[0].ThreadEvt
		if len(frames) != 1 || !th.Typed || th.Encrypted != typ.IsWhatsApp() || len(s.threads) != 1 || !slices.ContainsFunc(th.Participants, func(p Person) bool { return p.ID == "7" }) {
			t.Fatalf("real thread lost type or buffered participant: %+v", frames)
		}
		if (th.Kind == "user") != typ.IsOneToOne() || len(s.participants) != 0 {
			t.Fatalf("thread type/participant buffer incorrect: %+v", th)
		}
	}
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: 42, MessageId: "m", SenderId: 7}}})
	if len(frames) < 1 || frames[0].T != TThread || frames[0].ThreadEvt.Typed {
		t.Fatal("a message promoted the default group to a real type")
	}
	wire, err := json.Marshal(frames[0])
	if err != nil || bytes.Contains(wire, []byte(`"typed":true`)) {
		t.Fatal("unknown type was qualified on the wire")
	}
	for _, server := range []string{waTypes.GroupServer, waTypes.MessengerServer} {
		evt := fbMessage("42", "7", "typed", "hello")
		evt.Info.Chat.Server = server
		frames = s.waMessageFrames(evt)
		if len(frames) == 0 || frames[0].ThreadEvt == nil || !frames[0].ThreadEvt.Typed {
			t.Fatal("Messenger JID did not qualify the real thread type")
		}
	}
}
