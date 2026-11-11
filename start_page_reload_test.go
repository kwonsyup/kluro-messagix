// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/crypto"
	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// reloadRig is a resumed start's openSocket with every network call constructed: the resume sync's
// GraphQL request, the page load, the saved-state restore and the socket connect, each counted.
type reloadRig struct {
	s        *messagixSession
	sync     map[int]int // resume-sync requests by database
	loads    int
	restores [][]byte
	connects int
}

func newReloadRig(t *testing.T, syncErr func(db int) error, load func(context.Context) (*table.LSTable, error)) *reloadRig {
	t.Helper()
	r := &reloadRig{s: newTestSession(), sync: map[int]int{}}
	r.s.ctx = context.Background()
	r.s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		r.sync[v.Database]++
		if err := syncErr(v.Database); err != nil {
			return nil, err
		}
		return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 600, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"}}}, nil
	}
	r.s.pageLoad = func(ctx context.Context) (*table.LSTable, error) {
		r.loads++
		return load(ctx)
	}
	r.s.stateRestore = func(saved json.RawMessage) error {
		r.restores = append(r.restores, slices.Clone(saved))
		return nil
	}
	return r
}

func (r *reloadRig) open(ctx context.Context, saved json.RawMessage) ([]Frame, error) {
	return r.s.openSocket(ctx, true, saved, func(context.Context) error { r.connects++; return nil })
}

func reloadPageErr() error {
	return fmt.Errorf("graphql error %w", &types.ErrorResponse{ErrorCode: 1357004, ErrorDescription: secretText})
}

func pageTable() *table.LSTable {
	return &table.LSTable{
		LSDeleteThenInsertThread:     []*table.LSDeleteThenInsertThread{{ThreadKey: 900, ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "P"}},
		LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 90, ArmadilloThreadID: 9090}},
	}
}

// codesAt is the log codes written at one level, in order.
func codesAt(t *testing.T, buf *bytes.Buffer, level string) []string {
	t.Helper()
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
		if line == "" {
			continue
		}
		var obj struct{ Level, Code string }
		if err := json.Unmarshal([]byte(line), &obj); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if obj.Level == level {
			out = append(out, obj.Code)
		}
	}
	return out
}

// A resume sync whose database 1 answers "reload page" (1357004) loads the page exactly once, never asks the resume
// sync again (the page's own table is that sync), applies the page's table before the socket opens, and connects.
func TestStartPageReloadOnReloadPageLoadsOnceAndUsesThePage(t *testing.T) {
	buf := captureCodes(t)
	saved := savedConfig(t, 123)
	connectSawPage := false
	r := newReloadRig(t, func(db int) error { return reloadPageErr() }, func(context.Context) (*table.LSTable, error) { return pageTable(), nil })
	frames, err := r.s.openSocket(context.Background(), true, saved, func(context.Context) error {
		r.connects++
		r.s.mu.Lock()
		connectSawPage = r.s.cutover[9090] == 90
		r.s.mu.Unlock()
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.loads != 1 || r.sync[1] != 1 || r.sync[2] != 0 || r.connects != 1 || len(r.restores) != 0 {
		t.Fatalf("loads=%d sync=%v connects=%d restores=%d, want one page load, one sync request, one connect, no restore",
			r.loads, r.sync, r.connects, len(r.restores))
	}
	if !connectSawPage {
		t.Fatal("the page's table must be applied before the socket opens")
	}
	if lastThread(frames, "900") == nil {
		t.Fatal("the page's thread frame is part of the initial frames")
	}
	if r.s.lastFullRec.IsZero() {
		t.Fatal("the start's page load must hold off fullReconnect's page load")
	}
	st := r.s.listingStats(0, 0, 0)
	if st["start_page_reload"] != 1 || st["resume_sync_rows"] != 2 {
		t.Fatalf("stats = %v, want start_page_reload 1 and the page's 2 rows", st)
	}
	if w := codesAt(t, buf, "warn"); !slices.Equal(w, []string{"resume_sync_failed.reload_page"}) {
		t.Fatalf("warn codes = %v: the existing warning class stays, nothing else warns", w)
	}
	if i := codesAt(t, buf, "info"); !slices.Equal(i, []string{"start_page_reload_result.ok", "start_page_reload.1"}) {
		t.Fatalf("info codes = %v", i)
	}
	if strings.Contains(buf.String(), secretText) {
		t.Fatal("error text must stay out of the log")
	}
}

// A page load that fails leaves the start as it was without the reload: the saved state is put back into the client,
// the start connects with no frames, and the failure is counted by class only.
func TestStartPageReloadFailureContinuesTheStartAndIsCounted(t *testing.T) {
	for name, tc := range map[string]struct {
		load       func(context.Context) (*table.LSTable, error)
		restoreErr error
		wantWarn   []string
	}{
		"server status": {
			load: func(context.Context) (*table.LSTable, error) {
				return nil, fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 500)
			},
			wantWarn: []string{"resume_sync_failed.reload_page", "start_page_reload_result.http_5xx"},
		},
		"signed out": {
			load: func(context.Context) (*table.LSTable, error) {
				return nil, fmt.Errorf("failed to load inbox: %w", httpclient.ErrTokenInvalidated)
			},
			wantWarn: []string{"resume_sync_failed.reload_page", "start_page_reload_result.token_invalid"},
		},
		"no table": {
			load:     func(context.Context) (*table.LSTable, error) { return nil, nil },
			wantWarn: []string{"resume_sync_failed.reload_page", "start_page_reload_result.no_table"},
		},
		"restore fails too": {
			load:       func(context.Context) (*table.LSTable, error) { return nil, errors.New(secretText) },
			restoreErr: errors.New(secretText),
			wantWarn:   []string{"resume_sync_failed.reload_page", "start_page_reload_result.other", "start_page_reload_restore_failed"},
		},
	} {
		t.Run(name, func(t *testing.T) {
			buf := captureCodes(t)
			saved := savedConfig(t, 123)
			r := newReloadRig(t, func(db int) error { return reloadPageErr() }, tc.load)
			if tc.restoreErr != nil {
				r.s.stateRestore = func(json.RawMessage) error { r.restores = append(r.restores, nil); return tc.restoreErr }
			}
			frames, err := r.open(context.Background(), saved)
			if err != nil || frames != nil || r.connects != 1 {
				t.Fatalf("err=%v frames=%v connects=%d: a failed reload must still connect, with nothing applied", err, frames, r.connects)
			}
			if r.loads != 1 || r.sync[1] != 1 || len(r.restores) != 1 {
				t.Fatalf("loads=%d sync=%v restores=%d, want exactly one of each", r.loads, r.sync, len(r.restores))
			}
			if r.s.lastFullRec.IsZero() {
				t.Fatal("a failed start reload must still hold off fullReconnect's page load")
			}
			if tc.restoreErr == nil && !bytes.Equal(r.restores[0], saved) {
				t.Fatal("the restore must load the start's own saved state")
			}
			if st := r.s.listingStats(0, 0, 0); st["start_page_reload"] != 1 || st["resume_sync_rows"] != 0 {
				t.Fatalf("stats = %v, want the reload counted and no rows", st)
			}
			if w := codesAt(t, buf, "warn"); !slices.Equal(w, tc.wantWarn) || strings.Contains(buf.String(), secretText) {
				t.Fatalf("warn codes = %v, want %v and no error text", w, tc.wantWarn)
			}
			if i := codesAt(t, buf, "info"); !slices.Equal(i, []string{"start_page_reload.1"}) {
				t.Fatalf("info codes = %v", i)
			}
		})
	}
}

// Only "reload page" on database 1 loads the page: every other resume-sync outcome, and a start that did not resume,
// connects without one and counts 0.
func TestNoStartPageReloadOnOtherAnswers(t *testing.T) {
	for name, syncErr := range map[string]func(db int) error{
		"success":                 func(int) error { return nil },
		"please log in":           func(int) error { return fmt.Errorf("x %w", &types.ErrorResponse{ErrorCode: 1357001}) },
		"server status":           func(int) error { return fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 401) },
		"other ls error":          func(int) error { return &types.ErrorResponse{ErrorCode: 1675030} },
		"transport":               func(int) error { return fmt.Errorf("%w: %w", httpclient.ErrRequestFailed, errors.New("down")) },
		"generic":                 func(int) error { return errors.New("boom") },
		"reload page on contacts": func(db int) error { return map[bool]error{true: reloadPageErr()}[db == 2] },
	} {
		t.Run(name, func(t *testing.T) {
			buf := captureCodes(t)
			r := newReloadRig(t, syncErr, func(context.Context) (*table.LSTable, error) { return pageTable(), nil })
			if _, err := r.open(context.Background(), savedConfig(t, 123)); err != nil {
				t.Fatal(err)
			}
			if r.loads != 0 || r.connects != 1 || len(r.restores) != 0 || r.s.listingStats(0, 0, 0)["start_page_reload"] != 0 {
				t.Fatalf("loads=%d connects=%d restores=%d: no page load for this answer", r.loads, r.connects, len(r.restores))
			}
			if i := codesAt(t, buf, "info"); !slices.Equal(i, []string{"start_page_reload.0"}) {
				t.Fatalf("info codes = %v", i)
			}
		})
	}

	// not resumed: no sync, no reload, no reload code
	buf := captureCodes(t)
	r := newReloadRig(t, func(int) error { return reloadPageErr() }, func(context.Context) (*table.LSTable, error) { return pageTable(), nil })
	if _, err := r.s.openSocket(context.Background(), false, nil, func(context.Context) error { r.connects++; return nil }); err != nil {
		t.Fatal(err)
	}
	if r.loads != 0 || len(r.sync) != 0 || r.connects != 1 || buf.Len() != 0 {
		t.Fatalf("a fresh start only connects: loads=%d sync=%v connects=%d log=%q", r.loads, r.sync, r.connects, buf.String())
	}
}

// Never more than one page load per start, whatever keeps answering "reload page": the resume sync is not asked
// again, and a page load that itself answers "reload page" is not repeated.
func TestStartPageReloadNeverRepeats(t *testing.T) {
	for name, load := range map[string]func(context.Context) (*table.LSTable, error){
		"page load answers reload page": func(context.Context) (*table.LSTable, error) { return nil, reloadPageErr() },
		"page load succeeds":            func(context.Context) (*table.LSTable, error) { return pageTable(), nil },
	} {
		t.Run(name, func(t *testing.T) {
			captureCodes(t)
			r := newReloadRig(t, func(int) error { return reloadPageErr() }, load)
			if _, err := r.open(context.Background(), savedConfig(t, 123)); err != nil {
				t.Fatal(err)
			}
			if r.loads != 1 || r.sync[1] != 1 || r.sync[2] != 0 || r.connects != 1 {
				t.Fatalf("loads=%d sync=%v connects=%d, want exactly one page load and one sync request", r.loads, r.sync, r.connects)
			}
		})
	}
}

// Cancelling Start during the page load stops it before the socket opens; the page load's own timeout is not a
// cancel and continues the start.
func TestStartPageReloadHonoursCancel(t *testing.T) {
	captureCodes(t)
	blocked := func(ctx context.Context) (*table.LSTable, error) { <-ctx.Done(); return nil, ctx.Err() }
	r := newReloadRig(t, func(int) error { return reloadPageErr() }, blocked)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	if _, err := r.open(ctx, savedConfig(t, 123)); !errors.Is(err, context.Canceled) || r.connects != 0 {
		t.Fatalf("a cancelled Start must not connect: err=%v connects=%d", err, r.connects)
	}

	old := startPageReloadTimeout
	startPageReloadTimeout = 50 * time.Millisecond
	defer func() { startPageReloadTimeout = old }()
	r2 := newReloadRig(t, func(int) error { return reloadPageErr() }, blocked)
	frames, err := r2.open(context.Background(), savedConfig(t, 123))
	if err != nil || frames != nil || r2.connects != 1 || r2.loads != 1 || len(r2.restores) != 1 {
		t.Fatalf("a page-load timeout must restore and still connect: err=%v frames=%v connects=%d loads=%d restores=%d",
			err, frames, r2.connects, r2.loads, len(r2.restores))
	}
}

// savedClientState is a constructed saved messagix state blob in the library's own dump format: a page config
// (version, tokens, user, sync params) and the sync store's cursors.
func savedClientState(t *testing.T) json.RawMessage {
	t.Helper()
	cursor1, cursor2 := "saved-cursor-1", "saved-cursor-2"
	b, err := json.Marshal(struct {
		Configs     *httpclient.Configs
		SyncStore   map[int64]*socket.QueryMetadata
		PacketsSent uint16
		SessionID   int64
		Timestamp   time.Time
	}{
		Configs: &httpclient.Configs{
			VersionID: 123,
			LSDToken:  "saved-lsd",
			// a real dump carries the page's bitmaps: a page load sets them before any dump
			Bitmap:    crypto.NewBitmap().Update([]int{1, 5, 9}),
			CSRBitmap: crypto.NewBitmap().Update([]int{2, 4}),
			BrowserConfigTable: &types.SchedulerJSDefineConfig{
				CurrentUserInitialData:        types.CurrentUserInitialData{AccountID: "1", UserID: "1", AppID: "saved-app", Name: "Saved"},
				LSPlatformMessengerSyncParams: types.LSPlatformMessengerSyncParams{Mailbox: "mb-params", Contact: "ct-params"},
			},
		},
		SyncStore: map[int64]*socket.QueryMetadata{
			1: {DatabaseID: 1, SendSyncParams: true, LastAppliedCursor: &cursor1, SyncChannel: socket.MailBox},
			2: {DatabaseID: 2, SendSyncParams: true, LastAppliedCursor: &cursor2, SyncChannel: socket.Contact},
		},
		PacketsSent: 7,
		Timestamp:   time.Now(),
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// clientStateOf is a dumped client state without its save time, decoded for comparison; nil if the client has no
// state to dump.
func clientStateOf(t *testing.T, blob json.RawMessage) map[string]any {
	t.Helper()
	if len(blob) == 0 {
		return nil
	}
	var m map[string]any
	if err := json.Unmarshal(blob, &m); err != nil {
		t.Fatal(err)
	}
	delete(m, "Timestamp")
	return m
}

// failingPageTransport serves a messages page that parses (it writes a new token and user into the client's config
// in place) and then fails the page setup's GraphQL sync with an undecodable answer. That happens after the library
// has replaced the sync manager and marked the socket unsynced: the deepest partial state a failed page load leaves.
// It counts page loads.
func failingPageTransport(t *testing.T, cli *messagix.Client, pageLoads *atomic.Int64) {
	cli.GetHTTP().SetTransportOverride(fakeHTTP(func(r *http.Request) (*http.Response, error) {
		switch {
		case r.Method == http.MethodGet && r.URL.String() == cli.GetEndpoint("messages"):
			pageLoads.Add(1)
			return httpResponse(r, `<html><script type="application/json">{"require":[["ScheduledServerJS","handle",null,[{"__bbox":{"define":[["DTSGInitData",[],{"token":"partial-token"},1],["CurrentUserInitialData",[],{"ACCOUNT_ID":"1","USER_ID":"1","APP_ID":"partial-app","NAME":"Partial"},2]]}}]]]}</script><script src="https://static.invalid/version.js"></script></html>`), nil
		case r.URL.String() == "https://static.invalid/version.js":
			return httpResponse(r, `__d("LSVersion",[],function(a,b,c,d,e,f){e.exports="456"})`), nil
		case r.Method == http.MethodPost:
			return httpResponse(r, "not json"), nil
		}
		t.Errorf("unexpected HTTP request: %s %s", r.Method, r.URL)
		return nil, errors.New("unexpected request")
	}))
}

// The default wiring, against a real messagix client: the library's own page load fails after it has rewritten the
// client's config and sync state, and the default restore (the client's LoadState) puts back exactly the start's
// saved config and sync store, so the start continues as it would have without the reload.
func TestStartPageReloadDefaultRestorePutsBackTheSavedClientState(t *testing.T) {
	saved := savedClientState(t)

	// The premise: the same failed page load, without a restore, leaves the client's state changed.
	control := newWebTestClient()
	if err := control.LoadState(saved); err != nil {
		t.Fatal(err)
	}
	var controlLoads atomic.Int64
	failingPageTransport(t, control, &controlLoads)
	if _, _, err := control.LoadMessagesPage(t.Context()); err == nil || controlLoads.Load() != 1 {
		t.Fatalf("fixture: the page load must fail after one page fetch: err=%v loads=%d", err, controlLoads.Load())
	}
	blob, _ := control.DumpState()
	if after := clientStateOf(t, blob); reflect.DeepEqual(after, clientStateOf(t, saved)) {
		t.Fatal("fixture: a failed page load must leave the client's state changed")
	}

	buf := captureCodes(t)
	cli := newWebTestClient()
	if err := cli.LoadState(saved); err != nil {
		t.Fatal(err)
	}
	var pageLoads atomic.Int64
	failingPageTransport(t, cli, &pageLoads)
	s := newTestSession()
	s.ctx = context.Background()
	s.client = cli
	s.lsRequest = func(context.Context, *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		return nil, reloadPageErr()
	}
	connects := 0
	frames, err := s.openSocket(context.Background(), true, saved, func(context.Context) error { connects++; return nil })
	if err != nil || frames != nil || connects != 1 || pageLoads.Load() != 1 {
		t.Fatalf("err=%v frames=%v connects=%d page loads=%d", err, frames, connects, pageLoads.Load())
	}
	blob, err = cli.DumpState()
	if err != nil {
		t.Fatal(err)
	}
	if got, want := clientStateOf(t, blob), clientStateOf(t, saved); !reflect.DeepEqual(got, want) {
		t.Fatalf("restored client state = %v, want the saved %v", got, want)
	}
	if cur, err := cli.GetCurrentAccount(); err != nil || cur.GetName() != "Saved" {
		t.Fatalf("the client must serve the saved user again: %v %v", cur, err)
	}
	if w := codesAt(t, buf, "warn"); !slices.Equal(w, []string{"resume_sync_failed.reload_page", "start_page_reload_result.decode"}) {
		t.Fatalf("warn codes = %v", w)
	}
}

// A failed start reload holds off fullReconnect's page load as a successful one does: a fullReconnect right after
// it, inside fullReconnect's spacing, loads no page.
func TestStartPageReloadFailureHoldsOffFullReconnect(t *testing.T) {
	captureCodes(t)
	saved := savedClientState(t)
	cli := newWebTestClient()
	if err := cli.LoadState(saved); err != nil {
		t.Fatal(err)
	}
	var pageLoads atomic.Int64
	failingPageTransport(t, cli, &pageLoads)
	s := newTestSession()
	s.ctx = context.Background()
	s.client = cli
	s.lsRequest = func(context.Context, *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		return nil, reloadPageErr()
	}
	if _, err := s.openSocket(context.Background(), true, saved, func(context.Context) error { return nil }); err != nil {
		t.Fatal(err)
	}
	if pageLoads.Load() != 1 || s.lastFullRec.IsZero() {
		t.Fatalf("page loads=%d lastFullRec zero=%v: the failed reload must still hold off fullReconnect", pageLoads.Load(), s.lastFullRec.IsZero())
	}
	s.fullReconnect()
	if pageLoads.Load() != 1 {
		t.Fatalf("page loads = %d: fullReconnect right after the start's reload must not load the page again", pageLoads.Load())
	}
}
