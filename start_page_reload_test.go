// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
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
