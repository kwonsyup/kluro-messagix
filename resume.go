// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// resumeSyncTimeout bounds the whole resumed-start sync.
var resumeSyncTimeout = 20 * time.Second

// resumeSyncDatabases are the sync databases the resume sync asks for: the inbox (1)
// and the contacts (2). The page load also asks for 95, the message-requests inbox;
// requests are not imported, and 95's cursor is left to the library's own socket sync.
var resumeSyncDatabases = []int64{1, 2}

// startPageReloadTimeout bounds the one page load a resumed start may make when the
// resume sync says the saved page config is stale.
var startPageReloadTimeout = 60 * time.Second

// resumeSync is the GraphQL sync a page load runs, for the inbox (1) and the contacts
// (2), for a session resumed from saved state, which skipped the page load. The
// version and the sync params come from the saved state's own copy of the page
// config, so nothing needs the page. One attempt, bounded by resumeSyncTimeout, cancel
// honoured: a failure logs a fixed code with the request's error class
// (requestErrorClass, never the error text) and returns what was applied (nothing if
// database 1 failed) with database 1's error, if it failed.
//
// Each table is only applied to the session (frames for the initial emitter); the
// helper never writes the library's sync state. The cursors therefore stay where the
// saved state left them and the library's own socket sync replays the same rows. A
// replay is harmless: thread frames merge into the same thread record and are upserts
// downstream, and the helper's messages carry their ids, which the connector's
// import dedupes. The fail-closed rule for message requests applies to every row.
func (s *messagixSession) resumeSync(ctx context.Context, saved json.RawMessage) ([]Frame, error) {
	var cfg struct {
		Configs struct {
			VersionID          int64
			BrowserConfigTable struct {
				LSPlatformMessengerSyncParams types.LSPlatformMessengerSyncParams
			}
		}
	}
	if err := json.Unmarshal(saved, &cfg); err != nil || cfg.Configs.VersionID == 0 {
		logCode("warn", "resume_sync_skipped")
		return nil, nil
	}
	params := cfg.Configs.BrowserConfigTable.LSPlatformMessengerSyncParams
	request := s.lsRequest
	if request == nil {
		s.mu.Lock()
		cli := s.client
		s.mu.Unlock()
		if cli == nil {
			return nil, nil
		}
		request = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
			return cli.GetHTTP().MakeLSRequest(ctx, v, 1)
		}
	}
	wctx, cancel := context.WithTimeout(ctx, resumeSyncTimeout)
	defer cancel()
	var frames []Frame
	rows := 0
	for _, db := range resumeSyncDatabases {
		syncParams := params.Contact
		if db == 1 {
			syncParams = params.Mailbox
		}
		tbl, err := request(wctx, &graphql.LSPlatformGraphQLLightspeedVariables{
			Database:          int(db),
			LastAppliedCursor: nil,
			Version:           cfg.Configs.VersionID,
			EpochID:           0,
			SyncParams:        &syncParams,
		})
		if err != nil || tbl == nil {
			if ctx.Err() == nil {
				logCode("warn", "resume_sync_failed."+requestErrorClass(err))
			}
			if db == 1 {
				if err == nil {
					err = errNoTable
				}
				return nil, err
			}
			continue
		}
		frames = append(frames, s.handleTableFrames(tbl)...)
		rows += countRows(tbl)
	}
	s.mu.Lock()
	s.resumeRows = rows
	s.mu.Unlock()
	return frames, nil
}

// errNoTable is database 1's resume-sync failure when the request returned neither a
// table nor an error.
var errNoTable = errors.New("no table")

// startPageReload is the one page load a resumed start makes when the resume sync's
// database 1 answered "reload page" (1357004): the saved page config's tokens no
// longer validate, and every HTTP GraphQL request of this run (the history check too)
// would keep getting the same answer. It is the library's own page load, with the
// session's cookies and no sign-in, the same call a non-resumed start and
// fullReconnect make. On success the page's own table stands in for the resume sync
// (it is the sync the resume sync stands in for), the library starts the socket as a
// fresh page would (initial thread fetch, real sync params), and the refreshed config
// is saved by the next state dump after the socket syncs. It is never repeated: the
// caller makes it at most once per start, and it sets lastFullRec so fullReconnect
// does not load the page again within its own spacing.
//
// A failed load leaves the start exactly as it was before: the saved state is loaded
// into the client again (a partial page parse can have overwritten its config) and
// the start continues resumed, with no frames, as it did without this reload. The
// result is logged by class only: start_page_reload_result.ok or
// start_page_reload_result.<requestErrorClass>.
func (s *messagixSession) startPageReload(ctx context.Context, saved json.RawMessage) []Frame {
	load, restore := s.pageLoad, s.stateRestore
	if load == nil || restore == nil {
		s.mu.Lock()
		cli := s.client
		s.mu.Unlock()
		if cli == nil {
			return nil
		}
		if load == nil {
			load = func(ctx context.Context) (*table.LSTable, error) {
				_, tbl, err := cli.LoadMessagesPage(ctx)
				return tbl, err
			}
		}
		if restore == nil {
			restore = cli.LoadState
		}
	}
	s.mu.Lock()
	s.pageReloads++
	s.lastFullRec = time.Now()
	s.mu.Unlock()
	wctx, cancel := context.WithTimeout(ctx, startPageReloadTimeout)
	tbl, err := load(wctx)
	cancel()
	if err != nil || tbl == nil {
		logCode("warn", "start_page_reload_result."+requestErrorClass(err))
		if rerr := restore(saved); rerr != nil {
			logCode("warn", "start_page_reload_restore_failed")
		}
		return nil
	}
	logCode("info", "start_page_reload_result.ok")
	frames := s.handleTableFrames(tbl)
	s.mu.Lock()
	s.resumeRows = countRows(tbl)
	s.mu.Unlock()
	return frames
}

// countRows is the number of rows in a table, over every row-list field.
func countRows(tbl *table.LSTable) int {
	if tbl == nil {
		return 0
	}
	n := 0
	v := reflect.ValueOf(tbl).Elem()
	for i := 0; i < v.NumField(); i++ {
		if f := v.Field(i); f.Kind() == reflect.Slice || f.Kind() == reflect.Map {
			n += f.Len()
		}
	}
	return n
}
