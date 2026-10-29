// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
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

// resumeSync is the GraphQL sync a page load runs, for the inbox (1) and the contacts
// (2), for a session resumed from saved state, which skipped the page load. The
// version and the sync params come from the saved state's own copy of the page
// config, so nothing needs the page. One attempt, bounded by resumeSyncTimeout, cancel
// honoured: a failure logs a fixed code and returns what was applied (nothing if
// database 1 failed).
//
// Each table is only applied to the session (frames for the initial emitter); the
// helper never writes the library's sync state. The cursors therefore stay where the
// saved state left them and the library's own socket sync replays the same rows. A
// replay is harmless: thread frames merge into the same thread record and are upserts
// downstream, and the helper's messages carry their ids, which the connector's
// import dedupes. The fail-closed rule for message requests applies to every row.
func (s *messagixSession) resumeSync(ctx context.Context, saved json.RawMessage) []Frame {
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
		return nil
	}
	params := cfg.Configs.BrowserConfigTable.LSPlatformMessengerSyncParams
	request := s.lsRequest
	if request == nil {
		s.mu.Lock()
		cli := s.client
		s.mu.Unlock()
		if cli == nil {
			return nil
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
				logCode("warn", "resume_sync_failed")
			}
			if db == 1 {
				return nil
			}
			continue
		}
		frames = append(frames, s.handleTableFrames(tbl)...)
		rows += countRows(tbl)
	}
	s.mu.Lock()
	s.resumeRows = rows
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
