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

// resumeSyncDatabases are the sync databases the page load's GraphQL sync asks for.
var resumeSyncDatabases = []int64{1, 2, 95}

// resumeSync is the GraphQL sync a page load runs for databases 1, 2 and 95 (the
// library's setupConfigs and SyncDataGraphQL), for a session resumed from saved
// state, which skipped the page load. The version and the sync params come from the
// saved state's own copy of the page config, so nothing needs the page. One attempt,
// bounded by resumeSyncTimeout, cancel honoured: a failure logs a fixed code and
// returns what was applied (nothing if database 1 failed).
//
// Each database's table is applied to the session first and only then given to the
// library's sync bookkeeping (PostHandlePublishResponse), which moves that database's
// cursor and thread ranges, in the order the library applies them: every row is
// consumed before its cursor moves, so none is acknowledged and lost. The library
// itself keeps only database 1's table (plus 95's cutover links); databases 2 and 95
// are applied whole here because their cursors advance. The fail-closed rule for
// message requests applies to every row. The returned frames are for the initial
// emitter.
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
		// The library applies each database's cursors and thread ranges right after its
		// request (SyncDataGraphQL); the snapshot's are fresher than the cached ones.
		// Outside s.mu, before connect.
		s.postHandleTable(tbl)
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
