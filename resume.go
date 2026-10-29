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
// state, which skipped the page load. Database 1's table is the result and database
// 95's cutover links are added to it, as the library does. The version and the sync
// params come from the saved state's own copy of the page config, so nothing needs
// the page. One attempt, bounded by resumeSyncTimeout, cancel honoured: a failure
// logs a fixed code and returns whatever database 1 gave (nothing if it failed).
// The library's own bookkeeping (cursors, thread ranges) is not touched: its sync
// manager is not reachable from here, and the socket sync that follows does it.
func (s *messagixSession) resumeSync(ctx context.Context, saved json.RawMessage) *table.LSTable {
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
	var out *table.LSTable
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
		switch db {
		case 1:
			out = tbl
		case 95:
			out.LSCreateOpenToE2EEThreadLink = append(out.LSCreateOpenToE2EEThreadLink, tbl.LSCreateOpenToE2EEThreadLink...)
		}
	}
	s.mu.Lock()
	s.resumeRows = countRows(out)
	s.mu.Unlock()
	return out
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
