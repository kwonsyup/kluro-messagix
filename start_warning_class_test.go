// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"slices"
	"strings"
	"syscall"
	"testing"

	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// secretText stands for provider content an error can carry: it must never reach the log.
const secretText = "private-detail-7f3a"

// The two start warnings carry a fixed error class, built the way the pinned library wraps its errors, and never the
// error's text.
func TestRequestErrorClass(t *testing.T) {
	var jsonErr error
	{
		var v struct{}
		jsonErr = json.Unmarshal([]byte("{"+secretText), &v)
	}
	for want, err := range map[string]error{
		"no_table":          nil,
		"http_4xx":          fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 403),
		"http_5xx":          fmt.Errorf("%w: %w", httpclient.ErrMaxRetriesReached, fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 502)),
		"http_429":          httpclient.ErrRateLimited,
		"please_log_in":     fmt.Errorf("graphql error %w", &types.ErrorResponse{ErrorCode: 1357001, ErrorDescription: secretText}),
		"reload_page":       fmt.Errorf("graphql error %w", &types.ErrorResponse{ErrorCode: 1357004, ErrorDescription: secretText}),
		"ls_error":          fmt.Errorf("graphql error %w", &types.ErrorResponse{ErrorCode: 42, ErrorDescription: secretText}),
		"graphql_error":     fmt.Errorf("q failed: %w", (&types.ErrorResponse{Errors: []*types.GraphQLError{{Message: secretText}}}).AsError()),
		"token_invalid":     httpclient.ErrTokenInvalidatedRedirect,
		"checkpoint":        httpclient.RedirectedError{Type: httpclient.ErrCheckpointRequired, URL: "https://example.invalid/" + secretText},
		"transport_dns":     fmt.Errorf("%w: %w", httpclient.ErrRequestFailed, &net.DNSError{Err: secretText, Name: "example.invalid"}),
		"transport_refused": fmt.Errorf("%w: %w", httpclient.ErrRequestFailed, syscall.ECONNREFUSED),
		"read":              fmt.Errorf("%w: %w", httpclient.ErrResponseReadFailed, errors.New(secretText)),
		"decode":            fmt.Errorf("invalid response: %w", jsonErr),
		"timeout":           fmt.Errorf("q failed: %w", context.DeadlineExceeded),
		"cancelled":         context.Canceled,
		"other":             errors.New(secretText),
		"http_status":       fmt.Errorf("%w", httpclient.ErrUnexpectedError),
	} {
		if got := requestErrorClass(err); got != want {
			t.Errorf("requestErrorClass(%v) = %q, want %q", err, got, want)
		}
	}
}

// warnCodes is the codes of the warn lines logged to buf, in order.
func warnCodes(t *testing.T, buf *bytes.Buffer) []string {
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
		if obj.Level == "warn" {
			out = append(out, obj.Code)
		}
	}
	return out
}

func captureCodes(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	old := stderr
	stderr = &buf
	t.Cleanup(func() { stderr = old })
	return &buf
}

// A failed resume sync logs resume_sync_failed with the request's error class; the text stays out of the log.
func TestResumeSyncFailureLogsItsClass(t *testing.T) {
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"server status": {fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 401), "resume_sync_failed.http_4xx"},
		"reload page":   {fmt.Errorf("graphql error %w", &types.ErrorResponse{ErrorCode: 1357004, ErrorDescription: secretText}), "resume_sync_failed.reload_page"},
		"no table":      {nil, "resume_sync_failed.no_table"},
	} {
		t.Run(name, func(t *testing.T) {
			buf := captureCodes(t)
			s := newTestSession()
			s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
				return nil, tc.err
			}
			s.resumeSync(context.Background(), savedConfig(t, 123))
			if codes := warnCodes(t, buf); !slices.Equal(codes, []string{tc.want}) || strings.Contains(buf.String(), secretText) {
				t.Fatalf("log = %q, want code %q and no error text", buf.String(), tc.want)
			}
		})
	}
}

// A failed secure-history check logs history_check_failed with the request's error class; "no backup" is an answer,
// not a failure, and logs nothing.
func TestHistoryCheckFailureLogsItsClass(t *testing.T) {
	buf := captureCodes(t)
	if !historyCheckFailed(fmt.Errorf("useMWEncryptedBackupsFetchBackupIdsV2Query failed: %w", fmt.Errorf("%w %d", httpclient.ErrUnexpectedError, 500))) {
		t.Fatal("a server error is a failed check")
	}
	if !historyCheckFailed(fmt.Errorf("q failed: %w", (&types.ErrorResponse{Errors: []*types.GraphQLError{{Message: secretText}}}).AsError())) {
		t.Fatal("a GraphQL error is a failed check")
	}
	if historyCheckFailed(securehistory.ErrNoPINBackup) || historyCheckFailed(nil) {
		t.Fatal("no backup, or no error, is not a failed check")
	}
	want := []string{"history_check_failed.http_5xx", "history_check_failed.graphql_error"}
	if codes := warnCodes(t, buf); !slices.Equal(codes, want) || strings.Contains(buf.String(), secretText) {
		t.Fatalf("log = %q, want codes %v and no error text", buf.String(), want)
	}
}
