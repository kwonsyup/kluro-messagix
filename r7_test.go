// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

func encRow(key int64, folder string) *table.LSTable {
	return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: key, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, FolderName: folder, MemberCount: 2},
	}}
}

// Sol finding 1: an encrypted thread carrying a request or spam mark is inert, and History for it issues
// no task on any path (here: the archive path, with an enrollment and a client present).
func TestEncryptedRequestThreadIsInertAndHistoryRefused(t *testing.T) {
	for _, folder := range []string{folderPending, folderSpam} {
		t.Run(folder, func(t *testing.T) {
			s := newTestSession()
			frames := s.handleTableFrames(encRow(4242, folder))
			th := lastThread(frames, "e:4242")
			if th == nil || th.Typed || th.Kind != "" || !th.Request || len(th.Participants) != 0 || connectorWouldQualify(th) {
				t.Fatalf("marked encrypted thread must be inert: %+v", th)
			}
			s.client = &messagix.Client{}
			s.histDev = &securehistory.Device{ID: "dev"}
			rec := &recordingPager{}
			s.historyPager = rec
			for _, cursor := range []string{"", "5"} {
				if _, err := s.History(context.Background(), "e:4242", cursor, 20); codeOf(err) != "unavailable" {
					t.Fatalf("cursor %q: History = %v, want unavailable", cursor, err)
				}
			}
			if len(rec.all) != 0 {
				t.Fatalf("tasks sent: %v", rec.all)
			}
			// a later non-request folder row clears the mark and types the thread again
			frames = s.handleTableFrames(encRow(4242, "inbox"))
			if th := lastThread(frames, "e:4242"); th == nil || !th.Typed {
				t.Fatalf("cleared mark must re-emit the thread typed: %+v", th)
			}
		})
	}
}

// A typed encrypted thread that later gets a spam mark is re-emitted inert.
func TestEncryptedThreadMovedToSpamIsReemittedInert(t *testing.T) {
	s := newTestSession()
	if th := lastThread(s.handleTableFrames(encRow(4242, "inbox")), "e:4242"); th == nil || !th.Typed {
		t.Fatalf("premise: %+v", th)
	}
	if th := lastThread(s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, FolderName: folderSpam}}}), "e:4242"); th == nil || th.Typed {
		t.Fatalf("spam move must re-emit inert: %+v", th)
	}
}

// failRequests answers every sync-group-95 request with an error.
type failRequests struct{ recordingPager }

func (f *failRequests) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if ft, ok := tasks[0].(*socket.FetchThreadsTask); ok && ft.SyncGroup == requestSyncGroup {
		return nil, errors.New("scope failed")
	}
	return f.recordingPager.ExecuteTasks(ctx, tasks...)
}

func historyTasks(t *testing.T, s *messagixSession, rec *recordingPager, thread string) (string, int) {
	t.Helper()
	before := len(rec.all)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	_, err := s.History(ctx, thread, "", 20)
	return codeOf(err), len(rec.all) - before
}

// Sol finding 2: a fresh session refuses a thread it has not observed until the first scope scan completes with
// every request scope answered; a failed scope keeps the refusal; a thread seen in the walk is readable at once.
func TestFreshSessionRefusesUnseenThreadsUntilCleanScan(t *testing.T) {
	// before any scan
	s := newTestSession()
	rec := &recordingPager{}
	s.historyPager = rec
	if code, n := historyTasks(t, s, rec, "3003"); code != "unavailable" || n != 0 {
		t.Fatalf("unseen before scan: %q tasks=%d", code, n)
	}
	// an inbox thread seen in the walk is readable immediately
	s.handleTableFrames(threadRow(3010, "inbox"))
	if _, n := historyTasks(t, s, rec, "3010"); n == 0 {
		t.Fatal("a thread seen in this session without a mark must be readable at once")
	}
	// a failed request scope keeps the refusal
	failing := &failRequests{}
	if _, res, err, done := listGap(t, s, failing, context.Background(), 5*time.Second); !done || err != nil || res.Gap != gapRequestsIncomplete {
		t.Fatalf("failing scan: done=%v err=%v gap=%q", done, err, res.Gap)
	}
	if code, n := historyTasks(t, s, rec, "3003"); code != "unavailable" || n != 0 {
		t.Fatalf("unseen after a failed scan: %q tasks=%d", code, n)
	}
	// a clean scan opens unseen threads
	if _, res, err, done := listGap(t, s, &pager{}, context.Background(), 5*time.Second); !done || err != nil || res.Gap != "" {
		t.Fatalf("clean scan: done=%v err=%v gap=%q", done, err, res.Gap)
	}
	if _, n := historyTasks(t, s, rec, "3003"); n == 0 {
		t.Fatal("after a clean scan an unseen thread must be readable")
	}
}

// The gate is re-evaluated before every dispatch: with no scan done, no 209 goes out even once the call is waiting.
func TestUnseenGateAppliesToEveryDispatch(t *testing.T) {
	s := newTestSession()
	h := &hookPager{}
	s.historyPager = h
	s.mu.Lock()
	s.rowKeys[3003] = true // observed: first dispatch passes
	s.mu.Unlock()
	h.onTask = func(n int) {
		if n == 1 {
			s.mu.Lock()
			delete(s.rowKeys, 3003) // evidence lost mid-call (models a gate that closes again)
			s.mu.Unlock()
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), historyReRequest+3*time.Second)
	defer cancel()
	if _, err := s.History(ctx, "3003", "", 20); codeOf(err) != "unavailable" || len(h.all) != 1 {
		t.Fatalf("err=%v tasks=%d, want unavailable after exactly one task", err, len(h.all))
	}
}

// Sol finding 3: a pending Marketplace thread moved to spam keeps no message, edit or unsend events.
func TestSpamMovedMarketplaceThreadWithholdsLiveEvents(t *testing.T) {
	s := newTestSession()
	msg := func(id string) *table.LSTable {
		return &table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: 3005, MessageId: id, SenderId: 3006, Text: "hi", TimestampMs: 5}}}
	}
	has := func(frames []Frame, typ string) bool {
		for _, f := range frames {
			if f.T == typ {
				return true
			}
		}
		return false
	}
	s.handleTableFrames(marketThread(3005, -1, folderPending))
	if !has(s.handleTableFrames(msg("mid.1")), TMessage) {
		t.Fatal("premise: a pending Marketplace thread emits its messages")
	}
	frames := s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.MARKETPLACE, FolderName: folderSpam}}})
	if th := lastThread(frames, "3005"); th == nil || th.Typed {
		t.Fatalf("premise: spam move emits inert: %+v", th)
	}
	if f := s.handleTableFrames(msg("mid.2")); has(f, TMessage) {
		t.Fatalf("message emitted for a spam thread: %+v", f)
	}
	if f := s.handleTableFrames(&table.LSTable{LSEditMessage: []*table.LSEditMessage{{MessageID: "mid.1", Text: "x"}}}); has(f, TEdit) {
		t.Fatal("edit emitted for a spam thread")
	}
	if f := s.handleTableFrames(&table.LSTable{LSDeleteMessage: []*table.LSDeleteMessage{{ThreadKey: 3005, MessageId: "mid.1"}}}); has(f, TUnsend) {
		t.Fatal("unsend emitted for a spam thread")
	}
	// moved back to the inbox: events flow again
	s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 3005, ThreadType: table.MARKETPLACE, FolderName: "inbox"}}})
	if !has(s.handleTableFrames(msg("mid.3")), TMessage) {
		t.Fatal("an accepted thread must emit its messages again")
	}
}
