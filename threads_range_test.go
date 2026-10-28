// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// silentRanges is a client for which no group-95 range update ever arrives:
// the waits honour only their context, as the library's do.
type silentRanges struct {
	explicit      *table.LSTable // the reply to the explicit group-95 fetch
	explicitErr   error
	explicitBlock bool // the explicit fetch honours only its context
	rangeInReply  bool // the explicit reply carries the range update
	tasks         []socket.Task
	gen           uint64
	more          bool // the keystore reports more encrypted threads
	initialReady  bool // the initial range wait returns at once
}

func (r *silentRanges) WaitForInitialThreadRange(ctx context.Context, _ int64) error {
	if r.initialReady {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (r *silentRanges) WaitForThreadRange(ctx context.Context, _ int64, _ uint64) error {
	<-ctx.Done()
	return ctx.Err()
}

func (r *silentRanges) ThreadRangeGeneration(int64) uint64 { return r.gen }

func (r *silentRanges) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	r.tasks = append(r.tasks, tasks...)
	if r.explicitBlock {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return r.explicit, r.explicitErr
}

func (r *silentRanges) PostHandlePublishResponse(*table.LSTable) {
	if r.rangeInReply {
		r.gen++
	}
}

func (r *silentRanges) GetSyncGroupKeyStore(int64) *socket.KeyStoreData {
	return &socket.KeyStoreData{HasMoreBefore: r.more}
}

func (r *silentRanges) FetchMoreThreads(context.Context, int64) (*socket.KeyStoreData, *table.LSTable, error) {
	return &socket.KeyStoreData{HasMoreBefore: r.more, MinThreadKey: 7}, &table.LSTable{}, nil
}

func listWithin(t *testing.T, s *messagixSession, r threadRanges, ctx context.Context, bound time.Duration) (frames []Frame, err error, done bool) {
	t.Helper()
	res := make(chan error, 1)
	go func() {
		res <- s.listThreads(ctx, r, func(f Frame) int64 { frames = append(frames, f); return 0 })
	}()
	select {
	case err = <-res:
		return frames, err, true
	case <-time.After(bound):
		return nil, nil, false
	}
}

func TestListingFinishesWhenNoEncryptedRangeUpdateArrives(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = 100 * time.Millisecond
	defer func() { e2eeRangeWait = old }()

	for name, r := range map[string]*silentRanges{
		"initial wait": {},
		"batch wait":   {more: true, initialReady: true},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			_, err, done := listWithin(t, s, r, context.Background(), 3*time.Second)
			if !done {
				t.Fatal("thread listing blocked on a group-95 range update that never arrives")
			}
			var gap *listingGap
			if !errors.As(err, &gap) || gap.code != "e2ee_range_timeout" {
				t.Fatalf("listing must end with the e2ee_range_timeout gap, got %v", err)
			}
		})
	}
}

func TestListingKeepsKnownThreadsWhenEncryptedRangeTimesOut(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = 100 * time.Millisecond
	defer func() { e2eeRangeWait = old }()

	s := newTestSession()
	s.threads["100"] = &threadInfo{Key: 100, Kind: "user", Title: "Known", LastActivity: 5}
	frames, err, done := listWithin(t, s, &silentRanges{more: true}, context.Background(), 3*time.Second)
	var gap *listingGap
	if !done || !errors.As(err, &gap) {
		t.Fatalf("listing done=%v err=%v", done, err)
	}
	if len(frames) == 0 || frames[0].T != TThread {
		t.Fatalf("known thread not emitted first: %+v", frames)
	}
}

func TestListingCancelStillWinsOverTheBound(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = time.Minute
	defer func() { e2eeRangeWait = old }()

	s := newTestSession()
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, err, done := listWithin(t, s, &silentRanges{}, ctx, 3*time.Second)
	if !done {
		t.Fatal("cancel did not free the listing")
	}
	if err == nil {
		t.Fatal("a cancelled listing must report the cancel, not a clean end")
	}
}

// The loop answers a gap listing with threads_done carrying gap, and a normal
// listing with a threads_done that has none.
func TestThreadsDoneCarriesTheGapOnlyWhenTheListingEndedEarly(t *testing.T) {
	const startAndList = `{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`
	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"gap":    {&listingGap{gapE2EERangeTimeout}, "e2ee_range_timeout"},
		"normal": {nil, ""},
	} {
		t.Run(name, func(t *testing.T) {
			frames := runLoop(t, &fakeBackend{threadsErr: tc.err}, startAndList, `{"t":"threads","id":2}`)
			var done *Frame
			for i, f := range frames {
				if f.ID == 2 {
					done = &frames[i]
				}
			}
			if done == nil || done.T != TThreadsDone || done.Gap != tc.want || done.Err != "" {
				t.Fatalf("threads answer = %+v", done)
			}
		})
	}
	// the wire key is "gap"
	b, _ := json.Marshal(Frame{T: TThreadsDone, ID: 2, Gap: "e2ee_range_timeout"})
	if string(b) != `{"t":"threads_done","id":2,"gap":"e2ee_range_timeout"}` {
		t.Fatalf("wire = %s", b)
	}
	b, _ = json.Marshal(Frame{T: TThreadsDone, ID: 2})
	if string(b) != `{"t":"threads_done","id":2}` {
		t.Fatalf("wire = %s", b)
	}
}

func TestListingWithRangeUpdatesPresentHasNoGap(t *testing.T) {
	s := newTestSession()
	_, err, done := listWithin(t, s, &silentRanges{initialReady: true}, context.Background(), 3*time.Second)
	if !done || err != nil {
		t.Fatalf("listing done=%v err=%v", done, err)
	}
}

func encryptedTable() *table.LSTable {
	return &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, ThreadName: "Group", LastActivityTimestampMs: 20},
			{ThreadKey: 5151, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, LastActivityTimestampMs: 10},
		},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 4242, ContactId: 42}},
	}
}

func threadsByID(frames []Frame) map[string]*Thread {
	out := map[string]*Thread{}
	for _, f := range frames {
		if f.T == TThread {
			out[f.ThreadEvt.ID] = f.ThreadEvt
		}
	}
	return out
}

func checkEncryptedPair(t *testing.T, frames []Frame) {
	t.Helper()
	got := threadsByID(frames)
	if g := got["e:4242"]; g == nil || g.Kind != "group" || !g.Encrypted || g.Title != "Group" {
		t.Fatalf("encrypted group = %+v", g)
	}
	if u := got["e:5151"]; u == nil || u.Kind != "user" || !u.Encrypted {
		t.Fatalf("encrypted 1:1 = %+v", u)
	}
}

func TestExplicitE2EEFetchNamesTheThreadsWhenTheRangeNeverArrives(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = 100 * time.Millisecond
	defer func() { e2eeRangeWait = old }()

	// The reply carries the range: the encrypted listing is complete, no gap.
	r := &silentRanges{explicit: encryptedTable(), rangeInReply: true}
	frames, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil {
		t.Fatalf("listing done=%v err=%v", done, err)
	}
	checkEncryptedPair(t, frames)
	if len(r.tasks) != 1 {
		t.Fatalf("want exactly one explicit fetch, got %d", len(r.tasks))
	}
	task, ok := r.tasks[0].(*socket.FetchThreadsTask)
	if !ok || *task != (socket.FetchThreadsTask{ParentThreadKey: -1, ReferenceActivityTimestamp: 9999999999999, SyncGroup: 95}) {
		t.Fatalf("explicit task = %+v", r.tasks[0])
	}

	// No range in the reply: the threads are still named, but the listing may be incomplete.
	r = &silentRanges{explicit: encryptedTable()}
	frames, err, done = listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	var gap *listingGap
	if !done || !errors.As(err, &gap) || gap.code != "e2ee_range_timeout" {
		t.Fatalf("listing done=%v err=%v", done, err)
	}
	checkEncryptedPair(t, frames)
}

func TestExplicitE2EEFetchFailureKeepsTheGap(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = 100 * time.Millisecond
	defer func() { e2eeRangeWait = old }()

	for name, r := range map[string]*silentRanges{
		"error":   {explicitErr: errors.New("boom")},
		"nothing": {},
		"timeout": {explicitBlock: true},
		"empty":   {explicit: &table.LSTable{}, rangeInReply: false},
	} {
		t.Run(name, func(t *testing.T) {
			frames, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
			var gap *listingGap
			if !done || !errors.As(err, &gap) || gap.code != "e2ee_range_timeout" {
				t.Fatalf("listing done=%v err=%v", done, err)
			}
			if len(threadsByID(frames)) != 0 || len(r.tasks) != 1 {
				t.Fatalf("threads=%d fetches=%d", len(threadsByID(frames)), len(r.tasks))
			}
		})
	}
}

func TestExplicitE2EEFetchHonoursCancel(t *testing.T) {
	old := e2eeRangeWait
	e2eeRangeWait = 300 * time.Millisecond
	defer func() { e2eeRangeWait = old }()

	// The initial wait expires at 300 ms; the cancel lands inside the blocked explicit fetch.
	r := &silentRanges{explicitBlock: true}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(450 * time.Millisecond); cancel() }()
	_, err, done := listWithin(t, newTestSession(), r, ctx, 3*time.Second)
	var gap *listingGap
	if !done || err == nil || errors.As(err, &gap) || len(r.tasks) != 1 {
		t.Fatalf("a cancelled listing must report the cancel after one fetch, got done=%v err=%v fetches=%d", done, err, len(r.tasks))
	}
}
