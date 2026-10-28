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
	more          bool // the keystore reports more threads
	initialReady  bool // the initial range wait returns at once
	group1        *table.LSTable
	explicit      *table.LSTable // the reply to the explicit group-95 fetch
	explicitErr   error
	explicitBlock bool // the explicit fetch honours only its context
	rangeInReply  bool // the explicit reply carries the range update
	tasks         []socket.Task
	gen           uint64
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

func (r *silentRanges) FetchMoreThreads(_ context.Context, group int64) (*socket.KeyStoreData, *table.LSTable, error) {
	tbl := &table.LSTable{}
	if group == 1 && r.group1 != nil {
		tbl = r.group1
	}
	return &socket.KeyStoreData{HasMoreBefore: r.more, MinThreadKey: 7}, tbl, nil
}

func listWithin(t *testing.T, s *messagixSession, r threadRanges, ctx context.Context, bound time.Duration) (frames []Frame, res listingDone, err error, done bool) {
	t.Helper()
	type result struct {
		res listingDone
		err error
	}
	ch := make(chan result, 1)
	go func() {
		d, err := s.listThreads(ctx, r, func(f Frame) int64 { frames = append(frames, f); return 0 })
		ch <- result{d, err}
	}()
	select {
	case v := <-ch:
		return frames, v.res, v.err, true
	case <-time.After(bound):
		return nil, listingDone{}, nil, false
	}
}

func shortWait(t *testing.T, d time.Duration) {
	old := e2eeRangeWait
	e2eeRangeWait = d
	t.Cleanup(func() { e2eeRangeWait = old })
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

func wantStats(t *testing.T, got map[string]int, want map[string]int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("stats keys = %v, want %v", got, want)
	}
	for k, v := range want {
		if g, ok := got[k]; !ok || g != v {
			t.Fatalf("stats[%s] = %d (present %v), want %d; all = %v", k, g, ok, v, got)
		}
	}
}

// Group 95 is the message-requests inbox. An initial range that never arrives
// means there are no requests: the listing is complete. A later batch that never
// arrives, after the range said more exist, leaves known requests unread: a gap.
func TestListingGroup95Timeouts(t *testing.T) {
	shortWait(t, 100*time.Millisecond)
	for name, tc := range map[string]struct {
		r       *silentRanges
		wantGap string
	}{
		"initial range absent": {&silentRanges{explicit: &table.LSTable{}}, ""},
		"batch wait":           {&silentRanges{more: true, initialReady: true}, "requests_incomplete"},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			s.threads["100"] = &threadInfo{Key: 100, Kind: "user", Typed: true, Title: "Known", LastActivity: 5}
			frames, res, err, done := listWithin(t, s, tc.r, context.Background(), 3*time.Second)
			if !done {
				t.Fatal("thread listing blocked on a group-95 range update that never arrives")
			}
			if err != nil || res.Stats == nil || res.Gap != tc.wantGap {
				t.Fatalf("want stats and gap %q, got %+v err=%v", tc.wantGap, res, err)
			}
			if len(frames) == 0 || frames[0].T != TThread || frames[0].ThreadEvt.ID != "100" {
				t.Fatalf("known thread not emitted first: %+v", frames)
			}
		})
	}
}

func TestListingCancelStillWinsOverTheBound(t *testing.T) {
	shortWait(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(100 * time.Millisecond); cancel() }()
	_, res, err, done := listWithin(t, newTestSession(), &silentRanges{}, ctx, 3*time.Second)
	if !done {
		t.Fatal("cancel did not free the listing")
	}
	if err == nil || res.Stats != nil || res.Gap != "" {
		t.Fatalf("a cancelled listing must report the cancel, not stats: %+v err=%v", res, err)
	}
}

// threads_done carries the stats object and no gap key.
func TestThreadsDoneCarriesStats(t *testing.T) {
	const startAndList = `{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`
	stats := map[string]int{"cutover_links": 2, "encrypted_emitted": 1}
	frames := runLoop(t, &fakeBackend{threadStats: stats}, startAndList, `{"t":"threads","id":2}`)
	var done *Frame
	for i, f := range frames {
		if f.ID == 2 {
			done = &frames[i]
		}
	}
	if done == nil || done.T != TThreadsDone || done.Err != "" {
		t.Fatalf("threads answer = %+v", done)
	}
	wantStats(t, done.Stats, stats)
	b, _ := json.Marshal(Frame{T: TThreadsDone, ID: 2, Stats: map[string]int{"a": 1}})
	if string(b) != `{"t":"threads_done","id":2,"stats":{"a":1}}` {
		t.Fatalf("wire = %s", b)
	}
	b, _ = json.Marshal(Frame{T: TThreadsDone, ID: 2, Gap: "requests_incomplete"})
	if string(b) != `{"t":"threads_done","id":2,"gap":"requests_incomplete"}` {
		t.Fatalf("wire = %s", b)
	}
	frames = runLoop(t, &fakeBackend{threadGap: "requests_incomplete"}, startAndList, `{"t":"threads","id":2}`)
	done = nil
	for i, f := range frames {
		if f.ID == 2 {
			done = &frames[i]
		}
	}
	if done == nil || done.T != TThreadsDone || done.Gap != "requests_incomplete" || done.Err != "" {
		t.Fatalf("gap answer = %+v", done)
	}
	b, _ = json.Marshal(Frame{T: TThreadsDone, ID: 2})
	if string(b) != `{"t":"threads_done","id":2}` {
		t.Fatalf("wire = %s", b)
	}
}

// A group-1 row of an encrypted-over-WA type becomes an e: thread of the right kind.
func TestEncryptedGroup1RowsEmitAsEncryptedThreads(t *testing.T) {
	shortWait(t, 50*time.Millisecond)
	r := &silentRanges{group1: &table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_GROUP, ThreadName: "Group", LastActivityTimestampMs: 20},
		},
		LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{
			{ThreadKey: 5151, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, LastActivityTimestampMs: 10},
		},
		LSVerifyThreadExists: []*table.LSVerifyThreadExists{
			{ThreadKey: 6161, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE},
			{ThreadKey: 7171, ThreadType: table.ONE_TO_ONE}, // a plain existence row names nothing
		},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 4242, ContactId: 42}},
	}}
	frames, res, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	got := threadsByID(frames)
	if g := got["e:4242"]; g == nil || g.Kind != "group" || !g.Encrypted || g.Title != "Group" {
		t.Fatalf("encrypted group = %+v", g)
	}
	for _, id := range []string{"e:5151", "e:6161"} {
		if u := got[id]; u == nil || u.Kind != "user" || !u.Encrypted {
			t.Fatalf("%s = %+v", id, u)
		}
	}
	if got["7171"] != nil || got["e:7171"] != nil {
		t.Fatal("a plain existence row must not create a thread")
	}
	wantStats(t, res.Stats, map[string]int{
		"group1_threads": 3, "group1_encrypted_rows": 3, "hybrid_mappings": 0, "cutover_links": 0,
		"encrypted_known": 3, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_unlisted": 0,
	})
}

// A cutover link or hybrid mapping whose chat no table named is counted in
// encrypted_unlisted and not emitted: nothing is synthesized from the links.
func TestKnownEncryptedKeyWithNoTableRowIsCountedNotEmitted(t *testing.T) {
	shortWait(t, 50*time.Millisecond)
	s := newTestSession()
	s.threads["100"] = &threadInfo{Key: 100, Kind: "user", Typed: true, Participants: []int64{1, 100}}
	s.cutover[1100] = 100                                                                                           // no row for e:1100
	s.fbToWA[400] = 1400                                                                                            // no row for e:1400
	s.threads["e:1600"] = &threadInfo{Key: 1600, Enc: true, Kind: "user", Typed: true, Participants: []int64{1600}} // listed
	s.cutover[1600] = 600

	frames, res, err, done := listWithin(t, s, &silentRanges{initialReady: true}, context.Background(), 3*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	got := threadsByID(frames)
	if got["e:1100"] != nil || got["e:1400"] != nil {
		t.Fatalf("a link alone must not create a thread: %v", got)
	}
	if got["e:1600"] == nil {
		t.Fatal("the already-known encrypted thread must still be listed")
	}
	wantStats(t, res.Stats, map[string]int{
		"group1_threads": 0, "group1_encrypted_rows": 0, "hybrid_mappings": 1, "cutover_links": 2,
		"encrypted_known": 3, "encrypted_emitted": 1, "e2ee_connected": 0, "encrypted_unlisted": 2,
	})
}

// State saved by the build that kept a hybrid kind proof still loads; the field is ignored.
func TestStateWithHybridKindStillLoads(t *testing.T) {
	var st savedState
	blob := `{"cutover":{"1100":100},"fb_to_wa":{"400":1400},"hybrid_kind":{"1400":"user"}}`
	if err := json.Unmarshal([]byte(blob), &st); err != nil {
		t.Fatal(err)
	}
	s := newTestSession()
	s.restore(&st)
	if s.fbToWA[400] != 1400 || s.cutover[1100] != 100 {
		t.Fatalf("restored fbToWA=%v cutover=%v", s.fbToWA, s.cutover)
	}
}

func requestTable() *table.LSTable {
	return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 8181, ThreadType: table.ONE_TO_ONE, LastActivityTimestampMs: 30},
	}}
}

// A resumed session never sends the initial group-95 fetch, so when the initial
// wait expires the helper asks once itself.
func TestExplicitRequestsFetchWhenTheInitialWaitExpires(t *testing.T) {
	shortWait(t, 100*time.Millisecond)

	// One fetch with the library's initial-sync parameters; a request thread is emitted;
	// the range says no more, so the listing is complete.
	r := &silentRanges{explicit: requestTable(), rangeInReply: true}
	frames, res, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil || res.Gap != "" {
		t.Fatalf("done=%v err=%v res=%+v", done, err, res)
	}
	if u := threadsByID(frames)["8181"]; u == nil || u.Kind != "user" {
		t.Fatalf("request thread = %+v", threadsByID(frames))
	}
	if len(r.tasks) != 1 {
		t.Fatalf("want exactly one explicit fetch, got %d", len(r.tasks))
	}
	task, ok := r.tasks[0].(*socket.FetchThreadsTask)
	if !ok || *task != (socket.FetchThreadsTask{ParentThreadKey: -1, ReferenceActivityTimestamp: 9999999999999, SyncGroup: 95}) {
		t.Fatalf("explicit task = %+v", r.tasks[0])
	}

	// A range that says more exist and a later batch that never arrives: known requests are unread.
	r = &silentRanges{explicit: requestTable(), rangeInReply: true, more: true}
	_, res, err, done = listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil || res.Gap != "requests_incomplete" || len(r.tasks) != 1 {
		t.Fatalf("done=%v err=%v res=%+v fetches=%d", done, err, res, len(r.tasks))
	}

	// The initial range arrived on its own: no explicit fetch.
	r = &silentRanges{initialReady: true}
	if _, _, _, done = listWithin(t, newTestSession(), r, context.Background(), 3*time.Second); !done || len(r.tasks) != 0 {
		t.Fatalf("done=%v fetches=%d, want none", done, len(r.tasks))
	}
}

func TestExplicitRequestsFetchOutcomes(t *testing.T) {
	shortWait(t, 100*time.Millisecond)
	for name, tc := range map[string]struct {
		r       *silentRanges
		wantGap string
	}{
		"error":             {&silentRanges{explicitErr: errors.New("boom")}, "requests_incomplete"},
		"timeout":           {&silentRanges{explicitBlock: true}, "requests_incomplete"},
		"empty reply":       {&silentRanges{explicit: &table.LSTable{}}, ""},
		"range, no rows":    {&silentRanges{explicit: &table.LSTable{}, rangeInReply: true}, ""},
		"threads, no range": {&silentRanges{explicit: requestTable()}, ""},
	} {
		t.Run(name, func(t *testing.T) {
			_, res, err, done := listWithin(t, newTestSession(), tc.r, context.Background(), 3*time.Second)
			if !done || err != nil || res.Gap != tc.wantGap || len(tc.r.tasks) != 1 {
				t.Fatalf("done=%v err=%v res=%+v fetches=%d", done, err, res, len(tc.r.tasks))
			}
		})
	}
}

func TestExplicitRequestsFetchHonoursCancel(t *testing.T) {
	shortWait(t, 300*time.Millisecond)
	// The initial wait expires at 300 ms; the cancel lands inside the blocked explicit fetch.
	r := &silentRanges{explicitBlock: true}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(450 * time.Millisecond); cancel() }()
	_, res, err, done := listWithin(t, newTestSession(), r, ctx, 3*time.Second)
	if !done || err == nil || res.Gap != "" || len(r.tasks) != 1 {
		t.Fatalf("a cancelled listing must report the cancel after one fetch: done=%v err=%v res=%+v fetches=%d", done, err, res, len(r.tasks))
	}
}
