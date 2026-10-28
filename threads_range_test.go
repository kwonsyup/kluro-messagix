// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// silentRanges is a client for which no group-95 range update ever arrives:
// the waits honour only their context, as the library's do.
type silentRanges struct {
	more         bool // the keystore reports more threads
	initialReady bool // the initial range wait returns at once
	group1       *table.LSTable
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

func (r *silentRanges) ThreadRangeGeneration(int64) uint64 { return 0 }

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
		"initial range absent": {&silentRanges{}, ""},
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
		"encrypted_known": 3, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_skipped_group": 0,
	})
}

// A cutover or hybrid link whose chat no table named is listed when it is
// provably one-to-one, and skipped and counted when it is not.
func TestUnlistedEncryptedChatsAreListedOnlyWhenProvablyOneToOne(t *testing.T) {
	shortWait(t, 50*time.Millisecond)
	s := newTestSession()
	// 1: cutover to a known one-to-one open thread -> listed with that thread as its predecessor.
	s.threads["100"] = &threadInfo{Key: 100, Kind: "user", Typed: true, LastActivity: 9, Participants: []int64{1, 100}}
	s.cutover[1100] = 100
	// 2: cutover to a known group open thread -> skipped.
	s.threads["200"] = &threadInfo{Key: 200, Kind: "group", Typed: true, Participants: []int64{1, 2, 3}}
	s.cutover[1200] = 200
	// 3: cutover to an open thread the helper never saw -> skipped.
	s.cutover[1300] = 300
	// 4: hybrid mapping whose row proves a one-to-one -> listed, the peer is the facebook thread key.
	s.fbToWA[400] = 1400
	s.hybridKind[1400] = "user"
	// 5: hybrid mapping whose row proves a group -> skipped.
	s.fbToWA[500] = 1500
	s.hybridKind[1500] = "group"
	// 6: already a thread record -> not synthesized again.
	s.threads["e:1600"] = &threadInfo{Key: 1600, Enc: true, Kind: "user", Typed: true, Participants: []int64{1600}}
	s.cutover[1600] = 600

	frames, res, err, done := listWithin(t, s, &silentRanges{initialReady: true}, context.Background(), 3*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	count := map[string]int{}
	for _, f := range frames {
		if f.T == TThread {
			count[f.ThreadEvt.ID]++
		}
	}
	for _, id := range []string{"e:1100", "e:1400", "e:1600"} {
		if count[id] != 1 {
			t.Fatalf("%s emitted %d times, want once: %v", id, count[id], count)
		}
	}
	for _, id := range []string{"e:1200", "e:1300", "e:1500"} {
		if count[id] != 0 {
			t.Fatalf("%s must be skipped: %v", id, count)
		}
	}
	got := threadsByID(frames)
	a := got["e:1100"]
	if a.Kind != "user" || !a.Encrypted || a.Predecessor != "100" || len(a.Participants) != 2 || a.Participants[1].ID != "100" || a.LastActivity != 9 {
		t.Fatalf("cutover thread = %+v", a)
	}
	b := got["e:1400"]
	if b.Kind != "user" || b.Predecessor != "400" || len(b.Participants) != 2 || b.Participants[1].ID != "400" {
		t.Fatalf("hybrid thread = %+v", b)
	}
	// every synthesized frame is before the listing ends: listThreads returned it
	wantStats(t, res.Stats, map[string]int{
		"group1_threads": 0, "group1_encrypted_rows": 0, "hybrid_mappings": 2, "cutover_links": 4,
		"encrypted_known": 6, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_skipped_group": 3,
	})
}

// The hybrid kind proof is saved next to the hybrid mapping, so a restarted
// helper still lists a hybrid one-to-one and still skips a hybrid group; state
// saved before the proof was kept loads and leaves those chats skipped.
func TestHybridKindSurvivesARestart(t *testing.T) {
	shortWait(t, 50*time.Millisecond)
	s := newTestSession()
	s.handleTableFrames(&table.LSTable{LSVerifyHybridThreadExists: []*table.LSVerifyHybridThreadExists{
		{ThreadKey: 400, ThreadJID: 1400, ThreadType: table.ONE_TO_ONE},
		{ThreadKey: 500, ThreadJID: 1500, ThreadType: table.GROUP_THREAD, IsGroupThread: true},
	}})
	saved := s.dump()

	list := func(st savedState) (map[string]*Thread, map[string]int) {
		fresh := newTestSession()
		fresh.restore(&st)
		frames, res, err, done := listWithin(t, fresh, &silentRanges{initialReady: true}, context.Background(), 3*time.Second)
		if !done || err != nil {
			t.Fatalf("done=%v err=%v", done, err)
		}
		return threadsByID(frames), res.Stats
	}

	var st savedState
	if err := json.Unmarshal(saved, &st); err != nil {
		t.Fatal(err)
	}
	got, stats := list(st)
	if u := got["e:1400"]; u == nil || u.Kind != "user" || u.Predecessor != "400" {
		t.Fatalf("restored hybrid one-to-one = %+v", got)
	}
	if got["e:1500"] != nil || stats["encrypted_skipped_group"] != 1 {
		t.Fatalf("restored hybrid group must be skipped and counted: %v %v", got, stats)
	}

	old := st
	old.HybridKind = nil // state written before the proof was saved
	got, stats = list(old)
	if len(got) != 0 || stats["encrypted_skipped_group"] != 2 {
		t.Fatalf("old state must skip both until the hybrid row is seen again: %v %v", got, stats)
	}
}
