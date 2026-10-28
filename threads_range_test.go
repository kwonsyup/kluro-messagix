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
