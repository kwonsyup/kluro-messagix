// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// pager is a group-1 thread pager that records which sync groups were asked for.
type pager struct {
	group1 *table.LSTable
	groups []int64
}

func (r *pager) FetchMoreThreads(_ context.Context, group int64) (*socket.KeyStoreData, *table.LSTable, error) {
	r.groups = append(r.groups, group)
	tbl := &table.LSTable{}
	if group == 1 && r.group1 != nil {
		tbl = r.group1
	}
	return &socket.KeyStoreData{MinThreadKey: 7}, tbl, nil
}

func listWithin(t *testing.T, s *messagixSession, r threadPager, ctx context.Context, bound time.Duration) (frames []Frame, stats map[string]int, err error, done bool) {
	t.Helper()
	type result struct {
		stats map[string]int
		err   error
	}
	ch := make(chan result, 1)
	go func() {
		st, err := s.listThreads(ctx, r, func(f Frame) int64 { frames = append(frames, f); return 0 })
		ch <- result{st, err}
	}()
	select {
	case v := <-ch:
		return frames, v.stats, v.err, true
	case <-time.After(bound):
		return nil, nil, nil, false
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
	b, _ = json.Marshal(Frame{T: TThreadsDone, ID: 2})
	if string(b) != `{"t":"threads_done","id":2}` {
		t.Fatalf("wire = %s", b)
	}
}

// A group-1 row of an encrypted-over-WA type becomes an e: thread of the right kind.
func TestEncryptedGroup1RowsEmitAsEncryptedThreads(t *testing.T) {
	r := &pager{group1: &table.LSTable{
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
	frames, stats, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
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
	wantStats(t, stats, map[string]int{
		"group1_threads": 3, "group1_encrypted_rows": 3, "hybrid_mappings": 0, "cutover_links": 0,
		"encrypted_known": 3, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_unlisted": 0, "request_rows_skipped": 0, "untyped_typed_requests": 0,
	})
}

// A cutover link or hybrid mapping whose chat no table named is counted in
// encrypted_unlisted and not emitted: nothing is synthesized from the links.
func TestKnownEncryptedKeyWithNoTableRowIsCountedNotEmitted(t *testing.T) {
	s := newTestSession()
	s.threads["100"] = &threadInfo{Key: 100, Kind: "user", Typed: true, Participants: []int64{1, 100}}
	s.cutover[1100] = 100                                                                                           // no row for e:1100
	s.fbToWA[400] = 1400                                                                                            // no row for e:1400
	s.threads["e:1600"] = &threadInfo{Key: 1600, Enc: true, Kind: "user", Typed: true, Participants: []int64{1600}} // listed
	s.cutover[1600] = 600

	frames, stats, err, done := listWithin(t, s, &pager{}, context.Background(), 3*time.Second)
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
	wantStats(t, stats, map[string]int{
		"group1_threads": 0, "group1_encrypted_rows": 0, "hybrid_mappings": 1, "cutover_links": 2,
		"encrypted_known": 3, "encrypted_emitted": 1, "e2ee_connected": 0, "encrypted_unlisted": 2, "request_rows_skipped": 0, "untyped_typed_requests": 0,
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

// Message requests (sync group 95) are never listed: the listing pages group 1 only.
func TestListingNeverTouchesGroup95(t *testing.T) {
	r := &pager{}
	_, stats, err, done := listWithin(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil || stats == nil {
		t.Fatalf("done=%v err=%v stats=%v", done, err, stats)
	}
	if len(r.groups) == 0 {
		t.Fatal("group 1 was never paged")
	}
	for _, g := range r.groups {
		if g != 1 {
			t.Fatalf("listing asked for sync group %d", g)
		}
	}
}

func TestListingCancelStillReportsTheCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, stats, err, done := listWithin(t, newTestSession(), &pager{}, ctx, 3*time.Second)
	if !done || err == nil || stats != nil {
		t.Fatalf("a cancelled listing must report the cancel, not stats: done=%v stats=%v err=%v", done, stats, err)
	}
}

func frameKinds(frames []Frame) (threads map[string]*Thread, messages int) {
	threads = map[string]*Thread{}
	for _, f := range frames {
		switch f.T {
		case TThread:
			threads[f.ThreadEvt.ID] = f.ThreadEvt
		case TMessage:
			messages++
		}
	}
	return threads, messages
}

// A thread row in the pending folder, a message request status row, and live
// messages in either are skipped and counted; no frame is built. Moving the
// thread to the inbox lists it again.
func TestRequestThreadsAndTheirMessagesAreSkippedAndCounted(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
			{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "pending", ThreadName: "Stranger"},
			{ThreadKey: 600, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"},
		},
		LSAddParticipantIdToGroupThread: []*table.LSAddParticipantIdToGroupThread{{ThreadKey: 500, ContactId: 5}},
		LSInsertMessage: []*table.LSInsertMessage{
			{ThreadKey: 500, MessageId: "mid.R1", SenderId: 5, Text: "hi", TimestampMs: 1},
			{ThreadKey: 600, MessageId: "mid.I1", SenderId: 6, Text: "hello", TimestampMs: 2},
		},
	})
	threads, msgs := frameKinds(frames)
	if threads["500"] != nil || threads["600"] == nil || msgs != 1 {
		t.Fatalf("request must not be emitted: threads=%v messages=%d", threads, msgs)
	}
	// A later live message in the known request thread is skipped too.
	frames = s.handleTableFrames(&table.LSTable{LSInsertMessage: []*table.LSInsertMessage{
		{ThreadKey: 500, MessageId: "mid.R2", SenderId: 5, Text: "again", TimestampMs: 3},
	}})
	if len(frames) != 0 {
		t.Fatalf("a live message in a request thread must produce no frame: %+v", frames)
	}
	// A request status row marks a thread with no pending folder row.
	frames = s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 700, MessageRequestStatus: 1}},
		LSUpdateOrInsertThread:           []*table.LSUpdateOrInsertThread{{ThreadKey: 700, ThreadType: table.ONE_TO_ONE}},
	})
	if len(frames) != 0 {
		t.Fatalf("a status-1 request must produce no frame: %+v", frames)
	}
	stats := s.listingStats(0, 0, 0)
	if stats["request_rows_skipped"] != 4 { // thread row 500, message R1, message R2, thread row 700
		t.Fatalf("request_rows_skipped = %d, want 4: %v", stats["request_rows_skipped"], stats)
	}
	if stats["group1_threads"] != 1 {
		t.Fatalf("request rows must not count as group-1 threads: %v", stats)
	}
	// The user accepts it: the inbox row lists it from then on.
	frames = s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{
		{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"},
	}})
	if th, _ := frameKinds(frames); th["500"] == nil || th["500"].Kind != "user" {
		t.Fatalf("an accepted request must be listed: %+v", th)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

type typingRecorder struct {
	mu    sync.Mutex
	keys  []int64
	reply func(key int64) *table.LSTable
}

func (r *typingRecorder) ask(_ context.Context, key int64) (*table.LSTable, error) {
	r.mu.Lock()
	r.keys = append(r.keys, key)
	r.mu.Unlock()
	return r.reply(key), nil
}

func (r *typingRecorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

func liveMessage(key int64, id string) *table.LSTable {
	return &table.LSTable{LSInsertMessage: []*table.LSInsertMessage{{ThreadKey: key, MessageId: id, SenderId: key, Text: "hi", TimestampMs: 1}}}
}

// A live message in a thread no row typed asks the server once; the reply types it.
func TestUntypedLiveThreadIsTypedOnceThroughTheOpenThreadRequest(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	var mu sync.Mutex
	var emitted []Frame
	s.emit = func(f Frame) int64 { mu.Lock(); emitted = append(emitted, f); mu.Unlock(); return 0 }
	rec := &typingRecorder{reply: func(key int64) *table.LSTable {
		return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: key, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"}}}
	}}
	s.typeFn = rec.ask

	frames := s.handleTableFrames(liveMessage(900, "mid.U1"))
	if th, msgs := frameKinds(frames); th["900"] == nil || th["900"].Kind != "group" || msgs != 1 {
		t.Fatalf("the untyped thread is first emitted with the default kind: %v", th)
	}
	waitFor(t, "the typed thread frame", func() bool {
		mu.Lock()
		defer mu.Unlock()
		th, _ := frameKinds(emitted)
		return th["900"] != nil && th["900"].Kind == "user"
	})
	// Another message in the same thread, now typed or not: no second ask.
	s.handleTableFrames(liveMessage(900, "mid.U2"))
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 1 || rec.keys[0] != 900 {
		t.Fatalf("want exactly one ask for 900, got %v", rec.keys)
	}
	if got := s.listingStats(0, 0, 0)["untyped_typed_requests"]; got != 1 {
		t.Fatalf("untyped_typed_requests = %d, want 1", got)
	}
}

// The ask is bounded per minute overall; a key that was not asked can be asked later.
func TestUntypedAsksAreBoundedPerMinute(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	rec := &typingRecorder{reply: func(key int64) *table.LSTable { return &table.LSTable{} }}
	s.typeFn = rec.ask
	for i := int64(1); i <= untypedAsksPerMinute+5; i++ {
		s.handleTableFrames(liveMessage(1000+i, "mid."+string(rune('a'+i))))
	}
	waitFor(t, "the asks", func() bool { return rec.count() == untypedAsksPerMinute })
	time.Sleep(50 * time.Millisecond)
	if rec.count() != untypedAsksPerMinute {
		t.Fatalf("asked %d times, want at most %d", rec.count(), untypedAsksPerMinute)
	}
	if got := s.listingStats(0, 0, 0)["untyped_typed_requests"]; got != untypedAsksPerMinute {
		t.Fatalf("untyped_typed_requests = %d", got)
	}
}

// If the asked thread is a message request, its folder row marks it and it is skipped from then on.
func TestUntypedLiveThreadThatIsARequestIsSkippedAfterwards(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	var mu sync.Mutex
	var emitted []Frame
	s.emit = func(f Frame) int64 { mu.Lock(); emitted = append(emitted, f); mu.Unlock(); return 0 }
	rec := &typingRecorder{reply: func(key int64) *table.LSTable {
		return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: key, ThreadType: table.ONE_TO_ONE, FolderName: "pending"}}}
	}}
	s.typeFn = rec.ask
	s.handleTableFrames(liveMessage(950, "mid.Q1"))
	waitFor(t, "the request mark", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.requestKeys[950]
	})
	mu.Lock()
	n := len(emitted)
	mu.Unlock()
	if n != 0 {
		t.Fatalf("the typed reply must emit no frame for a request: %+v", emitted)
	}
	if frames := s.handleTableFrames(liveMessage(950, "mid.Q2")); len(frames) != 0 {
		t.Fatalf("later messages in the request are skipped: %+v", frames)
	}
}

// A thread that stays untyped (the reply typed nothing) is still asked about only once per run.
func TestAnUnansweredThreadIsAskedOnlyOnce(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	rec := &typingRecorder{reply: func(int64) *table.LSTable { return &table.LSTable{} }}
	s.typeFn = rec.ask
	s.handleTableFrames(liveMessage(1500, "mid.a"))
	waitFor(t, "the first ask", func() bool { return rec.count() == 1 })
	s.handleTableFrames(liveMessage(1500, "mid.b"))
	s.handleTableFrames(liveMessage(1500, "mid.c"))
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("asked %d times for one thread, want once", rec.count())
	}
}
