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
		"encrypted_known": 3, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_unlisted": 0, "request_threads": 0, "untyped_typed_requests": 0,
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
		"encrypted_known": 3, "encrypted_emitted": 1, "e2ee_connected": 0, "encrypted_unlisted": 2, "request_threads": 0, "untyped_typed_requests": 0,
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

// The helper only labels: a thread whose latest classifying row says pending or
// spam (or a status-1 request row) carries request=true, any other folder or
// status clears it, and every flip re-emits the thread frame.
func TestRequestFlagFollowsTheLatestClassifyingRow(t *testing.T) {
	s := newTestSession()
	flag := func(frames []Frame, id string) (bool, bool) {
		th, _ := frameKinds(frames)
		if th[id] == nil {
			return false, false
		}
		return th[id].Request, true
	}
	row := func(folder string) *table.LSTable {
		return &table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: folder}}}
	}
	if r, ok := flag(s.handleTableFrames(row("pending")), "500"); !ok || !r {
		t.Fatalf("a pending row must flag the thread frame: %v %v", r, ok)
	}
	if r, ok := flag(s.handleTableFrames(row("inbox")), "500"); !ok || r {
		t.Fatalf("an accepted row must flip the flag and re-emit: %v %v", r, ok)
	}
	if r, ok := flag(s.handleTableFrames(row("spam")), "500"); !ok || !r {
		t.Fatalf("spam counts as not accepted: %v %v", r, ok)
	}
	// A status row alone flips an existing thread and re-emits it.
	status := func(n int64) *table.LSTable {
		return &table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: n}}}
	}
	if r, ok := flag(s.handleTableFrames(status(0)), "500"); !ok || r {
		t.Fatalf("status 0 clears: %v %v", r, ok)
	}
	if r, ok := flag(s.handleTableFrames(status(1)), "500"); !ok || !r {
		t.Fatalf("status 1 flags: %v %v", r, ok)
	}
	// A row that repeats the state re-emits nothing new for the flag, and the stat counts flagged threads.
	if got := s.listingStats(0, 0, 0)["request_threads"]; got != 1 {
		t.Fatalf("request_threads = %d, want 1", got)
	}
	// A row with no folder does not classify.
	if r, _ := flag(s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE}}}), "500"); !r {
		t.Fatal("a folderless row must not clear the flag")
	}
	if got := s.listingStats(0, 0, 0)["group1_threads"]; got != 1 {
		t.Fatalf("group1_threads = %d: flagged threads are still rows the listing saw", got)
	}
}

// The helper suppresses nothing: messages, edits and unsends in a flagged thread
// are emitted exactly as in any other thread.
func TestMessagesEditsAndUnsendsInARequestFlowUnchanged(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "pending"}},
		LSInsertMessage:          []*table.LSInsertMessage{{ThreadKey: 500, MessageId: "mid.R1", SenderId: 5, Text: "hi", TimestampMs: 1}},
		LSEditMessage:            []*table.LSEditMessage{{MessageID: "mid.R1", Text: "hi!"}},
		LSDeleteMessage:          []*table.LSDeleteMessage{{ThreadKey: 500, MessageId: "mid.R1"}},
	})
	var gotMsg, gotEdit, gotUnsend bool
	for _, f := range frames {
		switch f.T {
		case TMessage:
			gotMsg = f.Message.ID == "mid.R1" && f.Message.Thread == "500"
		case TEdit:
			gotEdit = true
		case TUnsend:
			gotUnsend = true
		}
	}
	th, _ := frameKinds(frames)
	if !gotMsg || !gotEdit || !gotUnsend || th["500"] == nil || !th["500"].Request {
		t.Fatalf("message=%v edit=%v unsend=%v thread=%+v", gotMsg, gotEdit, gotUnsend, th["500"])
	}
	// A later live message in the flagged thread is emitted too.
	if _, n := frameKinds(s.handleTableFrames(liveMessage(500, "mid.R2"))); n != 1 {
		t.Fatalf("live message in a flagged thread: %d message frames", n)
	}
}

// An encrypted chat mapped from a flagged facebook thread is flagged too and flips with it.
func TestEncryptedChatFollowsItsFacebookThreadsRequestFlag(t *testing.T) {
	s := newTestSession()
	s.fbToWA[400] = 1400
	s.threads["e:1400"] = &threadInfo{Key: 1400, Enc: true, Kind: "user", Typed: true, Participants: []int64{1400}}
	s.threads["400"] = &threadInfo{Key: 400, Kind: "user", Typed: true, Participants: []int64{400}}
	row := func(folder string) *table.LSTable {
		return &table.LSTable{LSVerifyThreadExists: []*table.LSVerifyThreadExists{{ThreadKey: 400, ThreadType: table.ONE_TO_ONE, FolderName: folder}}}
	}
	th, _ := frameKinds(s.handleTableFrames(row("pending")))
	if th["e:1400"] == nil || !th["e:1400"].Request || th["400"] == nil || !th["400"].Request {
		t.Fatalf("flagged facebook thread and its encrypted chat: %+v %+v", th["400"], th["e:1400"])
	}
	for _, f := range s.sortedThreadFrames() {
		if f.ThreadEvt.ID == "e:1400" && !f.ThreadEvt.Request {
			t.Fatal("a later listing must carry the flag on the encrypted thread")
		}
	}
	th, _ = frameKinds(s.handleTableFrames(row("inbox")))
	if th["e:1400"] == nil || th["e:1400"].Request {
		t.Fatalf("acceptance must flip the encrypted chat and re-emit it: %+v", th["e:1400"])
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

// If the asked thread is a message request, the reply's folder row flags the thread frame.
func TestUntypedLiveThreadThatIsARequestIsFlaggedByTheReply(t *testing.T) {
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
	waitFor(t, "the flagged thread frame", func() bool {
		mu.Lock()
		defer mu.Unlock()
		th, _ := frameKinds(emitted)
		return th["950"] != nil && th["950"].Request && th["950"].Kind == "user"
	})
}

// A reply to an ask that went out before a newer classifying row arrived must not re-flag an accepted chat.
func TestStaleAskReplyDoesNotReflagAnAcceptedChat(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	release := make(chan struct{})
	s.typeFn = func(ctx context.Context, key int64) (*table.LSTable, error) {
		<-release
		return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: key, ThreadType: table.ONE_TO_ONE, FolderName: "pending"}}}, nil
	}
	var mu sync.Mutex
	var emitted []Frame
	s.emit = func(f Frame) int64 { mu.Lock(); emitted = append(emitted, f); mu.Unlock(); return 0 }
	s.handleTableFrames(liveMessage(960, "mid.S1")) // the ask goes out here
	// The user accepts the chat before the reply lands.
	s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 960, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"}}})
	close(release)
	// The reply types the thread (kind from its own row) but must not classify it.
	waitFor(t, "the reply to be applied", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(emitted) > 0
	})
	time.Sleep(50 * time.Millisecond)
	s.mu.Lock()
	flagged := s.requestKeys[960]
	s.mu.Unlock()
	mu.Lock()
	defer mu.Unlock()
	if flagged {
		t.Fatal("a stale reply must not re-flag an accepted chat")
	}
	for _, f := range emitted {
		if f.T == TThread && f.ThreadEvt.ID == "960" && f.ThreadEvt.Request {
			t.Fatalf("a stale reply emitted a flagged frame: %+v", f.ThreadEvt)
		}
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

func TestThreadRequestWireKey(t *testing.T) {
	on, _ := json.Marshal(Thread{ID: "5", Kind: "user", Request: true})
	off, _ := json.Marshal(Thread{ID: "5", Kind: "user"})
	if string(on) != `{"id":"5","kind":"user","request":true}` || string(off) != `{"id":"5","kind":"user"}` {
		t.Fatalf("wire = %s / %s", on, off)
	}
}
