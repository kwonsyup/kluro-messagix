// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/graphql"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
	"go.mau.fi/whatsmeow/types/events"
)

// pager is the thread listing's client: ExecuteTasks answers a group-1 page from a script and records the request.
// With no script it answers one page, from group1 if set, whose range row says there is nothing more.
type pager struct {
	group1 *table.LSTable
	pages  []*table.LSTable
	cursor string
	err    error
	tasks  []socket.FetchThreadsTask
}

func (r *pager) GetCursor(int64) string { return r.cursor }

func (r *pager) ExecuteTasks(_ context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	task := tasks[0].(*socket.FetchThreadsTask)
	r.tasks = append(r.tasks, *task)
	if r.err != nil {
		return nil, r.err
	}
	if r.pages != nil {
		if len(r.tasks) > len(r.pages) {
			return nil, errors.New("walked past the scripted pages")
		}
		return r.pages[len(r.tasks)-1], nil
	}
	tbl := &table.LSTable{}
	if r.group1 != nil {
		tbl = r.group1
	}
	tbl.LSUpsertSyncGroupThreadsRange = append(tbl.LSUpsertSyncGroupThreadsRange, &table.LSUpsertSyncGroupThreadsRange{SyncGroup: 1, ParentThreadKey: -1, HasMoreBefore: false})
	return tbl, nil
}

func listWithin(t *testing.T, s *messagixSession, r threadPager, ctx context.Context, bound time.Duration) (frames []Frame, stats map[string]int, err error, done bool) {
	frames, res, err, done := listGap(t, s, r, ctx, bound)
	return frames, res.Stats, err, done
}

func listGap(t *testing.T, s *messagixSession, r threadPager, ctx context.Context, bound time.Duration) (frames []Frame, res listingDone, err error, done bool) {
	t.Helper()
	type result struct {
		res listingDone
		err error
	}
	ch := make(chan result, 1)
	go func() {
		st, err := s.listThreads(ctx, r, func(f Frame) int64 { frames = append(frames, f); return 0 })
		ch <- result{st, err}
	}()
	select {
	case v := <-ch:
		return frames, v.res, v.err, true
	case <-time.After(bound):
		return nil, listingDone{}, nil, false
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
		"encrypted_known": 3, "encrypted_emitted": 3, "e2ee_connected": 0, "encrypted_unlisted": 0, "e2ee_offline_events": 0, "e2ee_offline_done": 0, "resume_sync_rows": 0,
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
		"encrypted_known": 3, "encrypted_emitted": 1, "e2ee_connected": 0, "encrypted_unlisted": 2, "e2ee_offline_events": 0, "e2ee_offline_done": 0, "resume_sync_rows": 0,
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

// The listing talks to the library only through ExecuteTasks (a plain request and response) and GetCursor: it
// cannot call FetchMoreThreads, WaitForInitialThreadRange or PostHandlePublishResponse, so it never reads or
// writes the library's keystore or sync state. The real client satisfies it.
var _ threadPager = (*messagix.Client)(nil)

func TestListingTalksToTheLibraryOnlyThroughPlainRequests(t *testing.T) {
	typ := reflect.TypeOf((*threadPager)(nil)).Elem()
	var names []string
	for i := 0; i < typ.NumMethod(); i++ {
		names = append(names, typ.Method(i).Name)
	}
	if len(names) != 2 || names[0] != "ExecuteTasks" || names[1] != "GetCursor" {
		t.Fatalf("threadPager methods = %v", names)
	}
}

// Message requests (sync group 95) are never listed: every request is for sync group 1, with the group-1 cursor.
func TestListingNeverTouchesGroup95(t *testing.T) {
	r := &pager{cursor: "cur-1"}
	_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err != nil || res.Stats == nil || res.Gap != "" {
		t.Fatalf("done=%v err=%v res=%+v", done, err, res)
	}
	if len(r.tasks) != 1 {
		t.Fatalf("walk made %d requests, want 1", len(r.tasks))
	}
	for _, task := range r.tasks {
		if task.SyncGroup != 1 || task.Cursor != "cur-1" {
			t.Fatalf("listing asked %+v", task)
		}
	}
}

func rangeRow(more bool, parent, minKey, minTS int64) *table.LSUpsertSyncGroupThreadsRange {
	return &table.LSUpsertSyncGroupThreadsRange{SyncGroup: 1, ParentThreadKey: parent, HasMoreBefore: more, MinThreadKey: minKey, MinLastActivityTimestampMS: minTS}
}

func page(thread int64, rows ...*table.LSUpsertSyncGroupThreadsRange) *table.LSTable {
	return &table.LSTable{
		LSDeleteThenInsertThread:      []*table.LSDeleteThenInsertThread{{ThreadKey: thread, ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "T"}},
		LSUpsertSyncGroupThreadsRange: rows,
	}
}

// The walk starts at the first page, addresses each next page by the previous reply's group-1 range row, and ends
// complete when a reply says there is nothing more: every thread is emitted.
func TestWalkFollowsEachRepliesRangeRowToTheEnd(t *testing.T) {
	r := &pager{pages: []*table.LSTable{
		page(101, rangeRow(true, -1, 90, 5000)),
		page(102, rangeRow(true, -1, 80, 4000)),
		page(103, rangeRow(false, -1, 70, 3000)),
	}}
	frames, res, err, done := listGap(t, newTestSession(), r, context.Background(), 10*time.Second)
	if !done || err != nil || res.Gap != "" {
		t.Fatalf("done=%v err=%v gap=%q", done, err, res.Gap)
	}
	want := []socket.FetchThreadsTask{
		{ParentThreadKey: -1, ReferenceThreadKey: 0, ReferenceActivityTimestamp: 9999999999999, Cursor: "", SyncGroup: 1},
		{ParentThreadKey: -1, ReferenceThreadKey: 90, ReferenceActivityTimestamp: 5000, Cursor: "", SyncGroup: 1},
		{ParentThreadKey: -1, ReferenceThreadKey: 80, ReferenceActivityTimestamp: 4000, Cursor: "", SyncGroup: 1},
	}
	if len(r.tasks) != 3 {
		t.Fatalf("requests = %+v", r.tasks)
	}
	for i := range want {
		if r.tasks[i] != want[i] {
			t.Fatalf("request %d = %+v, want %+v", i, r.tasks[i], want[i])
		}
	}
	got := threadsByID(frames)
	for _, id := range []string{"101", "102", "103"} {
		if got[id] == nil || got[id].Kind != "user" {
			t.Fatalf("thread %s not emitted: %v", id, got)
		}
	}
}

// A reply with no group-1 range row, or one that does not move, ends the walk incomplete; so does the page bound.
func TestWalkEndsIncompleteWithoutAProgressingRange(t *testing.T) {
	for name, pages := range map[string][]*table.LSTable{
		"no range row":        {page(101, rangeRow(true, -1, 90, 5000)), page(102)},
		"only another group":  {page(101, &table.LSUpsertSyncGroupThreadsRange{SyncGroup: 95, HasMoreBefore: true, MinThreadKey: 1})},
		"range does not move": {page(101, rangeRow(true, -1, 90, 5000)), page(102, rangeRow(true, -1, 90, 5000))},
	} {
		t.Run(name, func(t *testing.T) {
			r := &pager{pages: pages}
			_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 10*time.Second)
			if !done || err != nil || res.Gap != "inbox_incomplete" || res.Stats == nil {
				t.Fatalf("done=%v err=%v res=%+v", done, err, res)
			}
		})
	}
	t.Run("page bound", func(t *testing.T) {
		const bound = 3
		old := threadBatchLimit
		threadBatchLimit = bound
		defer func() { threadBatchLimit = old }()
		oldDelay := threadBatchDelay
		threadBatchDelay = time.Millisecond
		defer func() { threadBatchDelay = oldDelay }()
		var pages []*table.LSTable
		for i := int64(1); i <= bound+2; i++ {
			pages = append(pages, page(100+i, rangeRow(true, -1, 100-i, 9000-i)))
		}
		r := &pager{pages: pages}
		_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 10*time.Second)
		if !done || err != nil || res.Gap != "inbox_incomplete" || len(r.tasks) != bound {
			t.Fatalf("done=%v err=%v gap=%q requests=%d, want %d", done, err, res.Gap, len(r.tasks), bound)
		}
	})
}

// Only the inbox parent's own row continues or completes the walk. A competing group-1 row for another parent, before
// or after it, neither completes the inbox early nor redirects the next request into that folder; with several rows for
// the inbox parent the last one wins.
func TestWalkUsesOnlyTheRequestedParentsRangeRow(t *testing.T) {
	other := func(more bool) *table.LSUpsertSyncGroupThreadsRange { return rangeRow(more, 7, 55, 1) }
	for name, tc := range map[string]struct {
		pages    []*table.LSTable
		wantGap  string
		requests int
	}{
		"inbox more, then other done":  {[]*table.LSTable{page(101, rangeRow(true, -1, 90, 5000), other(false)), page(102, rangeRow(false, -1, 80, 4000))}, "", 2},
		"other done, then inbox more":  {[]*table.LSTable{page(101, other(false), rangeRow(true, -1, 90, 5000)), page(102, rangeRow(false, -1, 80, 4000))}, "", 2},
		"other more does not redirect": {[]*table.LSTable{page(101, rangeRow(true, -1, 90, 5000), other(true)), page(102, rangeRow(false, -1, 80, 4000))}, "", 2},
		"inbox done, other more":       {[]*table.LSTable{page(101, rangeRow(false, -1, 90, 5000), other(true))}, "", 1},
		"only another parent":          {[]*table.LSTable{page(101, other(false))}, "inbox_incomplete", 1},
		"only another parent, more":    {[]*table.LSTable{page(101, other(true))}, "inbox_incomplete", 1},
		"several inbox rows, last":     {[]*table.LSTable{page(101, rangeRow(true, -1, 90, 5000), rangeRow(false, -1, 80, 4000))}, "", 1},
	} {
		t.Run(name, func(t *testing.T) {
			r := &pager{pages: tc.pages}
			frames, res, err, done := listGap(t, newTestSession(), r, context.Background(), 10*time.Second)
			if !done || err != nil || res.Gap != tc.wantGap || len(r.tasks) != tc.requests {
				t.Fatalf("done=%v err=%v gap=%q requests=%d, want gap %q and %d requests", done, err, res.Gap, len(r.tasks), tc.wantGap, tc.requests)
			}
			for _, task := range r.tasks {
				if task.ParentThreadKey != -1 {
					t.Fatalf("a request was redirected to parent %d", task.ParentThreadKey)
				}
			}
			if tc.requests == 2 && (r.tasks[1].ReferenceThreadKey != 90 || r.tasks[1].ReferenceActivityTimestamp != 5000) {
				t.Fatalf("second request = %+v, want the inbox row's reference", r.tasks[1])
			}
			if threadsByID(frames)["101"] == nil {
				t.Fatal("the page's threads are still emitted")
			}
		})
	}
}

// A HasMoreBefore=false row for parent 0 ends the group (the library's own convention), whether or not the inbox
// parent's row is also there; parent 0 saying "more" decides nothing. Rows are read in order and the last deciding
// row wins, as the library's keystore updates do.
func TestWalkCompletesOnAParentZeroEndRow(t *testing.T) {
	zero := func(more bool) *table.LSUpsertSyncGroupThreadsRange { return rangeRow(more, 0, 0, 0) }
	for name, tc := range map[string]struct {
		pages    []*table.LSTable
		wantGap  string
		requests int
	}{
		"parent 0 end completes":              {[]*table.LSTable{page(101, zero(false))}, "", 1},
		"parent 0 more is not a decision":     {[]*table.LSTable{page(101, zero(true))}, "inbox_incomplete", 1},
		"inbox more, then parent 0 end":       {[]*table.LSTable{page(101, rangeRow(true, -1, 90, 5000), zero(false))}, "", 1},
		"parent 0 end, then inbox more":       {[]*table.LSTable{page(101, zero(false), rangeRow(true, -1, 90, 5000)), page(102, zero(false))}, "", 2},
		"parent 0 more does not redirect":     {[]*table.LSTable{page(101, rangeRow(true, -1, 90, 5000), zero(true)), page(102, rangeRow(false, -1, 80, 4000))}, "", 2},
		"inbox end, then another parent more": {[]*table.LSTable{page(101, rangeRow(false, -1, 90, 5000), rangeRow(true, 7, 55, 1))}, "", 1},
	} {
		t.Run(name, func(t *testing.T) {
			r := &pager{pages: tc.pages}
			_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 10*time.Second)
			if !done || err != nil || res.Gap != tc.wantGap || len(r.tasks) != tc.requests {
				t.Fatalf("done=%v err=%v gap=%q requests=%d, want gap %q and %d requests", done, err, res.Gap, len(r.tasks), tc.wantGap, tc.requests)
			}
			for _, task := range r.tasks {
				if task.ParentThreadKey != -1 {
					t.Fatalf("a request was redirected to parent %d", task.ParentThreadKey)
				}
			}
		})
	}
}

// A failed request is the listing's error, as before.
func TestWalkRequestErrorFailsTheListing(t *testing.T) {
	r := &pager{err: errors.New("boom")}
	_, res, err, done := listGap(t, newTestSession(), r, context.Background(), 3*time.Second)
	if !done || err == nil || res.Stats != nil {
		t.Fatalf("done=%v err=%v res=%+v", done, err, res)
	}
}

// threads_done carries the gap through the loop, and no gap key when complete.
func TestThreadsDoneCarriesTheInboxGap(t *testing.T) {
	const startAndList = `{"t":"start","id":1,"cookies":{"c_user":"1","xs":"x","datr":"d"}}`
	frames := runLoop(t, &fakeBackend{threadGap: "inbox_incomplete"}, startAndList, `{"t":"threads","id":2}`)
	var done *Frame
	for i, f := range frames {
		if f.ID == 2 {
			done = &frames[i]
		}
	}
	if done == nil || done.T != TThreadsDone || done.Gap != "inbox_incomplete" || done.Err != "" {
		t.Fatalf("gap answer = %+v", done)
	}
	b, _ := json.Marshal(Frame{T: TThreadsDone, ID: 2, Gap: "inbox_incomplete"})
	if string(b) != `{"t":"threads_done","id":2,"gap":"inbox_incomplete"}` {
		t.Fatalf("wire = %s", b)
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

// connectorWouldQualify mirrors the connector's threadQualified (messenger_runner.go on main):
// a one-to-one thread qualifies when it names another person, typed or not; a group needs Typed.
func connectorWouldQualify(th *Thread) bool {
	switch th.Kind {
	case "user":
		for _, p := range th.Participants {
			if !p.Self {
				return true
			}
		}
		return false
	case "group":
		return th.Typed
	}
	return false
}

func typedOf(frames []Frame, id string) (typed, found bool) {
	for i := len(frames) - 1; i >= 0; i-- {
		if f := frames[i]; f.T == TThread && f.ThreadEvt.ID == id {
			return f.ThreadEvt.Typed, true
		}
	}
	return false, false
}

func threadRow(key int64, folder string) *table.LSTable {
	return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: key, ThreadType: table.ONE_TO_ONE, FolderName: folder, ThreadName: "Stranger", LastActivityTimestampMs: 77}}}
}

// A thread row in the pending or spam folder, or a status-1 request row, leaves the
// thread untyped whatever its type; the kind is still set.
func TestUnacceptedThreadsAreNeverTyped(t *testing.T) {
	for name, tbl := range map[string]*table.LSTable{
		"pending": threadRow(500, "pending"),
		"spam":    threadRow(500, "spam"),
		"status 1": {
			LSDeleteThenInsertThread:         []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, ThreadName: "Stranger", LastActivityTimestampMs: 77}},
			LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 1}},
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			frames := s.handleTableFrames(tbl)
			if typed, found := typedOf(frames, "500"); !found || typed {
				t.Fatalf("a request thread must be emitted untyped: typed=%v found=%v", typed, found)
			}
			// the real kind and participants stay in memory for when it is accepted
			if th := s.threads["500"]; th == nil || th.Kind != "user" || len(th.Participants) == 0 {
				t.Fatalf("the real kind and participants are kept in memory: %+v", th)
			}
			// the wire frame qualifies for nothing: no kind, no type, no participants
			check := func(where string, th *Thread) {
				t.Helper()
				if th == nil || th.Kind != "" || th.Typed || th.Title != "" || th.LastActivity != 77 || len(th.Participants) != 0 || connectorWouldQualify(th) {
					t.Fatalf("%s: a request thread must be emitted with only its id and activity: %+v", where, th)
				}
			}
			for _, f := range frames {
				if f.T == TThread && f.ThreadEvt.ID == "500" {
					check("row", f.ThreadEvt)
				}
			}
			for _, f := range s.sortedThreadFrames() {
				if f.ThreadEvt.ID == "500" {
					check("listing", f.ThreadEvt)
				}
			}
			later := s.handleTableFrames(&table.LSTable{LSUpdateOrInsertThread: []*table.LSUpdateOrInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, ThreadName: "N"}}})
			for _, f := range later {
				if f.T == TThread && f.ThreadEvt.ID == "500" {
					check("later folderless row", f.ThreadEvt)
				}
			}
		})
	}
}

// A different non-empty folder, or a request status other than 1, clears the mark and
// re-emits the thread typed. An empty folder neither sets nor clears it.
func TestAcceptingARequestTypesAndReemitsTheThread(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(threadRow(500, "pending"))
	// an empty folder does not clear
	frames := s.handleTableFrames(threadRow(500, ""))
	if typed, _ := typedOf(frames, "500"); typed {
		t.Fatal("an empty folder must not clear the mark")
	}
	// an inbox row clears and re-emits typed
	frames = s.handleTableFrames(threadRow(500, "inbox"))
	if typed, found := typedOf(frames, "500"); !found || !typed {
		t.Fatalf("an inbox row must type and re-emit: typed=%v found=%v", typed, found)
	}
	for _, f := range frames {
		if f.T == TThread && f.ThreadEvt.ID == "500" && (f.ThreadEvt.Kind != "user" || f.ThreadEvt.Title != "Stranger" || !connectorWouldQualify(f.ThreadEvt)) {
			t.Fatalf("acceptance must re-emit the real kind and participants: %+v", f.ThreadEvt)
		}
	}
	// an empty folder does not set it either
	frames = s.handleTableFrames(threadRow(500, ""))
	if typed, _ := typedOf(frames, "500"); !typed {
		t.Fatal("an empty folder must not mark a thread")
	}
	// a status-1 row marks an existing typed thread and re-emits it untyped; status 0 clears it
	frames = s.handleTableFrames(&table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 1}}})
	if typed, found := typedOf(frames, "500"); !found || typed {
		t.Fatalf("a status-1 row must untype and re-emit: typed=%v found=%v", typed, found)
	}
	frames = s.handleTableFrames(&table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 0}}})
	if typed, found := typedOf(frames, "500"); !found || !typed {
		t.Fatalf("status 0 must type and re-emit: typed=%v found=%v", typed, found)
	}
	// the mark is memory only: a new session knows nothing of it
	if fresh := newTestSession(); len(fresh.folderMark)+len(fresh.statusMark) != 0 {
		t.Fatal("the mark must not be persisted")
	}
}

// Encrypted threads are unchanged by the mark.
func TestEncryptedThreadsAreNotAffectedByTheMark(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
		{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, FolderName: "pending"},
	}})
	if typed, found := typedOf(frames, "e:4242"); !found || !typed {
		t.Fatalf("encrypted rows are not covered by the mark: typed=%v found=%v", typed, found)
	}
}

// A spam or pending folder is only cleared by a folder, and a request status of 0 only clears what a
// status row set: spam plus a status-0 row in one batch stays unaccepted, as in the library.
func TestSpamFolderIsNotClearedByAStatusZeroRow(t *testing.T) {
	s := newTestSession()
	frames := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread:         []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "spam"}},
		LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 0}},
	})
	for _, f := range frames {
		if f.T == TThread && f.ThreadEvt.ID == "500" && (f.ThreadEvt.Kind != "" || f.ThreadEvt.Typed || len(f.ThreadEvt.Participants) != 0) {
			t.Fatalf("spam with a status-0 row must stay unaccepted: %+v", f.ThreadEvt)
		}
	}
	// Each source clears only itself: a status-1 row then a folder row leaves it unaccepted until both clear.
	s.handleTableFrames(&table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 1}}})
	frames = s.handleTableFrames(threadRow(500, "inbox"))
	if th := lastThread(frames, "500"); th != nil && th.Kind != "" {
		t.Fatalf("an inbox folder must not clear the status mark: %+v", th)
	}
	frames = s.handleTableFrames(&table.LSTable{LSDeleteThenInsertMessageRequest: []*table.LSDeleteThenInsertMessageRequest{{ThreadKey: 500, MessageRequestStatus: 0}}})
	if th := lastThread(frames, "500"); th == nil || th.Kind != "user" || !th.Typed {
		t.Fatalf("both sources cleared: the thread is accepted again: %+v", th)
	}
}

func lastThread(frames []Frame, id string) *Thread {
	for i := len(frames) - 1; i >= 0; i-- {
		if f := frames[i]; f.T == TThread && f.ThreadEvt.ID == id {
			return f.ThreadEvt
		}
	}
	return nil
}

// A live rejection that lands between the listing's snapshot of ids and its emit of a thread
// must not be undone by a stale typed frame.
func TestListingNeverEmitsAnOlderMarkThanALiveFrame(t *testing.T) {
	s := newTestSession()
	s.handleTableFrames(threadRow(500, "inbox"))
	var live []Frame
	s.beforeListEmit = func(id string) {
		if id == "500" {
			live = s.handleTableFrames(threadRow(500, "pending")) // the rejection, after the snapshot
		}
	}
	var listed []Frame
	if err := s.emitKnownThreads(context.Background(), func(f Frame) int64 { listed = append(listed, f); return 0 }); err != nil {
		t.Fatal(err)
	}
	if th := lastThread(live, "500"); th == nil || th.Kind != "" {
		t.Fatalf("the live frame is the rejected shape: %+v", th)
	}
	th := lastThread(listed, "500")
	if th == nil || th.Kind != "" || th.Typed || len(th.Participants) != 0 {
		t.Fatalf("the listing must emit the unaccepted shape, not a stale typed frame: %+v", th)
	}
}

// Live tables and listing pages are built and emitted inside one hold of the session lock, so a later
// row cannot land between a frame's build and its emit; thread frames stay ahead of their messages.
func TestTablesAreEmittedUnderTheSameLockAsTheirBuild(t *testing.T) {
	tbl := func() *table.LSTable {
		return &table.LSTable{
			LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "inbox"}},
			LSInsertMessage:          []*table.LSInsertMessage{{ThreadKey: 500, MessageId: "mid.1", SenderId: 500, Text: "hi", TimestampMs: 1}},
		}
	}
	for name, run := range map[string]func(s *messagixSession, emit emitFunc){
		"live table": func(s *messagixSession, emit emitFunc) { s.emit = emit; s.handleTable(tbl()) },
		"listing page": func(s *messagixSession, emit emitFunc) {
			if _, err := s.listThreads(context.Background(), &pager{group1: tbl()}, emit); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			var order []string
			emit := func(f Frame) int64 {
				if s.mu.TryLock() {
					s.mu.Unlock()
					t.Errorf("a %s frame was emitted outside the lock that built it", f.T)
				}
				order = append(order, f.T)
				return 0
			}
			run(s, emit)
			var types []string
			for _, o := range order {
				if o == TThread || o == TMessage {
					types = append(types, o)
				}
			}
			if len(types) < 2 || types[0] != TThread || types[1] != TMessage {
				t.Fatalf("the thread frame must come ahead of its message: %v", order)
			}
		})
	}
}

// Start's initial table is applied before the connection opens but its thread frames are emitted
// after ready; a live pending row in between must not be undone by the older frame.
func TestInitialThreadFramesAreBuiltAtEmitTime(t *testing.T) {
	s := newTestSession()
	snapshot := s.handleTableFrames(&table.LSTable{
		LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "Stranger", LastActivityTimestampMs: 77}},
		LSInsertMessage:          []*table.LSInsertMessage{{ThreadKey: 500, MessageId: "mid.1", SenderId: 500, Text: "hi", TimestampMs: 1}},
	})
	emitInitial := s.initialEmitter(snapshot)
	if emitInitial == nil {
		t.Fatal("an initial table with threads must yield an emitter")
	}
	// the rejection arrives after Start's snapshot, before main emits the initial frames
	s.handleTableFrames(threadRow(500, "pending"))
	var got []Frame
	emitInitial(func(f Frame) int64 { got = append(got, f); return 0 })
	th := lastThread(got, "500")
	if th == nil || th.Kind != "" || th.Typed || th.Title != "" || len(th.Participants) != 0 || th.LastActivity != 77 {
		t.Fatalf("the initial frame must carry the unaccepted shape, not the Start-time snapshot: %+v", th)
	}
	// the thread frame is still ahead of the message, and the message is emitted once
	var order []string
	for _, f := range got {
		order = append(order, f.T)
	}
	if len(order) != 2 || order[0] != TThread || order[1] != TMessage {
		t.Fatalf("order = %v, want thread then message", order)
	}
	// a table with nothing to emit yields no emitter
	if s.initialEmitter(nil) != nil {
		t.Fatal("no frames, no emitter")
	}
}

func savedConfig(t *testing.T, version int64) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(struct{ Configs *httpclient.Configs }{&httpclient.Configs{
		VersionID: version,
		BrowserConfigTable: &types.SchedulerJSDefineConfig{LSPlatformMessengerSyncParams: types.LSPlatformMessengerSyncParams{
			Mailbox: "mb-params", Contact: "ct-params", E2Ee: "e2-params",
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

type lsCall struct {
	db      int
	version int64
	params  string
	cursor  any
	epoch   int64
}

// A resumed start makes one GraphQL sync of exactly databases 1 and 2 (never 95, the message-requests inbox)
// with the saved version and sync params, and the rows flow through the normal path: a typed encrypted
// one-to-one is emitted as an e: thread, a pending row is minimal.
func TestResumeSyncAsksOnceForDatabases1And2AndFeedsTheNormalPath(t *testing.T) {
	s := newTestSession()
	var calls []lsCall
	s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		p, _ := v.SyncParams.(*string)
		calls = append(calls, lsCall{v.Database, v.Version, *p, v.LastAppliedCursor, v.EpochID})
		switch v.Database {
		case 1:
			return &table.LSTable{
				LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{
					{ThreadKey: 4242, ThreadType: table.ENCRYPTED_OVER_WA_ONE_TO_ONE, ThreadName: "Enc", LastActivityTimestampMs: 5},
					{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "pending", ThreadName: "Stranger", LastActivityTimestampMs: 9},
				},
				LSVerifyHybridThreadExists:   []*table.LSVerifyHybridThreadExists{{ThreadKey: 77, ThreadJID: 7777, ThreadType: table.ONE_TO_ONE}},
				LSCreateOpenToE2EEThreadLink: []*table.LSCreateOpenToE2EEThreadLink{{OpenThreadID: 42, ArmadilloThreadID: 4242}},
			}, nil
		}
		return &table.LSTable{}, nil
	}
	frames := s.resumeSync(context.Background(), savedConfig(t, 123))
	want := []lsCall{{1, 123, "mb-params", nil, 0}, {2, 123, "ct-params", nil, 0}}
	if len(calls) != 2 || calls[0] != want[0] || calls[1] != want[1] {
		t.Fatalf("sync calls = %+v, want %+v", calls, want)
	}
	if s.cutover[4242] != 42 {
		t.Fatalf("the cutover link must be applied: %v", s.cutover)
	}
	var got []Frame
	s.initialEmitter(frames)(func(f Frame) int64 { got = append(got, f); return 0 })
	enc := lastThread(got, "e:4242")
	if enc == nil || enc.Kind != "user" || !enc.Encrypted || !enc.Typed || enc.Predecessor != "42" {
		t.Fatalf("a typed encrypted one-to-one must be emitted as an e: thread: %+v", enc)
	}
	req := lastThread(got, "500")
	if req == nil || req.Kind != "" || req.Typed || req.Title != "" || len(req.Participants) != 0 {
		t.Fatalf("a pending row from the sync must be minimal: %+v", req)
	}
	if s.fbToWA[77] != 7777 {
		t.Fatalf("the hybrid mapping must be recorded: %v", s.fbToWA)
	}
	// rows: db 1 has 2 threads + 1 hybrid + 1 cutover link, db 2 none
	if got := s.listingStats(0, 0, 0)["resume_sync_rows"]; got != 4 {
		t.Fatalf("resume_sync_rows = %d, want 4", got)
	}
}

// A failed call never blocks the listing: database 1 failing yields nothing, a later database failing keeps
// database 1's table, and a saved config without a version skips the sync without any call.
func TestResumeSyncFailureNeverBlocksTheListing(t *testing.T) {
	for name, tc := range map[string]struct {
		fail    int
		version int64
		wantNil bool
		calls   int
	}{
		"database 1 fails": {1, 123, true, 1},
		"database 2 fails": {2, 123, false, 2},
		"no saved version": {0, 0, true, 0},
	} {
		t.Run(name, func(t *testing.T) {
			s := newTestSession()
			n := 0
			s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
				n++
				if v.Database == tc.fail {
					return nil, errors.New("boom")
				}
				return &table.LSTable{LSDeleteThenInsertThread: []*table.LSDeleteThenInsertThread{{ThreadKey: 600, ThreadType: table.ONE_TO_ONE}}}, nil
			}
			frames := s.resumeSync(context.Background(), savedConfig(t, tc.version))
			if (frames == nil) != tc.wantNil || n != tc.calls {
				t.Fatalf("frames nil=%v calls=%d, want nil=%v calls=%d", frames == nil, n, tc.wantNil, tc.calls)
			}
			_, stats, err, done := listWithin(t, s, &pager{}, context.Background(), 3*time.Second)
			if !done || err != nil || stats == nil {
				t.Fatalf("the listing must still finish: done=%v err=%v", done, err)
			}
		})
	}
}

func TestResumeSyncHonoursCancel(t *testing.T) {
	s := newTestSession()
	s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	done := make(chan []Frame, 1)
	go func() { done <- s.resumeSync(ctx, savedConfig(t, 123)) }()
	select {
	case tbl := <-done:
		if tbl != nil {
			t.Fatal("a cancelled sync returns nothing")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancel did not free the sync")
	}
}

// The E2EE offline-sync counts reach the stats.
func TestOfflineSyncStats(t *testing.T) {
	s := newTestSession()
	if st := s.listingStats(0, 0, 0); st["e2ee_offline_events"] != 0 || st["e2ee_offline_done"] != 0 {
		t.Fatalf("before any event: %v", st)
	}
	s.handleWAEvent(&events.OfflineSyncPreview{Total: 9, Messages: 7})
	if st := s.listingStats(0, 0, 0); st["e2ee_offline_events"] != 7 || st["e2ee_offline_done"] != 0 {
		t.Fatalf("after the preview: %v", st)
	}
	s.handleWAEvent(&events.OfflineSyncCompleted{})
	if st := s.listingStats(0, 0, 0); st["e2ee_offline_events"] != 7 || st["e2ee_offline_done"] != 1 {
		t.Fatalf("after completion: %v", st)
	}
}

// The resume snapshot is applied before the socket opens, so every socket row lands after it and
// wins: a thread the snapshot shows in the inbox that a socket row then moves to spam stays unaccepted.
func TestResumeSnapshotIsAppliedBeforeTheSocketOpens(t *testing.T) {
	s := newTestSession()
	s.ctx = context.Background()
	s.lsRequest = func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		if v.Database == 1 {
			return &table.LSTable{
				LSDeleteThenInsertThread:   []*table.LSDeleteThenInsertThread{{ThreadKey: 500, ThreadType: table.ONE_TO_ONE, FolderName: "inbox", ThreadName: "N"}},
				LSVerifyHybridThreadExists: []*table.LSVerifyHybridThreadExists{{ThreadKey: 77, ThreadJID: 7777, ThreadType: table.ONE_TO_ONE}},
			}, nil
		}
		return &table.LSTable{}, nil
	}
	appliedAtConnect := false
	connect := func(context.Context) error {
		s.mu.Lock()
		appliedAtConnect = s.fbToWA[77] == 7777 && s.threads["500"] != nil
		s.mu.Unlock()
		// the first socket delta: the thread moves to spam
		s.handleTableFrames(threadRow(500, "spam"))
		return nil
	}
	frames, err := s.openSocket(context.Background(), true, savedConfig(t, 123), connect)
	if err != nil {
		t.Fatal(err)
	}
	if !appliedAtConnect {
		t.Fatal("the snapshot must be applied before the socket opens")
	}
	if th := lastThread(frames, "500"); th == nil {
		t.Fatal("the snapshot's thread frame is part of the initial frames")
	}
	var got []Frame
	s.initialEmitter(frames)(func(f Frame) int64 { got = append(got, f); return 0 })
	if th := lastThread(got, "500"); th == nil || th.Kind != "" || th.Typed || th.Title != "" {
		t.Fatalf("the socket row that moved the thread to spam must win over the snapshot: %+v", th)
	}

	// A session that did not resume only connects: no sync request is made.
	fresh := newTestSession()
	called := 0
	fresh.lsRequest = func(context.Context, *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		called++
		return &table.LSTable{}, nil
	}
	fresh.ctx = context.Background()
	if f, err := fresh.openSocket(context.Background(), false, nil, func(context.Context) error { return nil }); err != nil || f != nil || called != 0 {
		t.Fatalf("a fresh session only connects: frames=%v err=%v syncCalls=%d", f, err, called)
	}
	// A connect failure is returned.
	if _, err := fresh.openSocket(context.Background(), false, nil, func(context.Context) error { return errors.New("down") }); err == nil {
		t.Fatal("a connect failure must be returned")
	}
}

// Cancelling Start during the resume sync stops it before the socket opens; the sync's own timeout
// is not a cancel and still connects.
func TestOpenSocketChecksTheCallersCancelBeforeConnecting(t *testing.T) {
	blocked := func(ctx context.Context, v *graphql.LSPlatformGraphQLLightspeedVariables) (*table.LSTable, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	// the caller cancels during the sync
	s := newTestSession()
	s.ctx = context.Background()
	s.lsRequest = blocked
	ctx, cancel := context.WithCancel(context.Background())
	go func() { time.Sleep(50 * time.Millisecond); cancel() }()
	connected := false
	_, err := s.openSocket(ctx, true, savedConfig(t, 123), func(context.Context) error { connected = true; return nil })
	if !errors.Is(err, context.Canceled) || connected {
		t.Fatalf("a cancelled Start must not connect: err=%v connected=%v", err, connected)
	}
	if code := codeOf(classifyStartError("connect", err)); code != "connect_cancelled" {
		t.Fatalf("Start reports %q", code)
	}

	// the sync's internal timeout still falls through to connect
	old := resumeSyncTimeout
	resumeSyncTimeout = 50 * time.Millisecond
	defer func() { resumeSyncTimeout = old }()
	s2 := newTestSession()
	s2.ctx = context.Background()
	s2.lsRequest = blocked
	connected = false
	frames, err := s2.openSocket(context.Background(), true, savedConfig(t, 123), func(context.Context) error { connected = true; return nil })
	if err != nil || !connected || frames != nil {
		t.Fatalf("a sync timeout must still connect: err=%v connected=%v frames=%v", err, connected, frames)
	}
}
