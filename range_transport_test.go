// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"
	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// Review P2 on cab3c480: the other walk tests deliver later rows through handleMetaEvent, which is how the library
// hands over a LIVE frame (a table on another stream). They never model a second data frame on the request's own
// one-off stream. This test does, through the pinned library's real transport: a dgw.Socket, with the session's own
// library logger, dialled to an in-process server that speaks the dgw frame protocol, and a pager that does exactly
// what messagix.Client.ExecuteTasks does below the request encoding (makeLSRequest: DoOneOffStream, then
// PublishResponseData.Parse). The reply is a real lightspeed payload.
//
// It pins the production behaviour, which the helper cannot change: the library returns only the stream's first data
// frame to ExecuteTasks; a second frame on the same stream is dropped by OneOffStream.receiveFrame, and a frame after
// the server ended the stream is dropped by the socket's read loop. Neither reaches the event handler, so the page's
// watch never sees the range they carry and the walk stops 2 after page 1, exactly as on 55b3df5. What the helper
// does is count each drop (oneoff_extra_frames, oneoff_unknown_stream_frames), so the installed counts say when the
// range went this way. The late-range wait covers live frames only.
func TestSecondFrameOnTheOneOffStreamIsDroppedAndCounted(t *testing.T) {
	for name, tc := range map[string]struct {
		endFirst             bool // the server ends the stream after the reply, before the second frame
		wantExtra, wantUnkwn int
	}{
		"second frame on the open stream":  {wantExtra: 1},
		"second frame after end of stream": {endFirst: true, wantUnkwn: 1},
	} {
		t.Run(name, func(t *testing.T) {
			withRangeWait(t, 300*time.Millisecond)
			s := newTestSession()
			reply := tpage(101, 5000)                          // page 1: its thread, no range
			second := rangeOnly(rangeRow(true, -1, 101, 5000)) // the range, on a second frame of the same stream
			srv := newDGWServer(t, lsPublish(t, reply), lsPublish(t, second), tc.endFirst)
			pager := dialDGW(t, s, srv)
			res, took := runOneOff(t, s, pager)
			if res.Gap != gapInboxIncomplete || res.Stats["inbox_stop"] != 2 || res.Stats["inbox_pages"] != 1 {
				t.Fatalf("gap=%q stop=%d pages=%d, want the walk to stop 2 after page 1; %v", res.Gap, res.Stats["inbox_stop"], res.Stats["inbox_pages"], res.Stats)
			}
			if !srv.sentSecond() {
				t.Fatal("the server never sent the second frame")
			}
			if res.Stats["oneoff_extra_frames"] != tc.wantExtra || res.Stats["oneoff_unknown_stream_frames"] != tc.wantUnkwn {
				t.Fatalf("drop counts = %d, %d; want %d, %d", res.Stats["oneoff_extra_frames"], res.Stats["oneoff_unknown_stream_frames"], tc.wantExtra, tc.wantUnkwn)
			}
			if res.Stats["inbox_rows_late"] != 0 || res.Stats["inbox_rows_total"] != 0 || res.Stats["inbox_pages_late"] != 0 {
				t.Fatalf("the dropped frame's range reached the walk: %v", res.Stats)
			}
			if took < 300*time.Millisecond {
				t.Fatalf("walk ended after %v, before the wait budget", took)
			}
		})
	}
}

// oneOffPager is the listing's client over a real dgw.Socket: what messagix.Client.ExecuteTasks does for a request,
// minus building the request payload (the server does not read it). Other scopes' first pages are answered empty.
type oneOffPager struct {
	sock *dgw.Socket
	done func() // called after the inbox request's reply was taken, as ExecuteTasks returns
}

func (p *oneOffPager) GetCursor(int64) string { return "" }

func (p *oneOffPager) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	if !isInboxTask(tasks[0]) {
		return &table.LSTable{}, nil
	}
	resp, err := p.sock.DoOneOffStream(ctx, []byte(`{"request_id":1,"type":3}`), false)
	if err != nil {
		return nil, err
	}
	var prd messagix.PublishResponseData
	if err := json.Unmarshal(resp, &prd); err != nil {
		return nil, err
	}
	tbl, err := prd.Parse(ctx)
	if err == nil {
		p.done()
	}
	return tbl, err
}

func runOneOff(t *testing.T, s *messagixSession, p *oneOffPager) (listingDone, time.Duration) {
	t.Helper()
	start := time.Now()
	_, res, err, done := listGap(t, s, p, context.Background(), 10*time.Second)
	if !done || err != nil {
		t.Fatalf("done=%v err=%v", done, err)
	}
	return res, time.Since(start)
}

// dgwServer answers the first one-off stream it is asked on with reply, then, once the client has taken the reply
// (released by the pager), with second on the same stream id. With endFirst it ends the stream before second.
type dgwServer struct {
	*httptest.Server
	release chan struct{}
	second  chan struct{}
}

func (d *dgwServer) sentSecond() bool {
	select {
	case <-d.second:
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

func newDGWServer(t *testing.T, reply, second []byte, endFirst bool) *dgwServer {
	t.Helper()
	d := &dgwServer{release: make(chan struct{}, 1), second: make(chan struct{})}
	d.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		ctx := r.Context()
		write := func(frames ...dgw.Frame) {
			var b []byte
			for _, f := range frames {
				b = f.MarshalAppend(b)
			}
			_ = c.Write(ctx, websocket.MessageBinary, b)
		}
		answered := false
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			for len(data) > 0 {
				f := dgw.CheckFrameType(data)
				if data, err = f.Unmarshal(data); err != nil {
					return
				}
				switch f := f.(type) {
				case *dgw.EstablishStreamFrame:
					write(&dgw.EstablishStreamFrame{StreamID: f.StreamID, RawParameters: json.RawMessage(`{"code":200}`)})
				case *dgw.DataFrame:
					if f.RequiresAck {
						write(&dgw.AckFrame{StreamID: f.StreamID, AckID: f.AckID})
					}
					if answered {
						continue
					}
					answered = true
					id := f.StreamID
					write(&dgw.DataFrame{StreamID: id, Payload: reply})
					go func() {
						<-d.release
						if endFirst {
							write(&dgw.EndOfDataFrame{StreamID: id})
						}
						write(&dgw.DataFrame{StreamID: id, Payload: second})
						close(d.second)
					}()
				}
			}
		}
	}))
	t.Cleanup(d.Close)
	return d
}

// dialDGW connects a real dgw.Socket, logging through the session's library logger as the session's client does
// (session.go: messagix.NewClient(..., s.libraryLogger(), ...), whose socket logs on a derived logger).
func dialDGW(t *testing.T, s *messagixSession, srv *dgwServer) *oneOffPager {
	t.Helper()
	sock := dgw.NewSocket(dgw.SocketOptions{
		GetCookies: func() string { return "" },
		OnConnect:  func(context.Context, func(error)) error { return nil },
		WSURL:      "ws" + strings.TrimPrefix(srv.URL, "http"),
		Log:        s.libraryLogger().With().Str("socket", "main").Logger(),
		Facebook:   true,
	})
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); _ = sock.Connect(ctx) }()
	t.Cleanup(func() { cancel(); sock.Disconnect(); <-stopped })
	deadline := time.Now().Add(5 * time.Second)
	for !sock.IsConnected() {
		if time.Now().After(deadline) {
			t.Fatal("dgw socket did not connect")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return &oneOffPager{sock: sock, done: func() {
		select {
		case srv.release <- struct{}{}:
		default:
		}
	}}
}

// lsPublish encodes tbl as the server's answer to a one-off request: a PublishResponseData whose payload is a
// lightspeed block of stored-procedure calls, one per row, each row's fields at their table index (int64 as
// I64_FROM_STRING), as PublishResponseData.Parse decodes it.
func lsPublish(t *testing.T, tbl *table.LSTable) []byte {
	t.Helper()
	spByTable := map[string]string{}
	for sp, name := range table.SPTable {
		spByTable[name] = sp
	}
	steps := []any{1}
	var deps []string
	v := reflect.ValueOf(tbl).Elem()
	for i := 0; i < v.NumField(); i++ {
		rows := v.Field(i)
		if rows.Kind() != reflect.Slice || rows.Len() == 0 {
			continue
		}
		sp, ok := spByTable[v.Type().Field(i).Name]
		if !ok {
			t.Fatalf("no stored procedure for %s", v.Type().Field(i).Name)
		}
		deps = append(deps, sp)
		for j := 0; j < rows.Len(); j++ {
			row := rows.Index(j).Elem()
			args := []any{}
			for k := 0; k < row.NumField(); k++ {
				idx, err := strconv.Atoi(row.Type().Field(k).Tag.Get("index"))
				if err != nil {
					continue
				}
				for len(args) <= idx {
					args = append(args, nil)
				}
				switch f := row.Field(k); f.Kind() {
				case reflect.Int64:
					args[idx] = []any{19, strconv.FormatInt(f.Int(), 10)}
				case reflect.String:
					args[idx] = f.String()
				case reflect.Bool:
					args[idx] = f.Bool()
				}
			}
			steps = append(steps, append([]any{5, sp}, args...))
		}
	}
	payload, err := json.Marshal(map[string]any{"name": nil, "step": steps})
	if err != nil {
		t.Fatal(err)
	}
	out, err := json.Marshal(messagix.PublishResponseData{RequestID: 1, Payload: string(payload), Sp: deps})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// The encoding above is what Parse reads back: the test's reply and second frame are the tables they claim to be.
func TestLSPublishRoundTrips(t *testing.T) {
	for _, tbl := range []*table.LSTable{tpage(101, 5000), rangeOnly(rangeRow(true, -1, 101, 5000))} {
		var prd messagix.PublishResponseData
		if err := json.Unmarshal(lsPublish(t, tbl), &prd); err != nil {
			t.Fatal(err)
		}
		got, err := prd.Parse(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(got.LSDeleteThenInsertThread) != len(tbl.LSDeleteThenInsertThread) || len(got.LSUpsertSyncGroupThreadsRange) != len(tbl.LSUpsertSyncGroupThreadsRange) {
			t.Fatalf("parsed %+v, want %+v", got, tbl)
		}
		for i, th := range tbl.LSDeleteThenInsertThread {
			g := got.LSDeleteThenInsertThread[i]
			if g.ThreadKey != th.ThreadKey || g.LastActivityTimestampMs != th.LastActivityTimestampMs || g.FolderName != th.FolderName || g.ThreadType != th.ThreadType {
				t.Fatalf("thread %+v, want %+v", g, th)
			}
		}
		for i, r := range tbl.LSUpsertSyncGroupThreadsRange {
			g := got.LSUpsertSyncGroupThreadsRange[i]
			if g.SyncGroup != r.SyncGroup || g.ParentThreadKey != r.ParentThreadKey || g.HasMoreBefore != r.HasMoreBefore || g.MinThreadKey != r.MinThreadKey || g.MinLastActivityTimestampMS != r.MinLastActivityTimestampMS {
				t.Fatalf("range %+v, want %+v", got.LSUpsertSyncGroupThreadsRange[i], r)
			}
		}
	}
}
