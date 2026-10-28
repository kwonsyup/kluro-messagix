// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

// silentRanges is a client for which no group-95 range update ever arrives:
// the waits honour only their context, as the library's do.
type silentRanges struct {
	more         bool // the keystore reports more encrypted threads
	initialReady bool // the initial range wait returns at once
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
			if err != nil {
				t.Fatalf("listing must end cleanly so threads_done is emitted, got %v", err)
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
	if !done || err != nil {
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
