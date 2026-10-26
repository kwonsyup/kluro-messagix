// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"sync"
	"time"
)

// ackWait is how long one event waits for the connector's durable-commit ack
// before the event is handed back undelivered. Tests shorten it.
var ackWait = 60 * time.Second

// ackHub pairs the seq the sidecar stamps on state, message, edit and unsend
// with the connector's "ack" answers. wait blocks until the ack for one seq
// arrives, the wait times out, or the protocol loop has ended.
type ackHub struct {
	mu      sync.Mutex
	next    int64
	waiting map[int64]chan struct{}
	acked   map[int64]bool
	closed  bool
}

func newAckHub() *ackHub {
	return &ackHub{waiting: map[int64]chan struct{}{}, acked: map[int64]bool{}}
}

// stamp assigns the next strictly increasing seq to a message-bearing frame
// and returns it; any other frame is left untouched and returns 0. It runs
// under the emitter's write lock, so seq order is emission order.
func (h *ackHub) stamp(f *Frame) int64 {
	switch f.T {
	case TState, TMessage, TEdit, TUnsend:
	default:
		return 0
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.next++
	f.Seq = h.next
	if f.waitAck && !h.closed {
		// Register before the emitter writes the frame. The connector may
		// commit and ack it before the event goroutine reaches wait.
		h.waiting[f.Seq] = make(chan struct{})
	}
	return f.Seq
}

// ack records the connector's durable commit of one event and wakes its
// waiter. An ack for an unknown seq is nothing.
func (h *ackHub) ack(seq int64) {
	if seq <= 0 {
		return
	}
	h.mu.Lock()
	ch, ok := h.waiting[seq]
	if ok && !h.acked[seq] {
		close(ch)
		h.acked[seq] = true
	}
	h.mu.Unlock()
}

func (h *ackHub) discard(seq int64) {
	h.mu.Lock()
	delete(h.waiting, seq)
	delete(h.acked, seq)
	h.mu.Unlock()
}

// wait blocks until the connector acks seq. It reports false when the wait
// timed out or the protocol loop ended: the event was not committed, so it
// must be delivered again rather than acknowledged to the server.
func (h *ackHub) wait(seq int64) bool {
	return h.waitContext(context.Background(), seq)
}

func (h *ackHub) waitContext(ctx context.Context, seq int64) bool {
	h.mu.Lock()
	if h.acked[seq] {
		delete(h.waiting, seq)
		delete(h.acked, seq)
		h.mu.Unlock()
		return true
	}
	if h.closed {
		delete(h.waiting, seq)
		delete(h.acked, seq)
		h.mu.Unlock()
		return false
	}
	ch := h.waiting[seq]
	if ch == nil {
		// Defensive path for callers that did not use the emitter's stamp
		// callback. Normal message events are registered before they are sent.
		ch = make(chan struct{})
		h.waiting[seq] = ch
	}
	h.mu.Unlock()
	timer := time.NewTimer(ackWait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		h.discard(seq)
		return false
	case <-ch:
		h.mu.Lock()
		acked := h.acked[seq]
		delete(h.waiting, seq)
		delete(h.acked, seq)
		h.mu.Unlock()
		return acked
	case <-timer.C:
		h.mu.Lock()
		if h.waiting[seq] == ch {
			delete(h.waiting, seq)
			delete(h.acked, seq)
		}
		h.mu.Unlock()
		logCode("warn", "ack_timeout")
		return false
	}
}

// close wakes every waiter as failed: the connector is gone, so nothing more
// can be committed.
func (h *ackHub) close() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return
	}
	h.closed = true
	for seq, ch := range h.waiting {
		if h.acked[seq] {
			continue
		}
		close(ch)
		delete(h.waiting, seq)
	}
}
