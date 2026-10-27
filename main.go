// SPDX-License-Identifier: AGPL-3.0-or-later
// Kluro Messenger protocol sidecar: speaks the line protocol in protocol.go
// on stdin/stdout and drives mautrix-meta's messagix underneath. See NOTICE.md
// for the library this program builds on.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// backend is the Messenger implementation the loop drives. The messagix-backed
// session implements it; tests use a fake. All methods may be called from the
// loop goroutine only. Live events (thread/message/edit/unsend/state) are
// emitted through the emit callback handed to Start, from any goroutine.
type backend interface {
	// Start signs in with cookies and restores state; it returns the ready
	// answer and the initial thread events to emit right after it. acks is
	// the hub whose wait blocks an event until the connector commits it.
	Start(ctx context.Context, emit emitFunc, acks *ackHub, cookies map[string]string, state []byte, mode, platform string) (Ready, []Frame, error)
	// HistoryPIN restores the secure-storage enrollment. It returns the new
	// history state, or an error: *authErr for a wrong/locked PIN.
	HistoryPIN(ctx context.Context, pin []byte) (HistoryState, error)
	// Threads pages the full inbox; thread events go to emit as found.
	Threads(ctx context.Context, emit emitFunc) error
	// History returns one page of a thread, newest first.
	History(ctx context.Context, thread, cursor string, count int) (*Page, error)
	// Send sends one text; it never retries internally.
	Send(ctx context.Context, thread, otid, text string) (*Sent, error)
	// Logout ends the session for good: best-effort provider sign-out and
	// local device deletion, then everything in memory is wiped. Messenger
	// clients have no server-side device de-registration operation.
	Logout(ctx context.Context) error
	Stop()
}

// stderr is where logCode writes; tests swap it to capture the log lines.
var stderr io.Writer = os.Stderr
var logMu sync.Mutex

// logCode is the sidecar's only stderr output: one JSON line carrying a level
// and a fixed snake_case code, nothing else. Provider errors can contain
// message content, so no error text or extra field is ever logged.
func logCode(level, code string) {
	b, err := json.Marshal(map[string]string{"level": level, "code": code})
	if err != nil {
		return
	}
	logMu.Lock()
	defer logMu.Unlock()
	_, _ = stderr.Write(append(b, '\n'))
}

// Ready is the answer to start and history_pin.
type Ready struct {
	Self    string
	Name    string
	History HistoryState
}

func main() {
	code := func() (code int) {
		defer func() {
			// The panic value never reaches stderr: it can carry content.
			if recover() != nil {
				logCode("error", "panic")
				code = 1
			}
		}()
		return run(os.Stdin, os.Stdout, func() backend { return newSession() })
	}()
	os.Exit(code)
}

type command struct {
	Frame
	ctx    context.Context
	cancel context.CancelFunc
}

func run(r io.Reader, w io.Writer, mkBackend func() backend) int {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acks := newAckHub()
	defer acks.close()
	em := newEmitter(w, acks)
	b := mkBackend()
	var commandMu sync.Mutex
	pending := map[int64]context.CancelFunc{}
	var listingCancel context.CancelFunc
	terminating := false
	incoming := make(chan command)
	commands := make(chan command)
	go func() {
		defer close(commands)
		var queue []command
		for incoming != nil || len(queue) > 0 {
			var out chan command
			var next command
			if len(queue) > 0 {
				out, next = commands, queue[0]
			}
			select {
			case <-ctx.Done():
				return
			case f, ok := <-incoming:
				if !ok {
					incoming = nil
				} else {
					queue = append(queue, f)
				}
			case out <- next:
				queue = queue[1:]
			}
		}
	}()
	go func() {
		defer close(incoming)
		defer acks.close()
		sc := bufio.NewScanner(r)
		sc.Buffer(make([]byte, 64<<10), maxLine)
		for sc.Scan() {
			var f Frame
			if err := json.Unmarshal(sc.Bytes(), &f); err != nil || f.T == "" {
				logCode("warn", "bad_frame")
				continue
			}
			if f.T == TAck {
				if f.Seq <= 0 {
					logCode("warn", "bad_ack")
				} else {
					acks.ack(f.Seq)
				}
				continue
			}
			if f.T == TCancel {
				commandMu.Lock()
				if cancel := pending[f.Target]; cancel != nil {
					cancel()
				}
				commandMu.Unlock()
				continue
			}
			cmdCtx, cancelCmd := context.WithCancel(ctx)
			cmd := command{Frame: f, ctx: cmdCtx, cancel: cancelCmd}
			commandMu.Lock()
			pending[f.ID] = cancelCmd
			commandMu.Unlock()
			if f.T == TStop || f.T == TLogout {
				// Read termination out of band, just like acks. The backend
				// remains serial, but a listing cannot hide the command.
				commandMu.Lock()
				terminating = true
				if listingCancel != nil {
					listingCancel()
				}
				commandMu.Unlock()
			}
			select {
			case incoming <- cmd:
			case <-ctx.Done():
				return
			}
			if f.T == TStop || f.T == TLogout {
				return // terminal commands need no further input
			}
		}
		if err := sc.Err(); err != nil {
			logCode("warn", "read_failed")
		}
	}()
	started := false
	logoutOnly := false
	handle := func(cmd command) int {
		f, ctx := cmd.Frame, cmd.ctx
		if ctx.Err() != nil {
			em.emit(Frame{T: TError, ID: f.ID, Err: "cancelled"})
			return -1
		}
		if started && logoutOnly && f.T != TLogout && f.T != TStop {
			em.emit(Frame{T: TError, ID: f.ID, Err: "bad_request"})
			return -1
		}
		switch f.T {
		case TStart:
			if started || (f.Mode != "" && f.Mode != "logout_only") {
				em.emit(Frame{T: TError, ID: f.ID, Err: "bad_request"})
				return -1
			}
			ready, initial, err := b.Start(ctx, em.emit, acks, f.Cookies, f.State, f.Mode, f.Platform)
			var auth *authErr
			if err != nil {
				logCode("warn", "start_failed")
				if errors.As(err, &auth) {
					em.emit(Frame{T: TAuth, ID: f.ID, Auth: auth.state})
					em.flush()
					return 2
				}
				em.emit(Frame{T: TError, ID: f.ID, Err: codeOf(err)})
				em.flush()
				return 1
			}
			started = true
			logoutOnly = f.Mode == "logout_only"
			em.emit(Frame{T: TReady, ID: f.ID, Self: ready.Self, Name: ready.Name, History: ready.History})
			for _, evt := range initial {
				evt.ID = 0
				em.emit(evt)
			}
		case THistoryPIN:
			if !started {
				em.emit(Frame{T: TError, ID: f.ID, Err: "not_started"})
				return -1
			}
			state, err := b.HistoryPIN(ctx, []byte(f.PIN))
			var auth *authErr
			switch {
			case err == nil:
				em.emit(Frame{T: TReady, ID: f.ID, History: state})
			case errors.As(err, &auth):
				em.emit(Frame{T: TAuth, ID: f.ID, Auth: auth.state})
			default:
				em.emit(Frame{T: TError, ID: f.ID, Err: codeOf(err)})
			}
		case TThreads:
			if !started {
				em.emit(Frame{T: TError, ID: f.ID, Err: "not_started"})
				return -1
			}
			listCtx, cancelList := context.WithCancel(ctx)
			commandMu.Lock()
			listingCancel = cancelList
			if terminating {
				cancelList()
			}
			commandMu.Unlock()
			err := b.Threads(listCtx, em.emit)
			if listCtx.Err() != nil {
				err = listCtx.Err()
			}
			commandMu.Lock()
			listingCancel = nil
			commandMu.Unlock()
			cancelList()
			if err != nil {
				em.emit(Frame{T: TError, ID: f.ID, Err: codeOf(err)})
				return -1
			}
			em.emit(Frame{T: TThreadsDone, ID: f.ID})
		case THistory:
			if !started {
				em.emit(Frame{T: TError, ID: f.ID, Err: "not_started"})
				return -1
			}
			page, err := b.History(ctx, f.Thread, f.Cursor, f.Count)
			if err != nil {
				em.emit(Frame{T: TError, ID: f.ID, Err: codeOf(err)})
				return -1
			}
			em.emit(Frame{T: TPage, ID: f.ID, Page: page})
		case TSend:
			if !started {
				em.emit(Frame{T: TError, ID: f.ID, Err: "not_started"})
				return -1
			}
			sent, err := b.Send(ctx, f.Thread, f.OTID, f.Text)
			if err != nil {
				em.emit(Frame{T: TError, ID: f.ID, Err: codeOf(err)})
				return -1
			}
			em.emit(Frame{T: TSent, ID: f.ID, Sent: sent})
		case TLogout:
			if !started {
				em.emit(Frame{T: TError, ID: f.ID, Err: "not_started"})
				return -1
			}
			acks.close()
			if err := b.Logout(ctx); err != nil {
				logCode("warn", "logout_failed")
				em.emit(Frame{T: TError, ID: f.ID, Err: "logout_failed"})
			} else {
				em.emit(Frame{T: TLoggedOut, ID: f.ID})
			}
			em.flush()
			acks.close()
			b.Stop()
			return 0
		case TStop:
			// The backend's Stop flushes a final state frame before it wipes
			// its stores, so the connector seals everything first.
			acks.close()
			b.Stop()
			em.flush()
			return 0
		default:
			em.emit(Frame{T: TError, ID: f.ID, Err: "bad_request"})
		}
		return -1
	}
	for cmd := range commands {
		code := handle(cmd)
		commandMu.Lock()
		delete(pending, cmd.ID)
		commandMu.Unlock()
		cmd.cancel()
		if code >= 0 {
			return code
		}
	}

	// EOF behaves like stop.
	acks.close()
	b.Stop()
	em.flush()
	return 0
}

// emitter writes protocol frames to stdout, one JSON object per line, and
// nothing else ever touches stdout.
type emitter struct {
	mu    sync.Mutex
	w     *bufio.Writer
	stamp func(*Frame) int64
	drop  func(int64)
}

func newEmitter(w io.Writer, acks *ackHub) *emitter {
	return &emitter{w: bufio.NewWriterSize(w, 64<<10), stamp: acks.stamp, drop: acks.discard}
}

// emit writes one frame and returns the seq it stamped on state, message,
// edit or unsend, 0 for anything else. Stamping happens under the write lock,
// so the seq order is the emission order.
func (e *emitter) emit(f Frame) int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	var seq int64
	if e.stamp != nil {
		seq = e.stamp(&f)
	}
	b, err := json.Marshal(f)
	if err != nil {
		logCode("error", "marshal_failed")
		return e.fail(seq)
	}
	if len(b) > maxLine {
		logCode("error", "frame_too_large")
		return e.fail(seq)
	}
	if _, err := e.w.Write(append(b, '\n')); err != nil {
		logCode("error", "write_failed")
		return e.fail(seq)
	}
	if err := e.w.Flush(); err != nil {
		logCode("error", "flush_failed")
		return e.fail(seq)
	}
	return seq
}

func (e *emitter) fail(seq int64) int64 {
	if seq > 0 && e.drop != nil {
		e.drop(seq)
	}
	return -1
}

func frameFits(f Frame) bool {
	b, err := json.Marshal(f)
	return err == nil && len(b) <= maxLine
}

func (e *emitter) flush() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := e.w.Flush(); err != nil {
		logCode("error", "flush_failed")
	}
}

// authErr marks a failure the user must resolve by signing in again or fixing
// the PIN; the loop answers it with an auth frame instead of an error code.
type authErr struct{ state AuthState }

func (e *authErr) Error() string { return fmt.Sprintf("messenger auth: %s", e.state) }

// codeErr is a failure with a fixed protocol code.
type codeErr struct{ code string }

func (e *codeErr) Error() string { return "messenger: " + e.code }

func errCode(code string) error { return &codeErr{code: code} }

// codeOf maps any error to a fixed code; provider text never leaves the sidecar.
func codeOf(err error) string {
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	var ce *codeErr
	if errors.As(err, &ce) {
		return ce.code
	}
	return "unknown"
}
