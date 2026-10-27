// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
)

// The sidecar protocol: one JSON object per line in each direction over the
// sidecar's stdin/stdout, so the connector and the AGPL-licensed Messenger
// protocol program stay separate programs. The connector owns persistence,
// checkpoints, retries and evidence; the sidecar owns only Messenger's wire
// protocol and encrypted-history cryptography. Every message carries "t".
//
// connector -> sidecar (commands; ID is echoed by the answer):
//   start        Cookies, State, Mode, Platform  connect; optional Mode "logout_only"
//                                      loads only web logout state, registers
//                                      nothing; accepts only logout and stop
//   history_pin  PIN                   restore encrypted-history enrollment
//   threads                            re-list threads (answer: thread events, then threads_done)
//   history      Thread, Cursor, Count one page of a thread, newest first
//   send         Thread, OTID, Text    send text (answer: sent or error)
//   logout                             sign out, wipe session state and exit
//                                      (answer: logged_out or error logout_failed)
//   stop                               disconnect, flush final state and exit
//   cancel       Target                cancel a running or queued command; no answer
//   ack          Seq                   the connector durably committed the
//                                      state, message, edit or unsend with
//                                      that seq (including durable no-ops)
//
// sidecar -> connector (events):
//   ready        Self, Name, History   connected; History is a HistoryState
//   state        State, Seq            durable sidecar state changed: seal and ack
//   health       Code, Bytes           fixed health code and state byte count; no ack
//   thread       Thread                a thread exists / changed
//   threads_done                       the answer to "threads" is complete
//   message      Message, Seq          a live message
//   edit         Edit, Seq             a live edit
//   unsend       Unsend, Seq           a live unsend
//   page         Page                  the answer to "history"
//   sent         Sent                  the answer to "send"
//   logged_out                         the answer to "logout": the session is gone
//   auth         Auth                  the session needs the user (AuthState)
//   error        Err                   the answer to a command failed, or fatal
//
// Every state, message, edit and unsend event carries "seq": a per-process counter
// that strictly increases in emission order. The connector answers each with
// "ack" once it has durably committed that event. Before it commits and acks
// an encrypted event, the connector must also seal any preceding state
// frame; that frame contains the ratchet state needed to restore and retry
// the event. The sidecar's encrypted-chat handler does not acknowledge the
// event to Messenger until then, so the server redelivers an event the
// connector never committed instead of the sidecar dropping it. Lightspeed
// events carry a seq but are not gated, as the lightspeed socket offers no
// redelivery and the connector's head checkpoint reconciles them by paging
// the newest page on start. State acks block only secure-history enrollment:
// before AddDevice the sidecar waits up to 60 seconds for its state ack.
//
// Start Platform is "facebook" or "messenger"; absent or empty defaults to
// "messenger". Other values and state from another platform are refused.
// Legacy state without a platform belongs to Messenger.
//
// A sidecar exit without "stop" is a crash: the connector restarts it from the
// sealed state and resumes from its own checkpoints.

// HistoryState is the encrypted-history enrollment, separate from sign-in.
type HistoryState string

const (
	HistoryNone     HistoryState = "none"      // the account has no secure-storage backup
	HistoryNeedsPIN HistoryState = "needs_pin" // a backup exists and the PIN restores it
	HistoryEnrolled HistoryState = "enrolled"  // encrypted history can be paged
)

// AuthState is why the session cannot continue by itself.
type AuthState string

const (
	AuthInvalid    AuthState = "invalid"    // the session was rejected: sign in again
	AuthCheckpoint AuthState = "checkpoint" // Facebook wants a security check in the browser
	AuthPINWrong   AuthState = "pin_wrong"
	AuthPINLocked  AuthState = "pin_locked" // no PIN attempts remain
)

type Thread struct {
	ID    string `json:"id"`
	Typed bool   `json:"typed,omitempty"` // type learned from Messenger data
	Kind  string `json:"kind"`            // "user" (one-to-one) or "group"
	Title string `json:"title,omitempty"`
	// Encrypted marks an end-to-end encrypted thread. Predecessor is the
	// pre-encryption thread this one continues, when Messenger links them: both
	// are the same conversation and history continues into it.
	Encrypted    bool     `json:"encrypted,omitempty"`
	Predecessor  string   `json:"predecessor,omitempty"`
	Participants []Person `json:"participants,omitempty"`
	LastActivity int64    `json:"last_activity_ms,omitempty"`
}

type Person struct {
	ID   string `json:"id"`
	Name string `json:"name,omitempty"`
	Self bool   `json:"self,omitempty"`
}

type Message struct {
	Thread     string `json:"thread"`
	ID         string `json:"id"`
	Sender     string `json:"sender"`
	SenderName string `json:"sender_name,omitempty"`
	Out        bool   `json:"out,omitempty"`
	Text       string `json:"text,omitempty"`
	Media      string `json:"media,omitempty"` // kind only: photo, video, audio, file, sticker, link
	ReplyTo    string `json:"reply_to,omitempty"`
	SentMS     int64  `json:"sent_ms"`
	OTID       string `json:"otid,omitempty"` // the offline threading id a send used, when echoed
	Encrypted  bool   `json:"encrypted,omitempty"`
}

type Edit struct {
	Thread string `json:"thread"`
	ID     string `json:"id"`
	By     string `json:"by"` // the editor; applied only if it is the stored author
	Text   string `json:"text"`
	AtMS   int64  `json:"at_ms"`
}

type Unsend struct {
	Thread string `json:"thread"`
	ID     string `json:"id"`
	By     string `json:"by,omitempty"`
}

// Page is one page of a thread, newest first. Cursor is opaque to the
// connector and handed back unchanged. Complete says the sidecar reached the
// start of everything it can read for this thread; Gap, when set, is why the
// thread's readable history ends early (a fixed string, never content).
type Page struct {
	Thread   string    `json:"thread"`
	Messages []Message `json:"messages"`
	Next     string    `json:"next,omitempty"`
	Complete bool      `json:"complete,omitempty"`
	Gap      string    `json:"gap,omitempty"`
	// Next thread to continue into, when this thread's own history ends at a
	// linked predecessor (encrypted thread -> its pre-encryption thread).
	Continue string `json:"continue,omitempty"`
}

type Sent struct {
	Thread    string `json:"thread"`
	MessageID string `json:"message_id,omitempty"`
	OTID      string `json:"otid,omitempty"`
}

// Frame is the one wire shape; only the fields of its type are set.
type Frame struct {
	T  string `json:"t"`
	ID int64  `json:"id,omitempty"`
	// Seq identifies one state, message, edit or unsend; the connector's ack
	// for it means the event is durably committed. Strictly increasing.
	Seq int64 `json:"seq,omitempty"`
	// waitAck is local routing metadata. Encrypted events and enrollment state
	// register a blocking ack waiter; ordinary state emission does not block.
	waitAck bool `json:"-"`

	// commands
	Target   int64             `json:"target,omitempty"`
	Cookies  map[string]string `json:"cookies,omitempty"`
	Mode     string            `json:"mode,omitempty"`
	Platform string            `json:"platform,omitempty"`
	State    []byte            `json:"state,omitempty"`
	PIN      string            `json:"pin,omitempty"`
	Thread   string            `json:"thread,omitempty"`
	Cursor   string            `json:"cursor,omitempty"`
	Count    int               `json:"count,omitempty"`
	OTID     string            `json:"otid,omitempty"`
	Text     string            `json:"text,omitempty"`

	// events
	Self      string       `json:"self,omitempty"`
	Name      string       `json:"name,omitempty"`
	History   HistoryState `json:"history,omitempty"`
	ThreadEvt *Thread      `json:"thread_evt,omitempty"`
	Message   *Message     `json:"message,omitempty"`
	Edit      *Edit        `json:"edit,omitempty"`
	Unsend    *Unsend      `json:"unsend,omitempty"`
	Page      *Page        `json:"page,omitempty"`
	Sent      *Sent        `json:"sent,omitempty"`
	Auth      AuthState    `json:"auth,omitempty"`
	Err       string       `json:"err,omitempty"`   // a fixed code, never provider text
	Code      string       `json:"code,omitempty"`  // health: a fixed code
	Bytes     int          `json:"bytes,omitempty"` // health: unencoded state bytes
}

// Frame types.
const (
	TStart, THistoryPIN, TThreads, THistory, TSend, TStop, TLogout = "start", "history_pin", "threads", "history", "send", "stop", "logout"
	TAck, TCancel                                                  = "ack", "cancel"
	THealth                                                        = "health"
	TReady, TState, TThread, TThreadsDone, TMessage                = "ready", "state", "thread", "threads_done", "message"
	TEdit, TUnsend, TPage, TSent, TAuth, TError, TLoggedOut        = "edit", "unsend", "page", "sent", "auth", "error", "logged_out"
)

// emitFunc writes one frame to the connector. It returns the seq stamped on
// the frame when it is state, message, edit or unsend, 0 otherwise.
type emitFunc func(Frame) int64

// maxLine bounds one frame; a history page of text fits well inside it.
const maxLine = 16 << 20

// ErrClosed is returned by Call once the sidecar's output has ended.
var ErrClosed = errors.New("messenger sidecar closed")

// Conn is the connector's end of the protocol. Events that answer no command
// go to the handler in order, on one goroutine; Call returns the answer to one
// command (the frame with its ID) and a *CodeError for an "error" answer.
type Conn struct {
	w   io.Writer
	wmu sync.Mutex

	mu      sync.Mutex
	next    int64
	waiting map[int64]chan Frame
	closed  bool
	done    chan struct{}
	err     error
}

// CodeError is a command the sidecar refused, with its fixed code.
type CodeError struct{ Code string }

func (e *CodeError) Error() string { return "messenger sidecar: " + e.Code }

// NewConn starts reading r; every frame without a waiting command goes to
// onEvent. It returns when r ends; Done reports that.
func NewConn(r io.Reader, w io.Writer, onEvent func(Frame)) *Conn {
	c := &Conn{w: w, waiting: map[int64]chan Frame{}, done: make(chan struct{})}
	go c.read(r, onEvent)
	return c
}

func (c *Conn) read(r io.Reader, onEvent func(Frame)) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 64<<10), maxLine)
	for sc.Scan() {
		var f Frame
		if json.Unmarshal(sc.Bytes(), &f) != nil || f.T == "" {
			continue
		}
		c.mu.Lock()
		ch, ok := c.waiting[f.ID]
		if ok && f.ID != 0 && answers(f.T) {
			delete(c.waiting, f.ID)
		} else {
			ok = false
		}
		c.mu.Unlock()
		if ok {
			ch <- f
			continue
		}
		onEvent(f)
	}
	c.mu.Lock()
	c.closed = true
	c.err = sc.Err()
	for id, ch := range c.waiting {
		close(ch)
		delete(c.waiting, id)
	}
	c.mu.Unlock()
	close(c.done)
}

// answers reports whether a frame type can be a command's answer.
func answers(t string) bool {
	switch t {
	case TReady, TThreadsDone, TPage, TSent, TError, TAuth, TLoggedOut:
		return true
	}
	return false
}

// Done is closed when the sidecar's output has ended.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Send writes one command that has no answer (stop, ack or cancel).
func (c *Conn) Send(f Frame) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.w.Write(append(b, '\n'))
	return err
}

// Call sends a command and waits for its answer.
func (c *Conn) Call(ctx context.Context, f Frame) (Frame, error) {
	ch := make(chan Frame, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return Frame{}, ErrClosed
	}
	c.next++
	f.ID = c.next
	c.waiting[f.ID] = ch
	c.mu.Unlock()
	if err := c.Send(f); err != nil {
		c.mu.Lock()
		delete(c.waiting, f.ID)
		c.mu.Unlock()
		return Frame{}, err
	}
	select {
	case ans, ok := <-ch:
		switch {
		case !ok:
			return Frame{}, ErrClosed
		case ans.T == TError:
			return ans, &CodeError{Code: ans.Err}
		}
		return ans, nil
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.waiting, f.ID)
		c.mu.Unlock()
		return Frame{}, fmt.Errorf("messenger sidecar call %s: %w", f.T, ctx.Err())
	}
}
