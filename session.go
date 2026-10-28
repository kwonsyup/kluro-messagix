// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/store"
	waTypes "go.mau.fi/whatsmeow/types"

	"go.mau.fi/mautrix-meta/pkg/messagix"
	"go.mau.fi/mautrix-meta/pkg/messagix/cookies"
	"go.mau.fi/mautrix-meta/pkg/messagix/dgw"
	"go.mau.fi/mautrix-meta/pkg/messagix/httpclient"
	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
	"go.mau.fi/mautrix-meta/pkg/messagix/types"
)

// logoutURL is the web session's logout endpoint the best-effort web logout
// posts to with the session's fb_dtsg.
const logoutURL = "https://www.facebook.com/logout.php"

// messagixSession is the messagix-backed backend: it owns the lightspeed
// client, the E2EE (whatsmeow) client and the secure-history enrollment, and
// keeps everything needed to resume as the same device in its state blob.
type messagixSession struct {
	emit emitFunc
	acks *ackHub

	ctx    context.Context
	cancel context.CancelFunc

	mu            sync.Mutex
	flushMu       sync.Mutex
	shutdownMu    sync.Mutex
	workWG        sync.WaitGroup
	e2eeConnectMu sync.Mutex
	e2eeStarting  bool
	stateLoopDone chan struct{}
	client        *messagix.Client
	e2ee          *whatsmeow.Client
	e2eeTry       bool // a connect attempt has been made
	waDevice      *store.Device
	waDeviceJID   string // saved device id, to find the device in the restored store
	waDB          *waDB
	waSnap        []byte // restored device-store snapshot, opened with the first e2ee connect
	histDev       *securehistory.Device
	histBak       *securehistory.Backup
	selfID        int64
	selfName      string

	threads      map[string]*threadInfo // by wire id
	participants map[int64][]int64      // participant rows waiting for their thread
	names        map[int64]string
	cutover      map[int64]int64   // encrypted chat id -> its pre-encryption (open) thread key
	fbToWA       map[int64]int64   // facebook thread key -> encrypted chat id (hybrid threads)
	rowKeys      map[int64]bool    // thread rows seen this run (counts only)
	encRowKeys   map[int64]bool    // ... of an encrypted-over-WA type
	msgIndex     map[string]string // message id -> wire thread id
	msgOrder     []string
	collectors   map[int64]*pageCollector
	lastFullRec  time.Time
	stopping     bool
	stopped      bool
	logoutOnly   bool
	platform     string

	lastState       time.Time
	lastStateBlob   []byte
	stateDirty      bool
	stateRetryAt    time.Time
	stateRetryDelay time.Duration
}

// threadInfo is the merged view of one thread; the wire Thread is built from it.
type threadInfo struct {
	Key          int64 // facebook thread key, or the encrypted chat id
	Enc          bool
	Kind         string // "user" or "group"
	Typed        bool
	Title        string
	LastActivity int64
	Participants []int64 // ordered, deduplicated
}

// wireID is the thread id on the protocol: the facebook thread key, or "e:" and
// the encrypted chat id.
func wireID(enc bool, key int64) string {
	if enc {
		return "e:" + strconv.FormatInt(key, 10)
	}
	return strconv.FormatInt(key, 10)
}

func parseWire(id string) (enc bool, key int64, ok bool) {
	if strings.HasPrefix(id, "e:") {
		enc, id = true, id[2:]
	}
	key, err := strconv.ParseInt(id, 10, 64)
	return enc, key, err == nil && key > 0
}

func newSession() *messagixSession {
	return &messagixSession{
		threads:      map[string]*threadInfo{},
		participants: map[int64][]int64{},
		names:        map[int64]string{},
		cutover:      map[int64]int64{},
		fbToWA:       map[int64]int64{},
		rowKeys:      map[int64]bool{},
		encRowKeys:   map[int64]bool{},
		msgIndex:     map[string]string{},
		collectors:   map[int64]*pageCollector{},
	}
}

// savedState is the sidecar's own durable state: the messagix connection state,
// the encrypted-chat device store (a snapshot of its SQL store) and device id,
// the secure-history enrollment and the thread facts that are not re-learned on
// reconnect (cutover and hybrid links).
type savedState struct {
	Platform      string                `json:"platform,omitempty"`
	Messagix      json.RawMessage       `json:"messagix,omitempty"`
	WADB          []byte                `json:"wa_db,omitempty"`
	WADevice      string                `json:"wa_device,omitempty"`
	HistoryDevice *securehistory.Device `json:"history_device,omitempty"`
	Cutover       map[int64]int64       `json:"cutover,omitempty"`
	FBToWA        map[int64]int64       `json:"fb_to_wa,omitempty"`
}

func (s *messagixSession) Start(ctx context.Context, emit emitFunc, acks *ackHub, cookieMap map[string]string, state []byte, mode, platform string) (Ready, []Frame, error) {
	if mode != "" && mode != "logout_only" {
		return Ready{}, nil, errCode("bad_request")
	}
	selected := types.Messenger
	switch platform {
	case "", "messenger":
		platform = "messenger"
	case "facebook":
		selected = types.Facebook
	default:
		return Ready{}, nil, errCode("bad_request")
	}
	var st savedState
	if len(state) > 0 {
		if err := json.Unmarshal(state, &st); err != nil {
			return Ready{}, nil, errCode("bad_request")
		}
		statePlatform := st.Platform
		if statePlatform == "" {
			statePlatform = "messenger"
		}
		if statePlatform != platform {
			return Ready{}, nil, errCode("bad_request")
		}
	}
	s.platform = platform
	s.logoutOnly = mode == "logout_only"
	s.emit = emit
	s.acks = acks
	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.stateLoopDone = make(chan struct{})
	go s.stateLoop()

	ck := &cookies.Cookies{Platform: selected}
	values := make(map[cookies.MetaCookieName]string, len(cookieMap))
	for k, v := range cookieMap {
		values[cookies.MetaCookieName(k)] = v
	}
	ck.UpdateValues(values)
	if missing := ck.GetMissingCookieNames(); len(missing) > 0 {
		return Ready{}, nil, errCode("bad_request")
	}
	selfID := ck.GetUserID()
	if selfID == 0 {
		return Ready{}, nil, errCode("bad_request")
	}
	s.selfID = selfID

	if len(state) > 0 {
		s.restore(&st)
	}

	// Library logs are discarded: they are not audited for sensitive content,
	// and stderr may carry only the sidecar's own level+code lines.
	cli := messagix.NewClient(ck, zerolog.Nop(), &messagix.Config{})
	if !s.logoutOnly {
		cli.SetEventHandler(s.handleMetaEvent)
	}
	s.mu.Lock()
	s.client = cli
	s.mu.Unlock()

	var currentUser types.UserInfo
	var initial *table.LSTable
	if len(st.Messagix) > 0 {
		if err := cli.LoadState(st.Messagix); err == nil {
			currentUser, _ = cli.GetCurrentAccount()
		} else {
			logCode("warn", "state_expired")
		}
	}
	if currentUser == nil {
		ui, tbl, err := cli.LoadMessagesPage(ctx)
		if err != nil {
			return Ready{}, nil, classifyStartError("load_page", err)
		}
		currentUser, initial = ui, tbl
	}
	if currentUser.GetFBID() == 0 {
		return Ready{}, nil, errCode("load_page_no_user")
	}
	s.selfName = currentUser.GetName()
	if s.logoutOnly {
		// The HTTP client and fb_dtsg are all web logout needs. No sockets,
		// secure-history lookup, device-store restore or registration.
		return Ready{Self: strconv.FormatInt(s.selfID, 10), Name: s.selfName}, nil, nil
	}

	// The initial table's threads are returned with the ready answer; its
	// messages are emitted as live (idempotent) once the loop is running.
	var frames []Frame
	if initial != nil {
		frames = s.handleTableFrames(initial)
	}
	if err := cli.Connect(s.ctx); err != nil {
		logCode("warn", "connect_failed")
		return Ready{}, nil, classifyStartError("connect", err)
	}
	s.scheduleE2EE()

	ready := Ready{Self: strconv.FormatInt(s.selfID, 10), Name: s.selfName, History: s.checkHistory(ctx)}
	s.markStateDirty()
	logCode("info", "session_started")
	return ready, frames, nil
}

// restore loads everything from a previous state blob except the messagix
// connection state, which is handed to the client after it is created.
func (s *messagixSession) restore(st *savedState) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.histDev = st.HistoryDevice
	s.waSnap = st.WADB
	s.waDeviceJID = st.WADevice
	for k, v := range st.Cutover {
		s.cutover[k] = v
	}
	for k, v := range st.FBToWA {
		s.fbToWA[k] = v
	}
}

// dump returns the state blob the sidecar would restore from right now.
func (s *messagixSession) dump() []byte {
	s.mu.Lock()
	st := savedState{HistoryDevice: s.histDev, WADevice: s.waDeviceJID}
	if s.platform == "facebook" {
		st.Platform = s.platform
	}
	if len(s.cutover) > 0 {
		st.Cutover = maps.Clone(s.cutover)
	}
	if len(s.fbToWA) > 0 {
		st.FBToWA = maps.Clone(s.fbToWA)
	}
	if s.waDevice != nil && s.waDevice.ID != nil {
		st.WADevice = s.waDevice.ID.String()
	}
	wdb, snap := s.waDB, s.waSnap
	cli := s.client
	s.mu.Unlock()
	if wdb != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		b, err := wdb.snapshot(ctx)
		cancel()
		if err != nil {
			logCode("warn", "snapshot_failed")
			return nil // keep the previous state rather than lose the device
		}
		st.WADB = b
	} else {
		st.WADB = snap
	}
	if cli != nil {
		if blob, err := cli.DumpState(); err == nil && len(blob) > 0 {
			st.Messagix = blob
		}
	}
	b, err := json.Marshal(st)
	if err != nil {
		return nil
	}
	return b
}

func (s *messagixSession) markStateDirty() {
	s.mu.Lock()
	s.stateDirty = true
	s.mu.Unlock()
}

// stateLoop emits the state blob when it changes, at most once per few seconds.
func (s *messagixSession) stateLoop() {
	defer close(s.stateLoopDone)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.flushState(false)
		}
	}
}

func (s *messagixSession) flushState(force bool) bool {
	// Keep the snapshot and its emission ordered with synchronous pre-event
	// seals. Without this lock a background flush could clear dirty, then let a
	// message emit before that flush had written the corresponding state.
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	return s.flushStateLocked(force, nil)
}

func (s *messagixSession) flushStateLocked(force bool, ackCtx context.Context) bool {
	s.mu.Lock()
	if s.emit == nil { // never started: there is nothing to seal
		s.mu.Unlock()
		return false
	}
	if time.Now().Before(s.stateRetryAt) {
		s.mu.Unlock()
		return false
	}
	dirty := s.stateDirty
	rateLimited := time.Since(s.lastState) < 3*time.Second && !force
	if (!dirty && !force) || rateLimited {
		s.mu.Unlock()
		return !dirty
	}
	// Reset dirty before dumping: a change that lands while the dump runs must
	// mark the state dirty again, or it would never be sealed.
	s.stateDirty = false
	s.mu.Unlock()
	blob := s.dump()
	s.mu.Lock()
	if blob == nil {
		s.stateDirty = true
		s.mu.Unlock()
		return false
	}
	if !force && bytes.Equal(blob, s.lastStateBlob) {
		s.stateRetryAt = time.Time{}
		s.stateRetryDelay = 0
		s.mu.Unlock()
		return true
	}
	// Reserve the largest possible seq so stamping cannot cross maxLine.
	if !frameFits(Frame{T: TState, State: blob, Seq: 1<<63 - 1}) {
		s.stateDirty = true
		s.stateRetryDelay = min(max(2*time.Second, 2*s.stateRetryDelay), 5*time.Minute)
		s.stateRetryAt = time.Now().Add(s.stateRetryDelay)
		s.mu.Unlock()
		logCode("error", "state_too_large")
		s.emit(Frame{T: THealth, Code: "state_too_large", Bytes: len(blob)})
		return false
	}
	s.stateRetryAt = time.Time{}
	s.stateRetryDelay = 0
	s.mu.Unlock()
	seq := s.emit(Frame{T: TState, State: blob, waitAck: ackCtx != nil})
	if seq < 0 || (ackCtx != nil && (seq == 0 || s.acks == nil || !s.acks.waitContext(ackCtx, seq))) {
		s.mu.Lock()
		s.stateDirty = true
		s.mu.Unlock()
		return false
	}
	s.mu.Lock()
	s.lastState = time.Now()
	s.lastStateBlob = blob
	s.mu.Unlock()
	return true
}

// sealState seals immediately. Deliveries already waiting on flushMu reuse
// the snapshot only if no newer store write has made it dirty again.
func (s *messagixSession) sealState() bool {
	s.flushMu.Lock()
	defer s.flushMu.Unlock()
	if s.ctx != nil && s.ctx.Err() != nil {
		return false
	}
	s.mu.Lock()
	dirty := s.stateDirty
	s.mu.Unlock()
	if !dirty {
		return true
	}
	return s.flushStateLocked(true, nil)
}

// connectE2EE mirrors the connector's connectE2EE: reuse the saved whatsmeow
// device or register a new one, then connect the Messenger-configured client.
func (s *messagixSession) connectE2EE() error {
	s.e2eeConnectMu.Lock()
	defer s.e2eeConnectMu.Unlock()
	s.mu.Lock()
	if s.stopping || s.logoutOnly {
		s.mu.Unlock()
		return errCode("not_started")
	}
	if s.e2ee != nil {
		s.mu.Unlock()
		return nil
	}
	s.e2eeTry = true
	cli := s.client
	jidStr := s.waDeviceJID
	s.mu.Unlock()
	if cli == nil {
		return errCode("not_started")
	}
	wdb, err := s.deviceStore()
	if err != nil {
		return err
	}
	var dev *store.Device
	if jidStr != "" {
		if jid, err := waTypes.ParseJID(jidStr); err == nil {
			if dev, err = wdb.container.GetDevice(s.ctx, jid); err != nil {
				return err
			}
		}
	}
	isNew := dev == nil
	if isNew {
		dev = wdb.container.NewDevice()
		if suggested := cli.MessengerLite.GetSuggestedDeviceID(); suggested != uuid.Nil {
			dev.FacebookUUID = suggested
		}
	}
	cli.SetDevice(dev)
	if isNew {
		if err := cli.RegisterE2EE(s.ctx, s.selfID); err != nil {
			return err
		}
		if dev.Account == nil {
			return errCode("unknown")
		}
		if err := dev.Save(s.ctx); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.waDevice = dev
	s.mu.Unlock()
	ecli, err := cli.PrepareE2EEClient()
	if err != nil {
		return err
	}
	ecli.SynchronousAck = true
	ecli.EnableDecryptedEventBuffer = true
	ecli.AddEventHandlerWithSuccessStatus(s.handleWAEvent)
	if err := ecli.ConnectContext(s.ctx); err != nil {
		return err
	}
	s.mu.Lock()
	s.e2ee = ecli
	s.mu.Unlock()
	s.markStateDirty()
	return nil
}

// deviceStore transfers the restored snapshot into the live store once. The
// connection owner holds e2eeConnectMu; Stop waits for that owner to finish.
func (s *messagixSession) deviceStore() (*waDB, error) {
	s.mu.Lock()
	wdb, snap := s.waDB, s.waSnap
	s.mu.Unlock()
	if wdb != nil {
		return wdb, nil
	}
	wdb, err := openWADB(s.ctx, snap, s.markStateDirty)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.waDB, s.waSnap = wdb, nil
	s.mu.Unlock()
	return wdb, nil
}

// scheduleE2EE admits one asynchronous connection attempt while holding the
// same lock Stop uses to close admission. Stop can then wait for every owner
// before taking its final state snapshot.
func (s *messagixSession) scheduleE2EE() {
	s.mu.Lock()
	if s.stopping || s.logoutOnly || s.e2ee != nil || s.e2eeStarting {
		s.mu.Unlock()
		return
	}
	s.e2eeStarting = true
	s.workWG.Add(1)
	s.mu.Unlock()
	go func() {
		defer s.workWG.Done()
		defer func() {
			s.mu.Lock()
			s.e2eeStarting = false
			s.mu.Unlock()
		}()
		if err := s.connectE2EE(); err != nil {
			logCode("warn", "e2ee_failed")
		}
	}()
}

// checkHistory reports the secure-history state: enrolled if a confirmed
// device is restored, needs_pin if a PIN backup exists, else none.
func (s *messagixSession) checkHistory(ctx context.Context) HistoryState {
	s.mu.Lock()
	dev := s.histDev
	cli := s.client
	backup := s.histBak
	s.mu.Unlock()
	if dev != nil && dev.ID != "" {
		return HistoryEnrolled
	}
	if backup != nil {
		return HistoryNeedsPIN
	}
	if cli == nil {
		return HistoryNone
	}
	bakCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	backup, err := cli.GetSecureHistoryClient().GetBackup(bakCtx)
	if err != nil {
		if !errors.Is(err, securehistory.ErrNoPINBackup) {
			logCode("warn", "history_check_failed")
		}
		return HistoryNone
	}
	s.mu.Lock()
	s.histBak = backup
	s.mu.Unlock()
	return HistoryNeedsPIN
}

// HistoryPIN runs the connector's restoreHistory flow: GetBackup, RecoverPIN,
// GetVirtualDevice, DeriveEpochs, PrepareDevice, then AddDevice with the
// enrollment persisted before the server confirms it.
func (s *messagixSession) HistoryPIN(ctx context.Context, pin []byte) (HistoryState, error) {
	defer zero(pin)
	if len(pin) != 6 {
		return HistoryNone, errCode("bad_request")
	}
	for _, c := range pin {
		if c < '0' || c > '9' {
			return HistoryNone, errCode("bad_request")
		}
	}
	s.mu.Lock()
	cli := s.client
	dev := s.histDev
	backup := s.histBak
	s.mu.Unlock()
	if cli == nil {
		return HistoryNone, errCode("not_started")
	}
	if dev != nil && dev.ID != "" {
		return HistoryEnrolled, nil
	}
	if err := s.waitE2EE(ctx); err != nil {
		return HistoryNone, err
	}
	history := cli.GetSecureHistoryClient()
	var err error
	if backup == nil {
		backup, err = history.GetBackup(ctx)
		if err != nil {
			return HistoryNone, mapAuthError(err)
		}
	}
	recovery, err := history.RecoverPIN(ctx, backup, pin)
	if err != nil {
		var recoveryErr *securehistory.RecoveryError
		if errors.As(err, &recoveryErr) {
			if recoveryErr.AttemptsRemaining != nil && *recoveryErr.AttemptsRemaining == 0 {
				return HistoryNone, &authErr{AuthPINLocked}
			}
			return HistoryNone, &authErr{AuthPINWrong}
		}
		return HistoryNone, mapAuthError(err)
	}
	virtual, err := history.GetVirtualDevice(ctx, backup, s.selfID, recovery.Code)
	if err != nil {
		return HistoryNone, mapAuthError(err)
	}
	epochs, err := virtual.DeriveEpochs()
	if err != nil {
		return HistoryNone, errCode("unknown")
	}
	s.mu.Lock()
	waDevice := s.waDevice
	s.mu.Unlock()
	if waDevice == nil || waDevice.IdentityKey == nil {
		return HistoryNone, errCode("unavailable")
	}
	enrollment, err := virtual.PrepareDevice(backup.ID, history.RegistrationID, waDevice.IdentityKey, epochs)
	if err != nil {
		return HistoryNone, errCode("unknown")
	}
	_, err = s.persistHistoryEnrollment(ctx, enrollment.Device, func() (*securehistory.Device, error) {
		return history.AddDevice(ctx, enrollment)
	})
	if err != nil {
		var rejected *securehistory.EnrollmentError
		if errors.As(err, &rejected) {
			return HistoryNone, errCode("rejected")
		}
		return HistoryNone, errCode("unknown")
	}
	s.flushState(true)
	logCode("info", "history_enrolled")
	return HistoryEnrolled, nil
}

// persistHistoryEnrollment seals prepared keys before AddDevice, but only
// retains the enrollment after confirmation. Every failure restores the prior
// device and dirties the state so a later seal replaces the failed attempt.
func (s *messagixSession) persistHistoryEnrollment(ctx context.Context, dev *securehistory.Device, add func() (*securehistory.Device, error)) (_ *securehistory.Device, err error) {
	s.mu.Lock()
	previous := s.histDev
	prepared := *dev
	prepared.ID = "" // prepared keys are not a confirmed enrollment
	s.histDev = &prepared
	s.stateDirty = true
	s.mu.Unlock()
	defer func() {
		if err != nil {
			s.mu.Lock()
			s.histDev = previous
			s.stateDirty = true
			s.mu.Unlock()
		}
	}()
	s.flushMu.Lock()
	ok := s.flushStateLocked(true, ctx)
	s.flushMu.Unlock()
	if !ok || ctx.Err() != nil {
		return nil, errCode("state_seal_failed")
	}
	confirmed, err := add()
	if err != nil {
		return nil, err
	}
	if confirmed == nil || confirmed.ID == "" {
		return nil, errCode("rejected")
	}
	s.mu.Lock()
	s.histDev = confirmed
	s.stateDirty = true
	s.mu.Unlock()
	return confirmed, nil
}

// waitE2EE makes sure the E2EE device exists: the enrollment signs the
// whatsmeow identity key, so there is no point continuing without it.
func (s *messagixSession) waitE2EE(ctx context.Context) error {
	s.mu.Lock()
	tried := s.e2eeTry
	s.mu.Unlock()
	if !tried {
		if err := s.connectE2EE(); err != nil {
			return errCode("unavailable")
		}
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		ready := s.waDevice != nil && s.waDevice.IdentityKey != nil
		s.mu.Unlock()
		if ready {
			return nil
		}
		select {
		case <-ctx.Done():
			return errCode("unavailable")
		case <-time.After(500 * time.Millisecond):
		}
	}
	return errCode("unavailable")
}

// Stop disconnects cleanly, flushes a final state frame while the memory
// store is still open, then closes and wipes the store; nothing durable is
// left in the process.
func (s *messagixSession) Stop() {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return
	}
	s.stopped = true
	s.stopping = true
	cancel, loopDone := s.cancel, s.stateLoopDone
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if s.acks != nil {
		s.acks.close()
	}
	if loopDone != nil {
		<-loopDone
	}
	// Startup may still be registering the device. Wait before taking custody
	// of the clients/store so nothing can appear after the final state frame.
	s.workWG.Wait()
	s.mu.Lock()
	cli, ecli, wdb := s.client, s.e2ee, s.waDB
	s.mu.Unlock()
	if ecli != nil {
		ecli.Disconnect()
	}
	if cli != nil {
		cli.SetEventHandler(nil)
		cli.Disconnect()
	}
	s.flushState(true)
	if wdb != nil {
		wdb.close()
		s.mu.Lock()
		if s.waDB == wdb {
			s.waDB = nil
		}
		s.mu.Unlock()
	}
}

// Logout ends the session for good: a best-effort Messenger web-session logout
// through messagix's own HTTP client, then the local device-store deletion the
// upstream bridge's logout performs (whatsmeow's Client.Logout refuses
// Messenger-configured clients). Every session and device state in memory is
// then wiped, and the emptied state is emitted so a sealed blob cannot restore
// the logged-out session.
func (s *messagixSession) Logout(ctx context.Context) error {
	s.shutdownMu.Lock()
	defer s.shutdownMu.Unlock()
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return errCode("not_started")
	}
	s.stopped = true
	s.stopping = true
	cancel, loopDone := s.cancel, s.stateLoopDone
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if s.acks != nil {
		s.acks.close()
	}
	if loopDone != nil {
		<-loopDone
	}
	s.workWG.Wait()
	s.mu.Lock()
	cli, ecli := s.client, s.e2ee
	dev, wdb := s.waDevice, s.waDB
	s.mu.Unlock()

	var err error
	if cli != nil {
		s.webLogout(ctx, cli)
	}

	s.mu.Lock()
	s.client, s.e2ee, s.waDevice, s.waDB = nil, nil, nil, nil
	s.waDeviceJID, s.waSnap = "", nil
	s.histDev, s.histBak = nil, nil
	s.threads = map[string]*threadInfo{}
	s.participants = map[int64][]int64{}
	s.names = map[int64]string{}
	s.cutover = map[int64]int64{}
	s.fbToWA = map[int64]int64{}
	s.msgIndex = map[string]string{}
	s.msgOrder = nil
	s.selfID, s.selfName = 0, ""
	s.stateDirty = true
	s.stateRetryAt = time.Time{}
	s.stateRetryDelay = 0
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	if ecli != nil {
		ecli.Disconnect()
	}
	if dev != nil {
		dctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		err = dev.Delete(dctx)
		cancel()
	}
	if cli != nil {
		cli.SetEventHandler(nil)
		cli.Disconnect()
	}
	if wdb != nil {
		wdb.close()
	}
	s.flushState(true)
	return err
}

// webLogout asks Facebook's web endpoint to end the browser session this
// sidecar's cookies hold, through messagix's own HTTP client: a form POST
// carrying the session's fb_dtsg. messagix exposes no logout method, so the
// call is strictly best effort: any failure is dropped and the wipe in Logout
// is what actually ends the session locally.
func (s *messagixSession) webLogout(ctx context.Context, cli *messagix.Client) {
	hc := cli.GetHTTP()
	if hc == nil {
		logCode("info", "web_logout_skipped")
		return
	}
	// NewHTTPQuery exposes the loaded page token without needing socket sync.
	dtsg := hc.NewHTTPQuery().FbDtsg
	if dtsg == "" {
		logCode("info", "web_logout_skipped")
		return
	}
	form := "fb_dtsg=" + url.QueryEscape(dtsg) + "&ref=mb"
	wctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	resp, _, err := hc.MakeRequestOnceNoRedirect(
		wctx, logoutURL, http.MethodPost,
		hc.BuildHeaders(true, false), []byte(form), types.FORM,
	)
	if err != nil || resp == nil || resp.StatusCode >= 400 {
		logCode("warn", "web_logout_failed")
		return
	}
	logCode("info", "web_logout_attempted")
}

// handleMetaEvent forwards messagix connection events; message tables are
// handled by handleTable in events.go.
func (s *messagixSession) handleMetaEvent(ctx context.Context, rawEvt any) {
	switch evt := rawEvt.(type) {
	case *table.LSTable:
		s.handleTable(evt)
	case *messagix.ConnectedEvent, *messagix.ReconnectedEvent:
		s.markStateDirty()
		s.scheduleE2EE()
	case *messagix.TransientDisconnectEvent:
		logCode("info", "transient_disconnect")
		if errors.Is(evt.Err, messagix.ErrFatalSocketSyncError) {
			s.fullReconnect()
		}
	case *messagix.PermanentErrorEvent:
		if dgwCloseStatus(evt.Err) == dgw.CloseStatusUnauthorized {
			logCode("warn", "auth_invalid")
			s.emit(Frame{T: TAuth, Auth: AuthInvalid})
			s.Stop()
			emitterFatal(2)
			return
		}
		logCode("warn", "permanent_error")
		s.fullReconnect()
	}
}

// fullReconnect redoes the page load, like the connector's FullReconnect: a
// stale sync state keeps failing until the page is loaded again.
func (s *messagixSession) fullReconnect() {
	s.mu.Lock()
	if s.stopping || time.Since(s.lastFullRec) < 5*time.Minute {
		s.mu.Unlock()
		return
	}
	s.lastFullRec = time.Now()
	cli := s.client
	ctx := s.ctx
	s.workWG.Add(1)
	s.mu.Unlock()
	defer s.workWG.Done()
	if cli == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	if _, _, err := cli.LoadMessagesPage(ctx); err != nil {
		logCode("warn", "full_reconnect_failed")
		return
	}
	if err := cli.Connect(s.ctx); err != nil {
		logCode("warn", "full_reconnect_failed")
	}
	s.markStateDirty()
}

// mapAuthError turns provider refusals into fixed protocol errors.
func mapAuthError(err error) error {
	switch {
	case errors.Is(err, httpclient.ErrTokenInvalidated),
		errors.Is(err, httpclient.ErrTokenInvalidatedRedirect),
		errors.Is(err, httpclient.ErrUserIDIsZero),
		errors.Is(err, types.ErrPleaseLogIn):
		return &authErr{AuthInvalid}
	case errors.Is(err, httpclient.ErrCheckpointRequired),
		errors.Is(err, httpclient.ErrConsentRequired),
		errors.Is(err, httpclient.ErrChallengeRequired),
		errors.Is(err, httpclient.ErrAccountSuspended):
		return &authErr{AuthCheckpoint}
	}
	return errCode("unknown")
}

// classifyStartError keeps auth answers as they are and names every other
// start failure by stage and a fixed class (never the error text, which can
// carry provider content): load_page_timeout, connect_dns, load_page_other...
func classifyStartError(stage string, err error) error {
	mapped := mapAuthError(err)
	var auth *authErr
	if errors.As(mapped, &auth) {
		return mapped
	}
	code := stage + "_" + errorClass(err)
	logCode("warn", code)
	return errCode(code)
}

func errorClass(err error) string {
	var dns *net.DNSError
	var netErr net.Error
	var tlsRecord tls.RecordHeaderError
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	switch {
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.As(err, &dns):
		return "dns"
	case errors.As(err, &tlsRecord), errors.As(err, &unknownAuthority), errors.As(err, &hostname):
		return "tls"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "refused"
	case errors.Is(err, syscall.ECONNRESET), errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "reset"
	case errors.As(err, &netErr) && netErr.Timeout():
		return "timeout"
	case errors.As(err, &netErr):
		return "net"
	}
	return "other"
}

func zero(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

// dgwCloseStatus is the websocket close status of a socket error.
func dgwCloseStatus(err error) websocket.StatusCode { return websocket.CloseStatus(err) }

// emitterFatal ends the process shortly after an auth frame was written.
func emitterFatal(code int) {
	go func() {
		time.Sleep(500 * time.Millisecond)
		os.Exit(code)
	}()
}
