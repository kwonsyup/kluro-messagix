// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waArmadilloBackupMessage"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	"go.mau.fi/whatsmeow/proto/waMsgApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-meta/pkg/messagix/securehistory"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const (
	historyWait        = 30 * time.Second
	historyReRequest   = 5 * time.Second
	maxHistoryPageSize = 100
)

// History returns one page of a thread, newest first. The connector owns the
// cursor and completeness; the sidecar only says what this page read.
func (s *messagixSession) History(ctx context.Context, thread, cursor string, count int) (*Page, error) {
	enc, key, ok := parseWire(thread)
	if !ok {
		return nil, errCode("bad_request")
	}
	count = min(max(count, 1), maxHistoryPageSize)
	if enc {
		return s.encryptedHistory(ctx, thread, key, cursor, count)
	}
	return s.lightspeedHistory(ctx, thread, key, cursor, count)
}

// lightspeedHistory pages a pre-encryption thread. The answer arrives as table
// rows, gathered by a collector, exactly as the web client receives them.
func (s *messagixSession) lightspeedHistory(ctx context.Context, thread string, key int64, cursor string, count int) (*Page, error) {
	s.mu.Lock()
	cli := s.client
	if _, busy := s.collectors[key]; busy || cli == nil {
		s.mu.Unlock()
		return nil, errCode("unavailable")
	}
	c := &pageCollector{max: count, done: make(chan struct{})}
	s.collectors[key] = c
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.collectors, key)
		s.mu.Unlock()
	}()

	var refTS int64
	var refID string
	if cursor != "" {
		tsStr, id, found := strings.Cut(cursor, ":")
		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if !found || err != nil || id == "" {
			return nil, errCode("bad_request")
		}
		refTS, refID = ts, id
	}
	request := func(ts int64, id string) error {
		var task socket.Task = &socket.FetchMessagesTask{
			ThreadKey: key, Direction: 0, ReferenceTimestampMs: ts, ReferenceMessageId: id,
			SyncGroup: 1, Cursor: cli.GetCursor(1),
		}
		if id == "" {
			task = &socket.CreateThreadTask{ThreadFBID: key, SyncGroup: 1}
		}
		_, err := cli.ExecuteTasks(ctx, task)
		return err
	}
	if err := request(refTS, refID); err != nil {
		return nil, mapSendErr(err)
	}
	timer := time.NewTimer(historyWait)
	defer timer.Stop()
	tick := time.NewTicker(historyReRequest)
	defer tick.Stop()
	reRequested := false
	for {
		c.mu.Lock()
		have := c.upsert
		c.mu.Unlock()
		if have != nil {
			enough := len(have.Messages) >= count || !have.Range.HasMoreBefore
			if !enough {
				// More is wanted: ask for the page before what we hold.
				if err := request(have.Range.MinTimestampMs, have.Range.MinMessageId); err != nil {
					return nil, mapSendErr(err)
				}
			} else {
				c.finish()
			}
		}
		select {
		case <-c.done:
			return s.lightspeedPage(thread, c), nil
		case <-ctx.Done():
			return nil, errCode("unavailable")
		case <-timer.C:
			return nil, errCode("timeout")
		case <-tick.C:
			if !reRequested {
				reRequested = true
				if err := request(refTS, refID); err != nil {
					return nil, mapSendErr(err)
				}
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (s *messagixSession) lightspeedPage(thread string, c *pageCollector) *Page {
	c.mu.Lock()
	up := c.upsert
	c.mu.Unlock()
	page := &Page{Thread: thread, Complete: up != nil && !up.Range.HasMoreBefore}
	if up == nil {
		page.Complete = true
		page.Gap = "no_older_available"
		return page
	}
	msgs := slices.Clone(up.Messages)
	slices.SortFunc(msgs, func(a, b *table.WrappedMessage) int {
		if a.PrimarySortKey != b.PrimarySortKey {
			return int(b.PrimarySortKey - a.PrimarySortKey) // newest first
		}
		return int(b.SecondarySortKey - a.SecondarySortKey)
	})
	msgs = slices.CompactFunc(msgs, func(a, b *table.WrappedMessage) bool { return a.MessageId == b.MessageId })
	s.mu.Lock()
	for _, m := range msgs {
		if m.IsUnsent {
			continue
		}
		wm := convertLSMessage(m)
		wm.Thread = thread
		wm.Out = m.SenderId == s.selfID
		s.indexMessageLocked(wm.ID, thread)
		page.Messages = append(page.Messages, wm)
	}
	s.mu.Unlock()
	if up.Range.MinMessageId != "" {
		page.Next = fmt.Sprintf("%d:%s", up.Range.MinTimestampMs, up.Range.MinMessageId)
	}
	return page
}

// encryptedHistory pages an end-to-end encrypted thread through the secure
// storage archive. Without an enrollment the thread is not readable; with one,
// the archive pages by timestamp and ends where the encrypted thread began,
// continuing into the pre-encryption thread it is linked to.
func (s *messagixSession) encryptedHistory(ctx context.Context, thread string, key int64, cursor string, count int) (*Page, error) {
	s.mu.Lock()
	dev := s.histDev
	cli := s.client
	t := s.threads[thread]
	open, linked := s.cutover[key]
	s.mu.Unlock()
	if cli == nil {
		return nil, errCode("unavailable")
	}
	if dev == nil || dev.ID == "" {
		return nil, errCode("not_enrolled")
	}
	timestamp := time.Now().Add(time.Minute).UnixMilli()
	if cursor != "" {
		ts, err := strconv.ParseInt(cursor, 10, 64)
		if err != nil {
			return nil, errCode("bad_request")
		}
		timestamp = ts
	}
	isGroup := t != nil && t.Kind == "group"
	chatUser := strconv.FormatInt(key, 10)
	archive, err := cli.GetSecureHistoryClient().FetchMessages(ctx, dev, chatUser, true, timestamp, count)
	if err != nil {
		return nil, mapAuthOrUnknown(err)
	}
	if archive.Range.HasMoreBefore == nil {
		return nil, errCode("unknown")
	}
	hasMore := *archive.Range.HasMoreBefore
	if hasMore && archive.Range.Before >= timestamp {
		return nil, errCode("unknown") // the archive did not move backwards
	}
	if updated, err := s.saveEpochs(dev, archive.Epochs); err != nil {
		return nil, errCode("unknown")
	} else if updated != nil {
		dev = updated
	}
	decrypted, err := archive.Decrypt(dev, chatUser, s.selfID, isGroup)
	if err != nil {
		return nil, errCode("decrypt_failed")
	}
	page := &Page{Thread: thread, Next: strconv.FormatInt(archive.Range.Before, 10), Complete: !hasMore}
	for _, d := range decrypted {
		m, ok := s.archiveMessage(thread, chatUser, d)
		if ok {
			page.Messages = append(page.Messages, m)
		}
	}
	slices.SortFunc(page.Messages, func(a, b Message) int {
		if a.SentMS != b.SentMS {
			return int(b.SentMS - a.SentMS)
		}
		return strings.Compare(b.ID, a.ID)
	})
	if !hasMore && linked {
		page.Continue = wireID(false, open)
		page.Complete = false
	}
	return page, nil
}

func (s *messagixSession) saveEpochs(fetched *securehistory.Device, epochs *securehistory.EpochDerivationSet) (*securehistory.Device, error) {
	if epochs == nil || len(epochs.Edges) == 0 {
		return nil, nil
	}
	s.mu.Lock()
	prev := s.histDev
	if prev == nil || prev.ID != fetched.ID {
		s.mu.Unlock()
		return nil, errors.New("history enrollment changed while fetching")
	}
	next, err := prev.WithEpochs(epochs)
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.histDev = next
	s.stateDirty = true
	s.mu.Unlock()
	return next, nil
}

// archiveMessage converts one decrypted archive message. Reactions and other
// supplements are ignored; the latest edit replaces the text.
func (s *messagixSession) archiveMessage(thread, chatUser string, d *securehistory.DecryptedMessage) (Message, bool) {
	if d == nil || d.Message == nil || d.Message.Message == nil {
		return Message{}, false
	}
	meta := d.Message.Message.GetMetadata()
	if meta.GetMessageID() == "" || meta.GetSenderID() == "" {
		return Message{}, false
	}
	app, ok := decodeArchive(d.Message.Message)
	if !ok {
		return Message{}, false
	}
	senderID, err := strconv.ParseInt(meta.GetSenderID(), 10, 64)
	if err != nil {
		return Message{}, false
	}
	text, media, _ := consumerContent(app)
	if app == nil {
		media = "attachment"
	}
	for _, sup := range d.Supplementals {
		if sup == nil || sup.Message == nil {
			continue
		}
		// An edit supplemental applies only when its own sender is the original
		// message's sender; anyone else's supplemental is ignored.
		if sup.Message.GetMetadata().GetSenderID() != meta.GetSenderID() {
			continue
		}
		supApp, ok := decodeArchive(sup.Message)
		if !ok || supApp == nil {
			continue
		}
		if edit := supApp.GetPayload().GetContent().GetEditMessage(); edit != nil && edit.GetMessage() != nil {
			text = edit.GetMessage().GetText()
		}
	}
	id := "wa:" + meta.GetMessageID()
	s.mu.Lock()
	s.indexMessageLocked(id, thread)
	s.mu.Unlock()
	return Message{
		Thread: thread, ID: id, Sender: meta.GetSenderID(), Out: senderID == s.selfID,
		Text: text, Media: media, SentMS: d.Message.TimestampMS, Encrypted: true,
	}, true
}

func decodeArchive(m *waArmadilloBackupMessage.BackupMessage) (*waConsumerApplication.ConsumerApplication, bool) {
	payload, ok := m.GetPayload().(*waArmadilloBackupMessage.BackupMessage_EncryptedTransportMessage)
	if !ok {
		// Admin and transport events carry no user text.
		_, admin := m.GetPayload().(*waArmadilloBackupMessage.BackupMessage_MiTransportAdminMessage)
		return nil, admin
	}
	var application waMsgApplication.MessageApplication
	if err := proto.Unmarshal(payload.EncryptedTransportMessage, &application); err != nil {
		return nil, false
	}
	decoded, err := whatsmeow.DecodeFBApplication(&application)
	if err != nil {
		return nil, false
	}
	app, _ := decoded.Message.(*waConsumerApplication.ConsumerApplication)
	return app, true
}

func mapAuthOrUnknown(err error) error {
	if mapped := mapAuthError(err); mapped != nil {
		var a *authErr
		if errors.As(mapped, &a) {
			return mapped
		}
	}
	return errCode("unknown")
}

var _ = waTypes.MessengerServer

const (
	threadBatchLimit = 400
	threadBatchDelay = 500 * time.Millisecond
	e2eeSyncGroup    = 95
)

// e2eeRangeWait bounds each wait for an encrypted-range update. Some accounts
// never receive one, and the library wait honours only its context.
var e2eeRangeWait = 20 * time.Second

// listingGap is returned by Threads when the listing ended early for a fixed
// reason: the loop answers with threads_done carrying that code as its gap,
// not with an error.
type listingGap struct{ code string }

func (g *listingGap) Error() string { return "messenger: listing gap " + g.code }

const gapE2EERangeTimeout = "e2ee_range_timeout"

// threadRanges is the part of the messagix client the thread listing uses.
type threadRanges interface {
	WaitForInitialThreadRange(ctx context.Context, syncGroup int64) error
	WaitForThreadRange(ctx context.Context, syncGroup int64, generation uint64) error
	ThreadRangeGeneration(syncGroup int64) uint64
	GetSyncGroupKeyStore(syncGroup int64) *socket.KeyStoreData
	FetchMoreThreads(ctx context.Context, syncGroup int64) (*socket.KeyStoreData, *table.LSTable, error)
}

// waitE2EERange runs one encrypted-range wait for at most e2eeRangeWait. It
// reports true when only the bound expired, with the caller's ctx still live.
func waitE2EERange(ctx context.Context, wait func(context.Context) error) (bool, error) {
	wctx, cancel := context.WithTimeout(ctx, e2eeRangeWait)
	defer cancel()
	err := wait(wctx)
	if err != nil && ctx.Err() == nil && wctx.Err() != nil {
		logCode("warn", gapE2EERangeTimeout)
		return true, nil
	}
	return false, err
}

// Threads lists every thread: the ones already known, then the rest of the
// inbox by paging the lightspeed thread ranges (the pre-encryption inbox and
// the encrypted-chat range), as the web client scrolls them in.
func (s *messagixSession) Threads(ctx context.Context, emit emitFunc) error {
	s.mu.Lock()
	cli := s.client
	s.mu.Unlock()
	if cli == nil {
		return errCode("not_started")
	}
	return s.listThreads(ctx, cli, emit)
}

func (s *messagixSession) listThreads(ctx context.Context, cli threadRanges, emit emitFunc) error {
	for _, f := range s.sortedThreadFrames() {
		if err := ctx.Err(); err != nil {
			return err
		}
		emit(f)
	}
	if err := s.pageThreads(ctx, cli, 1, emit); err != nil {
		return err
	}
	if err := s.pageThreads(ctx, cli, e2eeSyncGroup, emit); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		var gap *listingGap
		if errors.As(err, &gap) {
			return gap
		}
		// The encrypted range is optional: a failure leaves those threads to
		// arrive live.
		logCode("warn", "e2ee_threads_failed")
	}
	return nil
}

func (s *messagixSession) pageThreads(ctx context.Context, cli threadRanges, group int64, emit emitFunc) error {
	if group == e2eeSyncGroup {
		// Only the update itself fills the keystore, so the wait has to come
		// before the "no more threads" test below.
		timedOut, err := waitE2EERange(ctx, func(wctx context.Context) error {
			return cli.WaitForInitialThreadRange(wctx, group)
		})
		if err != nil {
			return err
		}
		if timedOut {
			return &listingGap{gapE2EERangeTimeout}
		}
	}
	var prevMin int64
	for batch := 0; batch < threadBatchLimit; batch++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		if group == e2eeSyncGroup {
			ks := cli.GetSyncGroupKeyStore(group)
			if ks == nil || !ks.HasMoreBefore {
				return nil
			}
		}
		gen := cli.ThreadRangeGeneration(group)
		ks, tbl, err := cli.FetchMoreThreads(ctx, group)
		if err != nil {
			return mapSendErr(err)
		}
		if tbl == nil || ks == nil {
			return nil
		}
		for _, f := range s.handleTableFrames(tbl) {
			if err := ctx.Err(); err != nil {
				return err
			}
			emit(f)
		}
		if group == e2eeSyncGroup {
			timedOut, err := waitE2EERange(ctx, func(wctx context.Context) error {
				return cli.WaitForThreadRange(wctx, group, gen)
			})
			if err != nil {
				return err
			}
			if timedOut {
				return &listingGap{gapE2EERangeTimeout}
			}
		}
		if !ks.HasMoreBefore || ks.MinThreadKey == prevMin {
			return nil
		}
		prevMin = ks.MinThreadKey
		select {
		case <-time.After(threadBatchDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func nowMS() int64 { return time.Now().UnixMilli() }
