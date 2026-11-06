// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
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

	"go.mau.fi/mautrix-meta/pkg/messagix"
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
	if err := s.acquireWalk(ctx); err != nil {
		return nil, err
	}
	defer s.releaseWalk()
	return s.lightspeedHistory(ctx, thread, key, cursor, count)
}

// lightspeedHistory pages a pre-encryption thread. The answer arrives as table
// rows, gathered by a collector, exactly as the web client receives them.
//
// A thread marked as a message request (pending folder or request status 1, the
// pending Marketplace threads the listing types included) is not read: no
// CreateThreadTask (209) and no FetchMessagesTask is sent for it, since what the
// server does with a thread open on a request is not established. It answers
// "unavailable", a retry, and is read once a later row clears the mark. Until then
// the thread holds only what the listing's own replies carried. The marks are
// checked again before every send of the call (first page, re-request, further
// pages), so a mark that arrives while the call waits stops it.
func (s *messagixSession) lightspeedHistory(ctx context.Context, thread string, key int64, cursor string, count int) (*Page, error) {
	s.mu.Lock()
	var cli threadPager = s.historyPager
	if cli == nil && s.client != nil {
		cli = s.client
	}
	if _, busy := s.collectors[key]; busy || cli == nil || s.requestMarkedLocked(key) {
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
	// Every send rechecks the request marks: a row that marks the thread while
	// this call waits stops the re-request and any further page.
	//
	// Accepted residual: this narrows, it does not close. A mark applied between
	// this check and the socket write (the library sends and waits in one call,
	// on the loop that applies marks, so no lock can span both), or a thread read
	// before any request fact reached the helper, can still be fetched. Do not
	// promise zero fetches or bounded request-fact arrival time.
	request := func(ts int64, id string) error {
		s.mu.Lock()
		marked := s.requestMarkedLocked(key)
		s.mu.Unlock()
		if marked {
			return errRequestMarked
		}
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
		return nil, historySendErr(err)
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
					return nil, historySendErr(err)
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
					return nil, historySendErr(err)
				}
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
}

// errRequestMarked is a History send withheld because the thread carries a
// request mark; it answers "unavailable", nothing having been sent.
var errRequestMarked = errors.New("thread is a message request")

func historySendErr(err error) error {
	if errors.Is(err, errRequestMarked) {
		return errCode("unavailable")
	}
	return mapSendErr(err)
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

// threadBatchLimit is the per-walk request cap. Meta chooses the page size (the task
// carries none), so the cap is sized on a conservative 20 threads per page: 50 requests
// reach 1000 chats, about 30 times the 32 chats seen on the largest observed account, while
// at the 500 ms delay a walk makes at most 50 requests over about 25 s. Reaching the cap
// ends the walk incomplete with inbox_stop 4 and saves its continuation (see pageThreads),
// so an inbox longer than the cap is covered over several walks.
var (
	threadBatchLimit = 50
	threadBatchDelay = 500 * time.Millisecond
)

// threadPager is the part of the messagix client the thread listing uses. The listing
// owns its paging: ExecuteTasks is a plain request and response, so it never reads or
// writes the library's keystore or cursors, and GetCursor is the one read of the
// group-1 cursor the library's own page fetch makes.
type threadPager interface {
	ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error)
	GetCursor(db int64) string
}

// listingDone is what a finished listing reports on threads_done: counts, and a fixed
// gap code when the inbox was not walked to its end.
type listingDone struct {
	Stats map[string]int
	Gap   string
}

// gapInboxIncomplete says the walk of sync group 1 ended before the server said there
// was nothing more (no range row in a reply, no progress, or the page bound).
const gapInboxIncomplete = "inbox_incomplete"

// gapRequestsIncomplete says the inbox walk was complete but a first-page request of
// another scope (a folder, or message requests) failed or had no answer.
const gapRequestsIncomplete = "requests_incomplete"

// Threads lists every thread: the ones already known, then the rest of the main
// inbox (sync group 1), walked page by page with the requests the web client scrolls
// it with, then the first page of every other scope (see scanScopes): the other
// group-1 parents (folders such as Marketplace) and the message requests (sync group
// 95). A message request is listed but stays inert unless it is a Marketplace thread
// (see unacceptedLocked). The counts and any gap it returns go out on threads_done.
func (s *messagixSession) Threads(ctx context.Context, emit emitFunc) (listingDone, error) {
	s.mu.Lock()
	cli := s.client
	s.mu.Unlock()
	if cli == nil {
		return listingDone{}, errCode("not_started")
	}
	return s.listThreads(ctx, cli, emit)
}

func (s *messagixSession) listThreads(ctx context.Context, cli threadPager, emit emitFunc) (listingDone, error) {
	if err := s.acquireWalk(ctx); err != nil {
		return listingDone{}, err
	}
	defer s.releaseWalk()
	emitted := map[string]bool{}
	counted := func(f Frame) int64 {
		if f.T == TThread && f.ThreadEvt != nil && f.ThreadEvt.Encrypted {
			emitted[f.ThreadEvt.ID] = true
		}
		return emit(f)
	}
	if err := s.emitKnownThreads(ctx, counted); err != nil {
		return listingDone{}, err
	}
	rows := inboxRowCounts{}
	tally := &scopeTally{}
	walker := tallyPager{threadPager: cli, tally: tally}
	gap, stop, pages, err := s.pageThreads(ctx, walker, counted, &rows)
	if err != nil {
		return listingDone{}, err
	}
	s.mu.Lock()
	s.scopeFolders = maps.Clone(tally.folders)
	s.mu.Unlock()
	parents := scopeParents(cli, tally.folders)
	scopePages, failed, err := s.scanScopes(ctx, walker, counted, parents, nil)
	if err != nil {
		return listingDone{}, err
	}
	s.logScopes(tally, parents, failed > 0)
	if failed > 0 && gap == "" {
		// The inbox is complete but a scope went unread: not a clean listing.
		gap = gapRequestsIncomplete
	}
	known, unlisted := s.encryptedCounts()
	stats := s.listingStats(known, len(emitted), unlisted)
	stats["inbox_stop"], stats["inbox_pages"] = stop, pages
	stats["inbox_rows_total"] = rows.total
	stats["inbox_rows_parent_inbox"] = rows.inbox
	stats["inbox_rows_parent0_more"] = rows.parent0More
	stats["inbox_rows_parent0_end"] = rows.parent0End
	stats["inbox_rows_other_parent"] = rows.other
	stats["scope_parents"], stats["scope_pages"], stats["scope_failed"] = len(parents), scopePages, failed
	return listingDone{Stats: stats, Gap: gap}, nil
}

// pageThreads walks sync group 1 from its first page. Each reply goes through the
// normal table path; the next page is addressed by the group-1 range row of that
// same reply (its minimum thread key and minimum activity time); the requested parent
// stays fixed. The walk is complete only when a reply says there is nothing more before.
// A reply with no deciding range row, a range that does not move, or the page bound ends it
// incomplete. The group-1 cursor is read from the library once per request, as its
// own page fetch does; nothing is written to the library.
// Inbox stop codes: 0 complete, 1 no table, 2 no range, 3 no movement, 4 page bound.
//
// A walk that ends at the page bound keeps its continuation position in s.inboxResume,
// and the next walk of this session resumes there, so an inbox longer than the cap is
// reached over several walks. The position is taken (cleared) when a walk starts, so a
// walk that completes, stops for any other reason or fails leaves nothing saved and the
// next walk starts from the top and sees new threads. It lives in process memory only:
// a companion restart re-walks from the top.
func (s *messagixSession) pageThreads(ctx context.Context, cli threadPager, emit emitFunc, rows *inboxRowCounts) (gap string, stop, pages int, err error) {
	ref := inboxTop
	s.mu.Lock()
	if r := s.inboxResume; r != nil {
		ref.ReferenceThreadKey, ref.ReferenceActivityTimestamp = r.threadKey, r.activityMS
	}
	s.inboxResume = nil
	s.mu.Unlock()
	gap, stop, pages, next, err := s.walkInbox(ctx, cli, emit, rows, ref, threadBatchLimit, nil)
	if err == nil && stop == 4 {
		s.mu.Lock()
		s.inboxResume = &inboxResumeRef{threadKey: next.ReferenceThreadKey, activityMS: next.ReferenceActivityTimestamp}
		s.mu.Unlock()
	}
	return gap, stop, pages, err
}

// inboxTop is the first page request of a walk: the newest threads of the main inbox.
var inboxTop = socket.FetchThreadsTask{ParentThreadKey: inboxParentKey, ReferenceThreadKey: 0, ReferenceActivityTimestamp: 9999999999999}

// walkInbox pages group 1 from ref for at most limit requests and returns the next
// request position (what a page-bound stop, code 4, would resume at). It owns no
// shared walk state: pageThreads keeps inboxResume, and the roster refresh passes its
// own start, limit and done. When done is set it is checked after each page is
// applied; true ends the walk with stop code 5 (nothing more wanted).
func (s *messagixSession) walkInbox(ctx context.Context, cli threadPager, emit emitFunc, rows *inboxRowCounts, ref socket.FetchThreadsTask, limit int, done func() bool) (gap string, stop, pages int, next socket.FetchThreadsTask, err error) {
	const group = 1
	for batch := 0; batch < limit; batch++ {
		if err := ctx.Err(); err != nil {
			return "", 0, pages, ref, err
		}
		task := ref
		task.Cursor = cli.GetCursor(group)
		task.SyncGroup = group
		tbl, err := s.fetchPage(ctx, cli, emit, task)
		if err != nil {
			return "", 0, pages, ref, err
		}
		if tbl == nil {
			return gapInboxIncomplete, 1, pages, ref, nil
		}
		pages++
		if err := ctx.Err(); err != nil {
			return "", 0, pages, ref, err
		}
		if done != nil && done() {
			return "", 5, pages, ref, nil
		}
		rng := group1Range(tbl, inboxParentKey, rows)
		if rng == nil {
			return gapInboxIncomplete, 2, pages, ref, nil
		}
		if !rng.HasMoreBefore {
			return "", 0, pages, ref, nil
		}
		next := socket.FetchThreadsTask{ParentThreadKey: inboxParentKey, ReferenceThreadKey: rng.MinThreadKey, ReferenceActivityTimestamp: rng.MinLastActivityTimestampMS}
		if next.ParentThreadKey == ref.ParentThreadKey && next.ReferenceThreadKey == ref.ReferenceThreadKey && next.ReferenceActivityTimestamp == ref.ReferenceActivityTimestamp {
			return gapInboxIncomplete, 3, pages, ref, nil
		}
		ref = next
		select {
		case <-time.After(threadBatchDelay):
		case <-ctx.Done():
			return "", 0, pages, ref, ctx.Err()
		}
	}
	return gapInboxIncomplete, 4, pages, ref, nil
}

// inboxResumeRef is the range position the next walk requests after a walk hit the page bound.
type inboxResumeRef struct{ threadKey, activityMS int64 }

// fetchPage issues one thread-list request and applies its reply through the normal
// table path. The reply's roster rows date from the request: a reconnect, or a live
// roster or mark change, while it was in flight leaves them stale. A nil table is
// returned as is, unapplied.
func (s *messagixSession) fetchPage(ctx context.Context, cli threadPager, emit emitFunc, task socket.FetchThreadsTask) (*table.LSTable, error) {
	task.IsAfter = 0
	task.AdditionalPagesToFetch = 0
	s.mu.Lock()
	mark := s.issueMarkLocked()
	s.mu.Unlock()
	tbl, err := cli.ExecuteTasks(ctx, &task)
	if err != nil {
		return nil, mapSendErr(err)
	}
	if tbl == nil {
		return nil, nil
	}
	s.handleReplyEmit(tbl, mark, func(f Frame) int64 {
		if ctx.Err() != nil {
			return 0
		}
		return emit(f)
	})
	return tbl, nil
}

// inboxParentKey is the parent thread key of the main inbox: the only parent the walk
// pages through. Every other scope gets its first page only (see scanScopes).
const inboxParentKey = -1

// requestSyncGroup is the sync group of message requests ("pending").
const requestSyncGroup = 95

// scopeParentCap bounds how many group-1 parents besides the inbox one listing looks into.
const scopeParentCap = 8

// stateDumper is the library's public read of its own state, which carries the page's
// parent thread keys (configs.ParentThreadKeys, from the page's threadsRangesQuery rows).
type stateDumper interface {
	DumpState() (json.RawMessage, error)
}

var _ stateDumper = (*messagix.Client)(nil)

// scopeParents is the group-1 parents a listing looks into besides the main inbox: the
// library's configs.ParentThreadKeys and any parent a group-1 range row of this walk
// named, without the inbox (-1) and the group-wide parent 0, ascending, at most
// scopeParentCap. A state that cannot be read adds nothing.
func scopeParents(cli threadPager, seen map[int64]bool) []int64 {
	set := map[int64]bool{}
	if d, ok := cli.(stateDumper); ok {
		if blob, err := d.DumpState(); err == nil && len(blob) > 0 {
			var st struct {
				Configs struct{ ParentThreadKeys []int64 }
			}
			if json.Unmarshal(blob, &st) == nil {
				for _, k := range st.Configs.ParentThreadKeys {
					set[k] = true
				}
			}
		}
	}
	for k := range seen {
		set[k] = true
	}
	delete(set, inboxParentKey)
	delete(set, 0)
	keys := slices.Sorted(maps.Keys(set))
	return keys[:min(len(keys), scopeParentCap)]
}

// scanScopes asks for the first page of every scope the inbox walk does not cover:
// the message requests (sync group 95) of the inbox first, then each other group-1
// parent in parents (a folder such as Marketplace), then the requests of each of those
// parents. These are the
// first-page requests the library's own connect sends for every parent key and both
// groups (socket.go sendInitialThreadFetch), whose replies it drops. Group 95 is asked
// without a cursor, as the library does. Only the first page: a new thread is the
// newest of its scope. Each reply goes through fetchPage, so the stale-reply fence and
// the request marks apply to it as to the inbox walk. It is a read: FetchThreadsTask
// only, never a mark-read or an accept. Each scope is asked once per scan, never
// retried: a failed request, or one with no answer, is counted in failed and the scan
// goes on to the next scope, so one failing folder cannot hide the others. Only a
// cancel is an error. done is checked after each page, as in walkInbox. At most
// 2*len(parents)+1 requests, threadBatchDelay apart.
func (s *messagixSession) scanScopes(ctx context.Context, cli threadPager, emit emitFunc, parents []int64, done func() bool) (pages, failed int, err error) {
	scopes := make([]socket.FetchThreadsTask, 0, 2*len(parents)+1)
	scopes = append(scopes, socket.FetchThreadsTask{ParentThreadKey: inboxParentKey, SyncGroup: requestSyncGroup})
	for _, p := range parents {
		scopes = append(scopes, socket.FetchThreadsTask{ParentThreadKey: p, SyncGroup: 1})
	}
	for _, p := range parents {
		scopes = append(scopes, socket.FetchThreadsTask{ParentThreadKey: p, SyncGroup: requestSyncGroup})
	}
	for _, task := range scopes {
		select {
		case <-time.After(threadBatchDelay):
		case <-ctx.Done():
			return pages, failed, ctx.Err()
		}
		task.ReferenceThreadKey, task.ReferenceActivityTimestamp = inboxTop.ReferenceThreadKey, inboxTop.ReferenceActivityTimestamp
		if task.SyncGroup == 1 {
			task.Cursor = cli.GetCursor(1)
		}
		tbl, err := s.fetchPage(ctx, cli, emit, task)
		if err := ctx.Err(); err != nil {
			return pages, failed, err
		}
		if err != nil || tbl == nil {
			failed++
			continue
		}
		pages++
		if done != nil && done() {
			return pages, failed, nil
		}
	}
	return pages, failed, nil
}

// tallyPager passes every request through and counts its reply for the scope
// diagnostic (see logScopes). It changes nothing it passes on.
type tallyPager struct {
	threadPager
	tally *scopeTally
}

func (p tallyPager) ExecuteTasks(ctx context.Context, tasks ...socket.Task) (*table.LSTable, error) {
	tbl, err := p.threadPager.ExecuteTasks(ctx, tasks...)
	if err == nil && tbl != nil && len(tasks) == 1 {
		if task, ok := tasks[0].(*socket.FetchThreadsTask); ok {
			p.tally.add(task, tbl)
		}
	}
	return tbl, err
}

// scopeKey is one request scope: a sync group and a parent thread key.
type scopeKey struct{ group, parent int64 }

// scopeRow is what the diagnostic keeps of one thread row: no id, no name.
type scopeRow struct {
	folder      string
	marketplace bool
	otherParent bool // the row's own parent is not the one requested
}

// scopeTally is one listing's thread rows by request scope, and the group-1 parents
// its range rows named. Counts only ever leave it.
type scopeTally struct {
	threads map[scopeKey]map[int64]scopeRow
	folders map[int64]bool
}

func (t *scopeTally) add(task *socket.FetchThreadsTask, tbl *table.LSTable) {
	if t.threads == nil {
		t.threads, t.folders = map[scopeKey]map[int64]scopeRow{}, map[int64]bool{}
	}
	key := scopeKey{int64(task.SyncGroup), task.ParentThreadKey}
	rows := t.threads[key]
	if rows == nil {
		rows = map[int64]scopeRow{}
		t.threads[key] = rows
	}
	see := func(thread int64, typ table.ThreadType, folder string, parent int64) {
		rows[thread] = scopeRow{folder: folder, marketplace: typ == table.MARKETPLACE, otherParent: parent != 0 && parent != task.ParentThreadKey}
	}
	for _, th := range tbl.LSDeleteThenInsertThread {
		see(th.ThreadKey, th.ThreadType, th.FolderName, th.ParentThreadKey)
	}
	for _, th := range tbl.LSUpdateOrInsertThread {
		see(th.ThreadKey, th.ThreadType, th.FolderName, th.ParentThreadKey)
	}
	for _, r := range tbl.LSUpsertSyncGroupThreadsRange {
		if r.SyncGroup == 1 && r.ParentThreadKey != inboxParentKey && r.ParentThreadKey != 0 {
			t.folders[r.ParentThreadKey] = true
		}
	}
}

// folderLetters names the diagnostic's folder buckets, in folderBucket's order.
const folderLetters = "iposaex"

// folderBucket is a thread row's folder as an index into folderLetters: inbox, pending,
// other, spam, archived, none, anything else.
func folderBucket(folder string) int {
	switch folder {
	case "inbox":
		return 0
	case folderPending:
		return 1
	case "other":
		return 2
	case folderSpam:
		return 3
	case "archived":
		return 4
	case "":
		return 5
	}
	return 6
}

// logScopes logs scopeCode at info, only when it differs from the last one logged.
func (s *messagixSession) logScopes(t *scopeTally, parents []int64, failed bool) {
	code := scopeCode(t, parents, failed)
	s.mu.Lock()
	same := code == s.scopeLogged
	s.scopeLogged = code
	s.mu.Unlock()
	if !same {
		logCode("info", code)
	}
}

// scopeCode is the diagnostic's counts-only code: k<other parents>, then one segment
// per scope that answered: "in" (the inbox walk), "f<n>" (group-1 parent n of parents,
// 1-based; "fx" past the cap), "r0" (requests of the inbox), "r<n>" (requests of
// parent n); then its threads by folder (i inbox, p pending, o other, s spam,
// a archived, e none, x anything else), m Marketplace (type 5) threads and q threads
// whose own parent is not the requested one; ".err" when the scope scan failed.
// Counts are capped at 999. Parent keys themselves are never logged.
func scopeCode(t *scopeTally, parents []int64, failed bool) string {
	label := func(k scopeKey) string {
		prefix := "f"
		if k.group == requestSyncGroup {
			prefix = "r"
		}
		if k.parent == inboxParentKey {
			if k.group == 1 {
				return "in"
			}
			return prefix + "0"
		}
		if i := slices.Index(parents, k.parent); i >= 0 {
			return prefix + strconv.Itoa(i+1)
		}
		return prefix + "x"
	}
	var segs []string
	for k, rows := range t.threads {
		var folder [len(folderLetters)]int
		var m, q int
		for _, r := range rows {
			folder[folderBucket(r.folder)]++
			if r.marketplace {
				m++
			}
			if r.otherParent {
				q++
			}
		}
		seg := label(k)
		for i, n := range folder {
			if n > 0 {
				seg += fmt.Sprintf("_%c%d", folderLetters[i], min(n, 999))
			}
		}
		seg += fmt.Sprintf("_m%d", min(m, 999))
		if q > 0 {
			seg += fmt.Sprintf("_q%d", min(q, 999))
		}
		segs = append(segs, seg)
	}
	slices.Sort(segs)
	code := fmt.Sprintf("thread_scopes.k%d", len(parents))
	for _, seg := range segs {
		code += "." + seg
	}
	if failed {
		code += ".err"
	}
	return code
}

// inboxRowCounts accumulates only group-1 row categories for one walk.
type inboxRowCounts struct {
	total, inbox, parent0More, parent0End, other int
}

// group1Range retains the last inbox or parent-0 end decision. If neither is
// present, the last parent-0 more row supplies a cursor. Other parents never
// decide the walk. The request parent remains fixed at inboxParentKey.
func group1Range(tbl *table.LSTable, parent int64, counts *inboxRowCounts) *table.LSUpsertSyncGroupThreadsRange {
	var out, fallback *table.LSUpsertSyncGroupThreadsRange
	for _, r := range tbl.LSUpsertSyncGroupThreadsRange {
		if r.SyncGroup != 1 {
			continue
		}
		counts.total++
		switch {
		case r.ParentThreadKey == parent:
			counts.inbox++
			out = r
		case r.ParentThreadKey == 0 && r.HasMoreBefore:
			counts.parent0More++
			fallback = r
		case r.ParentThreadKey == 0:
			counts.parent0End++
			out = r
		default:
			counts.other++
		}
	}
	if out == nil {
		return fallback
	}
	return out
}

func nowMS() int64 { return time.Now().UnixMilli() }
