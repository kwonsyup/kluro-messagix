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
	if _, busy := s.collectors[key]; busy || cli == nil || s.historyBlockedLocked(false, key) {
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
	// Accepted residuals: (1) the check-to-dispatch window: a mark applied between
	// this check and the socket write (the library sends and waits in one call,
	// on the loop that applies marks, so no lock can span both) can still be
	// fetched. (2) After a restart, a request thread beyond a request scope's
	// first page, or on a parent omitted by the 8-parent cap, can receive
	// REPEATED fetches (there is no one-fetch bound) until a mark is learned.
	// (3) A pending thread moved to spam keeps importing live messages, because
	// the connector keeps its earlier qualified state; this is pre-existing
	// connector behaviour, with no Meta-side effect.
	request := func(ts int64, id string) error {
		s.mu.Lock()
		marked := s.historyBlockedLocked(false, key)
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

// historyBlockedLocked reports whether History must not read the thread now: it
// carries a request or spam mark (open or encrypted alike), or this session has not
// observed it (a row of the inbox walk, a scope scan or a live table) and the first
// scope scan has not yet completed with every request scope answered. Until then a
// thread the connector imported before a restart may be a request whose mark this
// session has not learned. A thread seen without a mark is readable at once; once a
// clean scan has finished, an unseen thread is, by elimination, not on a request
// scope's first page. Evaluated before every dispatch, not only at entry. If the scan
// keeps failing, unseen threads stay unavailable. The connector gives up on a thread
// after five unavailable answers, but that count is per run and in memory only
// (kluro-agent 0d212504 messenger_runner.go:2378), so a runner restart tries again.
// The caller holds s.mu.
func (s *messagixSession) historyBlockedLocked(enc bool, key int64) bool {
	if s.threadMarkedLocked(enc, key) {
		return true
	}
	if s.scopeScanClean || s.rowKeys[key] || s.encRowKeys[key] {
		return false
	}
	if enc {
		for fb, wa := range s.fbToWA {
			if wa == key && s.rowKeys[fb] {
				return false
			}
		}
	}
	return true
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
	blocked := s.historyBlockedLocked(true, key)
	s.mu.Unlock()
	if cli == nil || blocked {
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
	// Recheck right before the dispatch, as the open-thread path does.
	s.mu.Lock()
	blocked = s.historyBlockedLocked(true, key)
	s.mu.Unlock()
	if blocked {
		return nil, errCode("unavailable")
	}
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
//
// inboxRangeWait is ONE budget per walk, not per page: the total time all of a walk's
// pages may spend waiting for a range on a later frame (see awaitInboxRange). A page
// whose reply has no range waits at most what is left of it; once it is spent, such a
// page can still be decided by rows that already arrived, and otherwise stops the walk
// at once (stop 2). So a walk holds the walk slot (History, the roster refresh) at most
// 10 s longer than before, whatever its length: about 3% of the connector's 5-minute
// relist. 10 s is twice the library's ack timeout (dgw AckTimeout, 5 s) and a third of
// what it allows a one-off request's data frame (dgw ResponseTimeout, 30 s); a range
// later than that is treated as never sent.
var (
	threadBatchLimit = 50
	threadBatchDelay = 500 * time.Millisecond
	inboxRangeWait   = 10 * time.Second
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
	extra, unknown := s.frameDrops()
	gap, stop, pages, err := s.pageThreads(ctx, walker, counted, &rows)
	extra2, unknown2 := s.frameDrops()
	rows.oneoffExtra, rows.unknownStream = int(extra2-extra), int(unknown2-unknown)
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
	rows.addStats(stats)
	s.logWalkTables(&rows)
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
// Inbox stop codes: 0 complete, 1 no table, 2 no range (none in the reply, and none on
// a later frame that answers the page within the walk's wait budget), 3 no movement,
// 4 page bound, 6 end not confirmed (a later frame carried an end row for the page, see
// pagePosition.endForPage; it cannot be tied to the walk's request, so the walk is
// still incomplete).
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
//
// A page's range is read from its reply first: the reply answers this request, so its
// rows are tied to it. Only when the reply decides nothing does the page look for a
// range row that a later frame carried to the live table path, waiting within the
// walk's one inboxRangeWait budget, as upstream waits for the range after each page
// (connector/threadbackfill.go). Such a row carries no request id (the library hands
// the event handler a parsed table only), so it is tied to the page by everything the
// library does expose: the connection epoch, the window, the sync group, the parent
// and the page's position (see lateCandidates). Even so it may only continue the walk,
// from a position strictly past the request's and not past the page's oldest thread;
// it never ends it. When the reply carries no thread rows at all, the page's oldest
// thread is unknown; a late row may then be checked against the oldest thread of the
// frame that carried it, and only on a watch opened after the socket's first sync (see
// pagePosition.frameFloor). Only a reply can complete the walk: a page whose end comes only on
// a later frame stops incomplete: 6 when that end row is one for the page (see
// pagePosition.endForPage; it changes neither the wait nor whether the walk goes on, only
// the stop code), else 2 as before. Every end row is counted. The late
// wait covers live frames only: a second data frame on the request's one-off stream is
// dropped by the library and only counted (oneoff_extra_frames). The watch opens before
// the request is sent and counts every range row that arrives while it is open,
// deciding or not.
func (s *messagixSession) walkInbox(ctx context.Context, cli threadPager, emit emitFunc, rows *inboxRowCounts, ref socket.FetchThreadsTask, limit int, done func() bool) (gap string, stop, pages int, next socket.FetchThreadsTask, err error) {
	const group = 1
	budget := inboxRangeWait
	var watch *rangeWatch
	defer func() { s.closeWatch(watch, rows) }()
	for batch := 0; batch < limit; batch++ {
		if err := ctx.Err(); err != nil {
			return "", 0, pages, ref, err
		}
		task := ref
		cursor := cli.GetCursor(group)
		task.Cursor = cursor
		task.SyncGroup = group
		watch = s.openWatch(cursor, rows)
		tbl, ids, err := s.fetchPage(ctx, cli, emit, task)
		s.setWatchIDs(watch, ids)
		if err != nil {
			return "", 0, pages, ref, err
		}
		if tbl == nil {
			return gapInboxIncomplete, 1, pages, ref, nil
		}
		pages++
		rows.reply.see(tbl, ids)
		if err := ctx.Err(); err != nil {
			return "", 0, pages, ref, err
		}
		if done != nil && done() {
			return "", 5, pages, ref, nil
		}
		replyThreads := threadRowCount(tbl)
		rows.replyThreads += replyThreads
		rng := replyRange(tbl, rows)
		endSeen := false
		if rng == nil {
			pos := pagePosition{ref: cursorPos{ts: ref.ReferenceActivityTimestamp, key: ref.ReferenceThreadKey}, postConnect: watch.postConnect}
			pos.floor, pos.floorOK = pageFloor(tbl)
			// An empty reply gives no floor; a late row may then use its own frame's, but only on a
			// watch opened after the socket's first sync (see rangeWatch.postConnect).
			pos.frameFloor = replyThreads == 0 && watch.postConnect
			var waited time.Duration
			rng, endSeen, waited, err = s.awaitInboxRange(ctx, watch, pos, max(budget, 0), rows)
			budget -= waited
			rows.waitMS += int(waited.Milliseconds())
			if err != nil {
				return "", 0, pages, ref, err
			}
			if rng != nil {
				rows.latePages++
			}
		}
		s.closeWatch(watch, rows)
		watch = nil
		if rng == nil && endSeen {
			return gapInboxIncomplete, 6, pages, ref, nil
		}
		if rng == nil {
			return gapInboxIncomplete, 2, pages, ref, nil
		}
		if !rng.more {
			return "", 0, pages, ref, nil
		}
		next := socket.FetchThreadsTask{ParentThreadKey: inboxParentKey, ReferenceThreadKey: rng.minKey, ReferenceActivityTimestamp: rng.minTS}
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
// returned as is, unapplied. ids are the request's packet and task ids when the client
// reports them (zero otherwise); they are only ever counted (see tieProbe).
func (s *messagixSession) fetchPage(ctx context.Context, cli threadPager, emit emitFunc, task socket.FetchThreadsTask) (*table.LSTable, messagix.RequestIDs, error) {
	task.IsAfter = 0
	task.AdditionalPagesToFetch = 0
	s.mu.Lock()
	mark := s.issueMarkLocked()
	s.mu.Unlock()
	tbl, ids, err := executeWithIDs(ctx, cli, &task)
	if err != nil {
		return nil, ids, mapSendErr(err)
	}
	if tbl == nil {
		return nil, ids, nil
	}
	s.handleReplyEmit(tbl, mark, func(f Frame) int64 {
		if ctx.Err() != nil {
			return 0
		}
		return emit(f)
	})
	return tbl, ids, nil
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
		tbl, _, err := s.fetchPage(ctx, cli, emit, task)
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
	if failed == 0 {
		// Every request scope answered: History may now read a thread this session
		// has not seen (see historyBlockedLocked).
		s.mu.Lock()
		s.scopeScanClean = true
		s.mu.Unlock()
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
	tbl, _, err := p.ExecuteTasksWithIDs(ctx, tasks...)
	return tbl, err
}

// ExecuteTasksWithIDs passes the request's ids through from the client it wraps.
func (p tallyPager) ExecuteTasksWithIDs(ctx context.Context, tasks ...socket.Task) (*table.LSTable, messagix.RequestIDs, error) {
	tbl, ids, err := executeWithIDs(ctx, p.threadPager, tasks...)
	if err == nil && tbl != nil && len(tasks) == 1 {
		if task, ok := tasks[0].(*socket.FetchThreadsTask); ok {
			p.tally.add(task, tbl)
		}
	}
	return tbl, ids, err
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

// inboxRowCounts accumulates one walk's range diagnostics, counts only. The walk never
// reads them to decide anything.
type inboxRowCounts struct {
	// The replies' group-1 LSUpsertSyncGroupThreadsRange rows by category, as before.
	total, inbox, parent0More, parent0End, other int
	// The replies' group-1 LSUpsertInboxThreadsRange rows, and their LSUpdateThreadsRangesV2 rows.
	inboxTable, v2 int
	// Thread rows in the walk's replies (the two thread tables pageFloor reads).
	replyThreads int
	// Range rows that later frames carried while a page's watch was open, by kind.
	late lateCounts
	// Pages a later frame's row decided; milliseconds spent waiting for one.
	latePages, waitMS int
	// Frames the library dropped during the walk (see frameDropHook): a second data
	// frame on a one-off stream, and a data frame for a stream it no longer has.
	oneoffExtra, unknownStream int
	// The replies' group-1 sync-group rows by parent class and more/end.
	replyClass rangeClass
	// The replies' task, sync-block and table-name diagnostics.
	reply replyDiag
	// The request ids of the live tables seen while a page's watch was open (see tieProbe).
	tie tieProbe
	// The end rows that stopped a page with 6, and what was counted beside them (see end6Counts).
	end6 end6Counts
	// Table names present in the later frames that carried a range row.
	lateNames map[string]bool
	// The connection epoch of the walk's first watch, and whether one was opened.
	firstEpoch int64
	opened     bool
}

// lateCounts is the range rows of later frames by table, kind and parent. "sync" is
// LSUpsertSyncGroupThreadsRange (group 1), "inbox" LSUpsertInboxThreadsRange (group 1),
// "v2" LSUpdateThreadsRangesV2. otherEpoch counts the rows that arrived after the
// connection epoch changed (they are dropped); rejected the more rows that could have
// continued the page but did not answer its position (see pagePosition.judge), split by
// reason into rejNoFloor, rejCeiling and rejFloor. rejEpoch is the more rows that could
// have continued the page but arrived after the epoch changed (a subset of otherEpoch;
// not in rejected). framesThreads is the late frames with a range row that also carried
// thread rows. The end rows (syncInboxEnd, syncP0End, inboxEnd) never decide anything:
// their counts are the late terminal rows the walk ignored.
type lateCounts struct {
	total                                                         int
	syncInboxMore, syncInboxEnd, syncP0More, syncP0End, syncOther int
	inboxMore, inboxEnd, v2Inbox, v2Other                         int
	otherEpoch, rejected                                          int
	rejNoFloor, rejCeiling, rejFloor, rejEpoch, framesThreads     int
	// syncOtherEnd is the end rows among syncOther.
	syncOtherEnd int
	// Rows of the task tables (LSRemoveTask, LSTaskExists, the task-completion table),
	// database-1 sync blocks, and the blocks whose current cursor is the one the page was
	// sent with, in the later frames that carried a range row.
	removeTask, taskExists, taskDone, db1Blocks, cursorSent int
	// resync is the database-1 resync markers that live tables carried while a watch
	// was open, in its epoch.
	resync int
}

func (c *lateCounts) add(r rangeRows) {
	c.total += r.len()
	for _, row := range r.sync {
		switch {
		case row.ParentThreadKey == inboxParentKey && row.HasMoreBefore:
			c.syncInboxMore++
		case row.ParentThreadKey == inboxParentKey:
			c.syncInboxEnd++
		case row.ParentThreadKey == 0 && row.HasMoreBefore:
			c.syncP0More++
		case row.ParentThreadKey == 0:
			c.syncP0End++
		default:
			c.syncOther++
			if !row.HasMoreBefore {
				c.syncOtherEnd++
			}
		}
	}
	for _, row := range r.inbox {
		if row.HasMoreBefore {
			c.inboxMore++
		} else {
			c.inboxEnd++
		}
	}
	for _, row := range r.v2 {
		if row.ParentThreadKey == inboxParentKey {
			c.v2Inbox++
		} else {
			c.v2Other++
		}
	}
}

func (c *lateCounts) merge(o lateCounts) {
	c.total += o.total
	c.syncInboxMore += o.syncInboxMore
	c.syncInboxEnd += o.syncInboxEnd
	c.syncP0More += o.syncP0More
	c.syncP0End += o.syncP0End
	c.syncOther += o.syncOther
	c.inboxMore += o.inboxMore
	c.inboxEnd += o.inboxEnd
	c.v2Inbox += o.v2Inbox
	c.v2Other += o.v2Other
	c.otherEpoch += o.otherEpoch
	c.rejected += o.rejected
	c.rejNoFloor += o.rejNoFloor
	c.rejCeiling += o.rejCeiling
	c.rejFloor += o.rejFloor
	c.rejEpoch += o.rejEpoch
	c.framesThreads += o.framesThreads
	c.syncOtherEnd += o.syncOtherEnd
	c.removeTask += o.removeTask
	c.taskExists += o.taskExists
	c.taskDone += o.taskDone
	c.db1Blocks += o.db1Blocks
	c.cursorSent += o.cursorSent
	c.resync += o.resync
}

// reject counts a candidate row that did not answer the page, by reason.
func (c *lateCounts) reject(why rejectReason) {
	c.rejected++
	switch why {
	case rejNoFloor:
		c.rejNoFloor++
	case rejCeiling:
		c.rejCeiling++
	case rejFloor:
		c.rejFloor++
	}
}

// addStats puts the range diagnostics on threads_done (the five reply categories are
// set by the caller, as before).
func (c *inboxRowCounts) addStats(stats map[string]int) {
	stats["inbox_rows_inbox_table"] = c.inboxTable
	stats["inbox_rows_v2"] = c.v2
	stats["inbox_rows_late"] = c.late.total
	stats["inbox_pages_late"] = c.latePages
	stats["inbox_range_wait_ms"] = c.waitMS
	stats["inbox_late_sync_inbox_more"] = c.late.syncInboxMore
	stats["inbox_late_sync_inbox_end"] = c.late.syncInboxEnd
	stats["inbox_late_sync_p0_more"] = c.late.syncP0More
	stats["inbox_late_sync_p0_end"] = c.late.syncP0End
	stats["inbox_late_sync_other"] = c.late.syncOther
	stats["inbox_late_inbox_more"] = c.late.inboxMore
	stats["inbox_late_inbox_end"] = c.late.inboxEnd
	stats["inbox_late_v2_inbox"] = c.late.v2Inbox
	stats["inbox_late_v2_other"] = c.late.v2Other
	stats["inbox_late_other_epoch"] = c.late.otherEpoch
	stats["inbox_late_rejected"] = c.late.rejected
	stats["inbox_late_rej_nofloor"] = c.late.rejNoFloor
	stats["inbox_late_rej_floor"] = c.late.rejFloor
	stats["inbox_late_rej_ceiling"] = c.late.rejCeiling
	stats["inbox_late_rej_epoch"] = c.late.rejEpoch
	stats["inbox_reply_threads"] = c.replyThreads
	stats["inbox_late_frames_threads"] = c.late.framesThreads
	stats["oneoff_extra_frames"] = c.oneoffExtra
	stats["oneoff_unknown_stream_frames"] = c.unknownStream
	stats["inbox_late_sync_other_end"] = c.late.syncOtherEnd
	stats["inbox_late_remove_task"] = c.late.removeTask
	stats["inbox_late_task_exists"] = c.late.taskExists
	stats["inbox_late_task_done"] = c.late.taskDone
	stats["inbox_late_db1_blocks"] = c.late.db1Blocks
	stats["inbox_late_cursor_sent"] = c.late.cursorSent
	stats["inbox_watch_resync"] = c.late.resync
	c.replyClass.addStats(stats, "inbox_rows_g1_")
	c.reply.addStats(stats)
	c.tie.addStats(stats)
	c.end6.addStats(stats)
}

// walkRange is one page's range decision: more says whether the walk goes on, and
// from where (the minimum thread key and activity time of what the page held).
type walkRange struct {
	more          bool
	minKey, minTS int64
}

// rangeRows is the range rows of one or more tables, from the three tables the server
// writes a thread range to (upstream logs the same three, connector handlemeta.go).
// Sync-group and inbox rows are kept for sync group 1 only; the V2 table carries no
// sync group, so all its rows are kept (they are only ever counted).
type rangeRows struct {
	sync  []*table.LSUpsertSyncGroupThreadsRange
	inbox []*table.LSUpsertInboxThreadsRange
	v2    []*table.LSUpdateThreadsRangesV2
}

func rangeRowsOf(tbl *table.LSTable) rangeRows {
	var r rangeRows
	if tbl == nil {
		return r
	}
	for _, row := range tbl.LSUpsertSyncGroupThreadsRange {
		if row.SyncGroup == 1 {
			r.sync = append(r.sync, row)
		}
	}
	for _, row := range tbl.LSUpsertInboxThreadsRange {
		if row.SyncGroup == 1 {
			r.inbox = append(r.inbox, row)
		}
	}
	r.v2 = append(r.v2, tbl.LSUpdateThreadsRangesV2...)
	return r
}

func (r rangeRows) len() int { return len(r.sync) + len(r.inbox) + len(r.v2) }

func (r *rangeRows) add(o rangeRows) {
	r.sync = append(r.sync, o.sync...)
	r.inbox = append(r.inbox, o.inbox...)
	r.v2 = append(r.v2, o.v2...)
}

// replyRange counts a reply's range rows and decides the page from them. The reply is
// the answer to the walk's own request, so its rows need no further tie to it:
//  1. the last group-1 sync-group row for the inbox parent, or a parent-0 end row
//     (the library ends a sync group only on a parent-0 no-more row,
//     syncManager.go updateThreadRanges), whichever comes last, as before;
//  2. else the last group-1 LSUpsertInboxThreadsRange row (a sync group and a
//     has-more flag, no parent: the inbox's own range);
//  3. else the last parent-0 more row, as a cursor, as before.
//
// V2 rows are counted only: that table has neither a sync group nor a has-more flag,
// so it never moves the cursor or completes a walk. Rows for any other parent decide
// nothing. nil means the reply has no range.
func replyRange(tbl *table.LSTable, counts *inboxRowCounts) *walkRange {
	r := rangeRowsOf(tbl)
	var decide, fallback *table.LSUpsertSyncGroupThreadsRange
	for _, row := range r.sync {
		counts.total++
		switch {
		case row.ParentThreadKey == inboxParentKey:
			counts.inbox++
			decide = row
		case row.ParentThreadKey == 0 && row.HasMoreBefore:
			counts.parent0More++
			fallback = row
		case row.ParentThreadKey == 0:
			counts.parent0End++
			decide = row
		default:
			counts.other++
		}
		counts.replyClass.add(row)
	}
	counts.inboxTable += len(r.inbox)
	counts.v2 += len(r.v2)
	if decide != nil {
		return &walkRange{more: decide.HasMoreBefore, minKey: decide.MinThreadKey, minTS: decide.MinLastActivityTimestampMS}
	}
	if n := len(r.inbox); n > 0 {
		row := r.inbox[n-1]
		return &walkRange{more: row.HasMoreBefore, minKey: row.MinThreadKey, minTS: row.MinLastActivityTimestampMs}
	}
	if fallback != nil {
		return &walkRange{more: true, minKey: fallback.MinThreadKey, minTS: fallback.MinLastActivityTimestampMS}
	}
	return nil
}

// cursorPos is a position in the inbox walk's order: a request's reference, a range
// row's minimum, a thread's place. The walk goes from newer to older; at the same
// activity time the smaller thread key is the older one, as a range row names its
// minimum key and minimum time together.
type cursorPos struct{ ts, key int64 }

// olderThan says whether p comes strictly after q in the walk.
func (p cursorPos) olderThan(q cursorPos) bool {
	return p.ts < q.ts || (p.ts == q.ts && p.key < q.key)
}

// pageFloor is the oldest position among the thread rows of a page's reply: the range
// that answers the page starts there. ok is false when the reply has no thread row
// with a time.
func pageFloor(tbl *table.LSTable) (floor cursorPos, ok bool) {
	see := func(key, ts int64) {
		if p := (cursorPos{ts: ts, key: key}); ts > 0 && (!ok || p.olderThan(floor)) {
			floor, ok = p, true
		}
	}
	if tbl == nil {
		return floor, false
	}
	for _, th := range tbl.LSDeleteThenInsertThread {
		see(th.ThreadKey, th.LastActivityTimestampMs)
	}
	for _, th := range tbl.LSUpdateOrInsertThread {
		see(th.ThreadKey, th.LastActivityTimestampMs)
	}
	return floor, ok
}

// threadRowCount is the number of thread rows in a table, in the two tables pageFloor reads.
func threadRowCount(tbl *table.LSTable) int {
	if tbl == nil {
		return 0
	}
	return len(tbl.LSDeleteThenInsertThread) + len(tbl.LSUpdateOrInsertThread)
}

// pagePosition is what a later frame's row is checked against: the page's request
// reference (its ceiling) and the oldest thread its reply carried (its floor). When the
// reply carried no thread rows at all, frameFloor lets a row use the oldest thread of
// the frame that carried it instead; it is set only on a watch opened after the socket's
// first sync, so the library's own connect-time thread fetches (which may start deeper
// in the inbox, from its saved state) can never lend a row a floor.
type pagePosition struct {
	ref         cursorPos
	floor       cursorPos
	floorOK     bool
	frameFloor  bool
	postConnect bool // the page's watch was opened after the socket's first sync
}

// possible says whether any late row could ever answer this page: without a floor from
// the reply or from the row's own frame, none can.
func (p pagePosition) possible() bool { return p.floorOK || p.frameFloor }

// floorFor is the floor a row carried by frame f is judged against: the reply's, else
// (with frameFloor) the frame's own oldest thread. ok is false when there is none.
func (p pagePosition) floorFor(f lateFrame) (floor cursorPos, ok bool) {
	if p.floorOK {
		return p.floor, true
	}
	if p.frameFloor {
		return f.floor, f.floorOK
	}
	return cursorPos{}, false
}

type rejectReason int

const (
	accepted rejectReason = iota
	rejNoFloor
	rejCeiling
	rejFloor
)

// judge says whether a more row whose minimum is at pos, carried by frame f, can
// continue the walk from this page, comparing the whole (time, key) position in the
// walk's order, and if not, why. It must be strictly older than the request's reference,
// so the walk moves forward, and not older than the page's oldest thread, so the next
// request skips none of this page's threads, not even one at the same time. That floor
// is the reply's; only when the reply had no thread rows (and frameFloor is set) is it
// the oldest thread of the row's own frame. A row with no floor is never taken. A row
// from deeper in the inbox (an older request's, a folder's, a cumulative range beside a
// single new thread) fails the floor; a row from a page before this one, or at the
// reference, fails the ceiling.
func (p pagePosition) judge(pos cursorPos, f lateFrame) rejectReason {
	floor, ok := p.floorFor(f)
	switch {
	case !ok:
		return rejNoFloor
	case !pos.olderThan(p.ref):
		return rejCeiling
	case pos.olderThan(floor):
		return rejFloor
	}
	return accepted
}

// isCandidateSync and isCandidateInbox say whether a group-1 range row could continue
// a page at all: a more row for the inbox parent or parent 0, or an inbox-table more row.
func isCandidateSync(row *table.LSUpsertSyncGroupThreadsRange) bool {
	return row.HasMoreBefore && (row.ParentThreadKey == inboxParentKey || row.ParentThreadKey == 0)
}

func isCandidateInbox(row *table.LSUpsertInboxThreadsRange) bool { return row.HasMoreBefore }

func candidateCount(r rangeRows) (n int) {
	for _, row := range r.sync {
		if isCandidateSync(row) {
			n++
		}
	}
	for _, row := range r.inbox {
		if isCandidateInbox(row) {
			n++
		}
	}
	return n
}

// lateCandidates splits later frames' rows into those that may continue the page, and
// counts the ones that could have but do not answer its position, by reason. A late row
// carries no request id, so it never concludes the walk: only the reply to the walk's
// own request can say there is nothing more. Only more rows may continue it (see
// isCandidateSync). End rows of every table are left out here (they are counted by
// kind, see lateCounts), and V2 never decides.
func lateCandidates(frames []lateFrame, p pagePosition, counts *lateCounts) (out rangeRows) {
	for _, f := range frames {
		for _, row := range f.rows.sync {
			if !isCandidateSync(row) {
				continue
			}
			if why := p.judge(cursorPos{ts: row.MinLastActivityTimestampMS, key: row.MinThreadKey}, f); why == accepted {
				out.sync = append(out.sync, row)
			} else {
				counts.reject(why)
			}
		}
		for _, row := range f.rows.inbox {
			if !isCandidateInbox(row) {
				continue
			}
			if why := p.judge(cursorPos{ts: row.MinLastActivityTimestampMs, key: row.MinThreadKey}, f); why == accepted {
				out.inbox = append(out.inbox, row)
			} else {
				counts.reject(why)
			}
		}
	}
	return out
}

// lateRange continues a page from the candidate rows of later frames, always with
// more: the last inbox-parent row, else the last inbox-table row, else the last
// parent-0 row. nil means no candidate yet.
func lateRange(r rangeRows) *walkRange {
	var inbox, p0 *table.LSUpsertSyncGroupThreadsRange
	for _, row := range r.sync {
		if row.ParentThreadKey == inboxParentKey {
			inbox = row
		} else {
			p0 = row
		}
	}
	if inbox != nil {
		return &walkRange{more: true, minKey: inbox.MinThreadKey, minTS: inbox.MinLastActivityTimestampMS}
	}
	if n := len(r.inbox); n > 0 {
		row := r.inbox[n-1]
		return &walkRange{more: true, minKey: row.MinThreadKey, minTS: row.MinLastActivityTimestampMs}
	}
	if p0 != nil {
		return &walkRange{more: true, minKey: p0.MinThreadKey, minTS: p0.MinLastActivityTimestampMS}
	}
	return nil
}

// rangeWatch gathers the range rows of live tables (frames that are not the reply to
// the walk's own request) while one inbox page is outstanding, frame by frame, each with
// the oldest thread its frame carried. It belongs to the connection epoch it was opened
// in: a row that arrives after the epoch changed (a reconnect, a disconnect, the offline
// sync's end) is counted and dropped, and the change wakes the waiter, which then stops.
// postConnect is whether it was opened after the socket's first sync completed (the link
// was up): the library's own connect-time thread fetches are answered before that. Its
// fields are guarded by s.mu; ready is signalled, never blocking, when rows are kept or
// the epoch changes.
//
// The rest is counted only, never decided on: cursor is the database-1 cursor the page
// was sent with; ids are the page request's ids once its reply returned; seen is every
// live table that reached the event handler while the watch was open, with its frame's
// request id (see tieProbe); resync is whether one of them carried a database-1 resync
// marker in the watch's epoch; names is the table names of the frames it kept.
type rangeWatch struct {
	epoch       int64
	postConnect bool
	frames      []lateFrame
	counts      lateCounts
	ready       chan struct{}

	cursor     string
	firstEpoch int64 // the epoch of the walk's first watch
	ids        messagix.RequestIDs
	seen       []liveSeen
	resync     bool
	names      map[string]bool
	closed     bool
}

// lateFrame is one live table's range rows and the oldest of its thread rows. The
// other fields serve endForPage and the counts beside stop 6: the newest thread row, the
// number of thread rows and whether any has no time, the frame's request tag, its
// task-table ids and its database-1 current cursors.
type lateFrame struct {
	rows    rangeRows
	floor   cursorPos
	floorOK bool

	newest  cursorPos
	threads int
	untimed bool
	keys    map[int64]bool
	tag     liveTag
	taskIDs []int64
	cursors []string
}

// openWatch opens the walk's watch for the page about to be requested, sent with
// cursor. Walks hold the walk slot, so at most one watch is open at a time. A new
// page's watch also restarts the last-page tally of position-rejected end rows (see
// end6Counts.rejLastPage), so only the last requested page's rejects are kept.
func (s *messagixSession) openWatch(cursor string, counts *inboxRowCounts) *rangeWatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !counts.opened {
		counts.opened, counts.firstEpoch = true, s.connEpoch
	}
	counts.end6.rejLastPage = 0
	w := &rangeWatch{epoch: s.connEpoch, postConnect: !s.linkDown, ready: make(chan struct{}, 1), cursor: cursor, firstEpoch: counts.firstEpoch}
	s.rangeWatch = w
	return w
}

// setWatchIDs records the page request's ids on its watch, for the counts only.
func (s *messagixSession) setWatchIDs(w *rangeWatch, ids messagix.RequestIDs) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w.ids = ids
}

// closeWatch closes w (if it is still the open one) and adds what it counted to counts,
// once.
func (s *messagixSession) closeWatch(w *rangeWatch, counts *inboxRowCounts) {
	if w == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.rangeWatch == w {
		s.rangeWatch = nil
	}
	if w.closed {
		return
	}
	w.closed = true
	counts.late.merge(w.counts)
	w.counts = lateCounts{}
	counts.tie.count(w)
	for name := range w.names {
		if counts.lateNames == nil {
			counts.lateNames = map[string]bool{}
		}
		counts.lateNames[name] = true
	}
}

// bumpEpochLocked starts a new connection epoch and wakes an open range watch, whose
// page can no longer be answered. The caller holds s.mu.
func (s *messagixSession) bumpEpochLocked() {
	s.connEpoch++
	if w := s.rangeWatch; w != nil {
		w.wake()
	}
}

func (w *rangeWatch) wake() {
	select {
	case w.ready <- struct{}{}:
	default:
	}
}

// offerLocked counts a live table's range rows and keeps them for the page, with the
// table's oldest thread, if the connection epoch is still the watch's. The caller holds
// s.mu.
func (s *messagixSession) offerLocked(w *rangeWatch, tbl *table.LSTable) {
	r := rangeRowsOf(tbl)
	if r.len() == 0 {
		return
	}
	w.counts.add(r)
	if threadRowCount(tbl) > 0 {
		w.counts.framesThreads++
	}
	f := lateFrame{rows: r, tag: s.liveTag, taskIDs: taskIDsOf(tbl)}
	w.counts.removeTask += len(tbl.LSRemoveTask)
	w.counts.taskExists += len(tbl.LSTaskExists)
	w.counts.taskDone += len(tbl.LSMailboxTaskCompletionApiOnTaskCompletion)
	for _, b := range tbl.GetLSExecuteFirstBlockForSyncTransactionV4() {
		if b.DatabaseID == 1 {
			w.counts.db1Blocks++
			f.cursors = append(f.cursors, b.CurrentCursor)
			if w.cursor != "" && b.CurrentCursor == w.cursor {
				w.counts.cursorSent++
			}
		}
	}
	if w.names == nil {
		w.names = map[string]bool{}
	}
	for _, name := range tbl.NonNilFields() {
		w.names[name] = true
	}
	if s.connEpoch != w.epoch {
		w.counts.otherEpoch += r.len()
		w.counts.rejEpoch += candidateCount(r)
		return
	}
	f.floor, f.floorOK = pageFloor(tbl)
	f.newest, f.threads, f.untimed, f.keys = frameThreads(tbl)
	w.frames = append(w.frames, f)
	w.wake()
}

// awaitInboxRange waits, at most budget, for a later frame to carry a more row that
// answers the page at pos, and continues the page from the candidates gathered so far
// each time rows arrive. A late row never ends the walk (see lateCandidates). It
// returns how long it waited. nil without an error means the page has no late range:
// the walk stops as before (stop 2). It stops waiting at once, before the budget is
// spent, when nothing more can be accepted: no row can have a floor (pos.possible), the
// connection epoch changed, or a candidate row arrived that could only be judged
// against its own frame and that frame had no thread rows (the page's range came apart
// from its threads; a later row would not be the answer to this page).
//
// Stop 6 means exactly: an end row for the page was seen (see pagePosition.endForPage)
// and no more row that answers the page arrived before the budget ran out. It is an
// unproven end, since the end row cannot be tied to the walk's own request, and it is
// always incomplete. An end row never shortens the wait and never changes whether the
// walk goes on: the page waits as it would without it, and a more row that answers the
// page still continues the walk. Its one effect is the stop code when the budget runs
// out: endSeen is then true (stop 6) instead of the no-range stop 2. Every early end of
// the wait keeps stop 2. A more row in the end row's own frame makes that end row not
// one for the page (stop 2, counted as inbox_end_rej_more_in_frame); a later frame's
// more row that does not answer the page leaves the end standing, and such frames are
// counted in inbox_end6_contradicted, so stop 6 can be split into ends that stood alone
// and ends that were contradicted. The decisions read no diagnostic counter. Only a
// cancel is an error.
func (s *messagixSession) awaitInboxRange(ctx context.Context, w *rangeWatch, pos pagePosition, budget time.Duration, counts *inboxRowCounts) (rng *walkRange, endSeen bool, waited time.Duration, err error) {
	start := time.Now()
	timer := time.NewTimer(budget)
	defer timer.Stop()
	var gathered rangeRows
	var end *lateFrame // the first end row for the page; it only picks the stop code
	contradicted := 0  // frames after it with more rows that did not answer the page (counted only)
	var endRow *table.LSUpsertSyncGroupThreadsRange
	for {
		s.mu.Lock()
		got := w.frames
		w.frames = nil
		moved := s.connEpoch != w.epoch
		s.mu.Unlock()
		if len(got) > 0 {
			noFloor := false
			for _, f := range got {
				if _, ok := pos.floorFor(f); !ok && candidateCount(f.rows) > 0 {
					noFloor = true // a candidate that can never be judged: see the doc comment
				}
			}
			gathered.add(lateCandidates(got, pos, &counts.late))
			if rng := lateRange(gathered); rng != nil {
				return rng, false, time.Since(start), nil
			}
			if noFloor {
				return nil, false, time.Since(start), nil
			}
			for i := range got {
				if end != nil && candidateCount(got[i].rows) > 0 {
					// After the end row, a frame whose more rows did not answer the page
					// (one that did would have continued the walk above): counted only.
					contradicted++
				}
				row := pos.endForPage(got[i])
				if row == nil {
					counts.end6.reject(w, pos, got[i])
				} else if end == nil {
					end, endRow = &got[i], row
				}
			}
		}
		if moved || !pos.possible() {
			return nil, false, time.Since(start), nil
		}
		select {
		case <-w.ready:
		case <-timer.C:
			if end == nil {
				return nil, false, time.Since(start), nil
			}
			s.mu.Lock()
			moved = s.connEpoch != w.epoch
			if !moved {
				counts.end6.count(s, w, pos, *end, endRow)
				counts.end6.contradicted += contradicted
			}
			s.mu.Unlock()
			return nil, !moved, time.Since(start), nil
		case <-ctx.Done():
			return nil, false, time.Since(start), ctx.Err()
		}
	}
}

func nowMS() int64 { return time.Now().UnixMilli() }
