// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"fmt"
	"time"
)

// A reconnect ends every roster snapshot (see setLinkDown), and the only messagix
// route that returns participant rows is the inbox walk (FetchThreadsTask), which
// the connector otherwise starts on its own relist timer. The roster refresh runs
// that same walk once, from the top, right after the link comes back, so the
// two-person groups that Send depends on are certified again within seconds.
var (
	// rosterRefreshDebounce is the least time between two refresh starts; a reconnect
	// inside it replaces the pending refresh and waits out the rest.
	rosterRefreshDebounce = 60 * time.Second
	// rosterRefreshPages caps the refresh walk, well under threadBatchLimit: it only
	// has to re-see the groups it is for, and stops as soon as it has.
	rosterRefreshPages = 10
)

// rosterLogCodeCap bounds the counts in the refresh's diagnostic code.
const rosterLogCodeCap = 9

// acquireWalk takes the one network walk slot, or reports why the caller was
// cancelled first. The helper assumes one ExecuteTasks caller at a time.
func (s *messagixSession) acquireWalk(ctx context.Context) error {
	select {
	case s.walkSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *messagixSession) releaseWalk() { <-s.walkSem }

// scheduleRosterRefresh queues the refresh for a link that just came back up. The
// latest reconnect wins: a pending refresh is replaced and a running one is
// cancelled (its reply would be stamped with an ended epoch and certify nothing).
// Starts are at least rosterRefreshDebounce apart.
func (s *messagixSession) scheduleRosterRefresh() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopping || s.logoutOnly {
		return
	}
	if s.refreshTimer != nil {
		s.refreshTimer.Stop()
		s.refreshTimer = nil
	}
	if s.refreshCancel != nil {
		s.refreshCancel()
	}
	var delay time.Duration
	if !s.refreshLast.IsZero() {
		delay = max(rosterRefreshDebounce-time.Since(s.refreshLast), 0)
	}
	s.refreshGen++
	gen := s.refreshGen
	s.refreshTimer = time.AfterFunc(delay, func() { s.runRosterRefresh(gen) })
}

func (s *messagixSession) runRosterRefresh(gen int64) {
	s.mu.Lock()
	if s.stopping || gen != s.refreshGen {
		s.mu.Unlock()
		return
	}
	s.refreshTimer = nil
	var cli threadPager = s.refreshPager
	if cli == nil && s.client != nil {
		cli = s.client
	}
	emit := s.emit
	if cli == nil || emit == nil {
		s.mu.Unlock()
		return
	}
	base := s.ctx
	if base == nil {
		base = context.Background()
	}
	ctx, cancel := context.WithCancel(base)
	s.refreshCancel = cancel
	s.refreshLast = time.Now()
	s.workWG.Add(1)
	s.mu.Unlock()
	defer s.workWG.Done()
	defer cancel()
	if s.afterRefresh != nil {
		defer s.afterRefresh()
	}
	s.rosterRefresh(ctx, cli, func(f Frame) int64 { f.ID = 0; return emit(f) })
}

// rosterTargetsLocked is the threads a refresh is for: the typed two-person groups
// that could be certified at all. The caller holds s.mu.
func (s *messagixSession) rosterTargetsLocked() []*threadInfo {
	var out []*threadInfo
	for _, t := range s.threads {
		if !t.Enc && t.Typed && t.Kind == "group" && t.MemberCount == 2 && !s.unacceptedLocked(t.Key) && !s.supersededLocked(t) {
			out = append(out, t)
		}
	}
	return out
}

// rosterRefresh walks the inbox from the top, at most rosterRefreshPages requests,
// then the first page of every other scope (scanScopes) if a target is still
// unconfirmed, until every target group is certified again. It never touches inboxResume, and
// never re-emits known threads as incomplete (the connector's listing does that).
// Frames go out through emit as a normal walk's do; each page's rows are stamped
// with the epoch the page was issued in, so a reply that outlives a further
// disconnect certifies nothing. It logs one counts-only line:
// roster_refresh.p<pages>.s<stop>.m<groups still unconfirmed>.
func (s *messagixSession) rosterRefresh(ctx context.Context, cli threadPager, emit emitFunc) {
	s.mu.Lock()
	n := len(s.rosterTargetsLocked())
	s.mu.Unlock()
	if n == 0 {
		return
	}
	if err := s.acquireWalk(ctx); err != nil {
		return
	}
	defer s.releaseWalk()
	// Targets are taken once the walk slot is held, so a Threads walk that ran
	// first has already been applied.
	missing := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		m := 0
		for _, t := range s.rosterTargetsLocked() {
			if !s.membersCompleteLocked(t) {
				m++
			}
		}
		return m
	}
	done := func() bool {
		s.mu.Lock()
		down := s.linkDown
		s.mu.Unlock()
		return down || missing() == 0
	}
	rows := inboxRowCounts{}
	_, stop, pages, _, err := s.walkInbox(ctx, cli, emit, &rows, inboxTop, rosterRefreshPages, done)
	if err == nil && stop != 5 && !done() {
		// A target outside the main inbox (a Marketplace folder or request) is
		// only re-seen on its own scope's first page.
		var scoped int
		scoped, _, err = s.scanScopes(ctx, cli, emit, scopeParents(cli, nil), done)
		pages += scoped
	}
	if ctx.Err() != nil {
		return // superseded by a newer reconnect or stopping: it logs for itself
	}
	if err != nil {
		logCode("warn", "roster_refresh_failed")
		return
	}
	logCode("info", fmt.Sprintf("roster_refresh.p%d.s%d.m%d", min(pages, rosterLogCodeCap), stop, min(missing(), rosterLogCodeCap)))
}
