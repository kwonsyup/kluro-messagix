// SPDX-License-Identifier: AGPL-3.0-or-later
package main

// The helper holds no message: every message it converts goes out at once (the
// withholding for marked threads was reverted). What it does decide is what each
// thread frame says, and the connector holds a thread's live messages until that
// frame qualifies (an inert request frame never does). msgKinds counts the messages
// the helper emitted by what it knew of their thread at that moment, so the counts say
// how many went to threads it reports as inert or incomplete. Counts only: no id, no
// title, no name. Which threads the connector accepted is decided in the connector
// (from the thread frame), so the helper does not count that.

// Message thread kinds, in order of precedence (a message counts once, in the first
// kind that fits its thread):
const (
	msgKindUnknown      = iota // a thread no row named before this message, or one with no kind, or a one-to-one thread naming no other person
	msgKindEncMarked           // an encrypted thread with a request or spam mark (its frame is inert)
	msgKindEnc                 // any other encrypted thread
	msgKindSpam                // an open thread whose latest folder is spam (inert)
	msgKindRequest             // any other message request: pending folder or request status 1, not an accepted Marketplace thread (inert)
	msgKindMarketplace         // a Marketplace thread (type 5), including a pending one the helper accepts
	msgKindFolder              // a thread whose latest folder is another one (not the inbox, pending or spam)
	msgKindGroupUntyped        // a group whose type the helper has not learned
	msgKindOpen                // none of the above
	msgKindCount
)

var msgKindNames = [msgKindCount]string{"unknown", "enc_marked", "enc", "spam", "request", "marketplace", "folder", "group_untyped", "open"}

// msgKindLocked is the kind of the thread a message to wire thread (enc, key) goes to;
// known is whether the helper had a record of that thread before the message. The
// caller holds s.mu.
func (s *messagixSession) msgKindLocked(enc bool, key int64, known bool) int {
	t := s.threads[wireID(enc, key)]
	switch {
	case t == nil || !known:
		return msgKindUnknown
	case enc && s.encMarkedLocked(key, s.requestMarkedLocked):
		return msgKindEncMarked
	case enc:
		return msgKindEnc
	case s.spamMark[key]:
		return msgKindSpam
	case s.unacceptedLocked(key):
		return msgKindRequest
	case s.marketplaceLocked(key):
		return msgKindMarketplace
	case s.otherFolder[key]:
		return msgKindFolder
	case t.Kind == "group" && !t.Typed:
		return msgKindGroupUntyped
	case t.Kind == "" || (t.Kind == "user" && len(s.othersLocked(t)) == 0):
		return msgKindUnknown
	}
	return msgKindOpen
}

// othersLocked is a thread's participants other than self.
func (s *messagixSession) othersLocked(t *threadInfo) (out []int64) {
	for _, p := range t.Participants {
		if p != s.selfID {
			out = append(out, p)
		}
	}
	return out
}

// countMsgLocked counts one emitted message by its thread's kind. The caller holds s.mu.
func (s *messagixSession) countMsgLocked(enc bool, key int64, known bool) {
	s.msgKinds[s.msgKindLocked(enc, key, known)]++
}

func (s *messagixSession) addMsgKindStats(stats map[string]int) {
	for i, name := range msgKindNames {
		stats["emitted_msgs_"+name] = s.msgKinds[i]
	}
}
