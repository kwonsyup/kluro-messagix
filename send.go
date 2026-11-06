// SPDX-License-Identifier: AGPL-3.0-or-later
package main

import (
	"context"
	"errors"
	"strconv"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waConsumerApplication"
	waTypes "go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/mautrix-meta/pkg/messagix/methods"
	"go.mau.fi/mautrix-meta/pkg/messagix/socket"
	"go.mau.fi/mautrix-meta/pkg/messagix/table"
)

const sendTimeout = 75 * time.Second

// Send sends one text to exactly the named thread. It never retries: a send
// whose outcome is not known is answered "unknown" so the connector reconciles
// by reading the thread, never by sending again.
func (s *messagixSession) Send(ctx context.Context, thread, otid, text string, expect []string) (*Sent, error) {
	enc, key, ok := parseWire(thread)
	if !ok || text == "" {
		return nil, errCode("bad_request")
	}
	s.mu.Lock()
	cli, ecli := s.client, s.e2ee
	var rosterErr error
	if cli != nil && expect != nil {
		rosterErr = s.expectLocked(thread, expect)
	}
	s.mu.Unlock()
	if cli == nil {
		return nil, errCode("not_started")
	}
	if rosterErr != nil {
		return nil, rosterErr
	}
	sctx, cancel := context.WithTimeout(ctx, sendTimeout)
	defer cancel()
	if enc {
		if ecli == nil {
			return nil, errCode("unavailable")
		}
		jid := waTypes.NewJID(strconv.FormatInt(key, 10), s.encryptedServer(thread))
		msg := &waConsumerApplication.ConsumerApplication{
			Payload: &waConsumerApplication.ConsumerApplication_Payload{
				Payload: &waConsumerApplication.ConsumerApplication_Payload_Content{
					Content: &waConsumerApplication.ConsumerApplication_Content{
						Content: &waConsumerApplication.ConsumerApplication_Content_MessageText{
							MessageText: &waCommon.MessageText{Text: proto.String(text)},
						},
					},
				},
			},
		}
		resp, err := ecli.SendFBMessage(sctx, jid, msg, nil, whatsmeow.SendRequestExtra{})
		if err != nil {
			return nil, mapSendErr(err)
		}
		return &Sent{Thread: thread, MessageID: "wa:" + resp.ID, OTID: otid}, nil
	}
	id, err := strconv.ParseInt(otid, 10, 64)
	if err != nil || id <= 0 {
		id = methods.GenerateEpochID()
	}
	task := &socket.SendMessageTask{
		ThreadId: key, Otid: id, Text: text,
		Source: table.MESSENGER_INBOX_IN_THREAD, InitiatingSource: table.FACEBOOK_INBOX,
		SendType: table.TEXT, SyncGroup: 1,
	}
	resp, err := cli.ExecuteTasks(sctx, task)
	if err != nil {
		return nil, mapSendErr(err)
	}
	sent := &Sent{Thread: thread, OTID: strconv.FormatInt(id, 10)}
	if resp != nil {
		for _, r := range resp.LSReplaceOptimsiticMessage {
			if r.OfflineThreadingId == sent.OTID {
				sent.MessageID = r.MessageId
			}
		}
	}
	return sent, nil
}

// expectLocked is nil only for an open group with a complete roster whose
// fresh others (seen in this connection epoch) are exactly expect, in any
// order. The caller holds s.mu.
func (s *messagixSession) expectLocked(thread string, expect []string) error {
	t := s.threads[thread]
	if t == nil || !s.membersCompleteLocked(t) {
		return errCode("roster_changed")
	}
	have := s.freshOthersLocked(t)
	if len(have) != len(expect) {
		return errCode("roster_changed")
	}
	want := map[string]bool{}
	for _, e := range expect {
		want[e] = true
	}
	if len(want) != len(expect) {
		return errCode("roster_changed")
	}
	for _, h := range have {
		if !want[h] {
			return errCode("roster_changed")
		}
	}
	return nil
}

// mapSendErr is the code of a failed provider call. A call that may have
// reached the provider is "unknown", the only code that asks for reconcile.
func mapSendErr(err error) error {
	var a *authErr
	switch {
	case errors.As(err, &a):
		return err
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
		return errCode("unknown")
	}
	if mapped := mapAuthError(err); mapped != nil {
		if errors.As(mapped, &a) {
			return mapped
		}
	}
	return errCode("unknown")
}
