# Upstream attribution

kluro-messagix is a program built on **mautrix-meta** (https://github.com/mautrix/meta), an AGPL-3.0-or-later
project. It is built against a modified fork of it. go.mod requires the upstream version and replaces it with the fork:

    go.mau.fi/mautrix-meta v0.2610.1-0.20261024105111-2e7f46f1597a   (upstream commit 2e7f46f1597a)
      => github.com/kwonsyup/mautrix-meta v0.0.0-20261111115334-a349dea558a1   (fork commit a349dea558a1)

The fork is upstream commit `2e7f46f1597af0e538e96c35e918b91987a8bb40` plus one commit,
`a349dea558a1853cda7856beb212bf92c3cab435` (2026-11-11), on the branch `kluro-request-id` of
https://github.com/kwonsyup/mautrix-meta. That commit changes only `pkg/messagix/socket.go` and
`pkg/messagix/threads.go`: it adds `Client.ExecuteTasksWithIDs`, which lets a caller read the request id and task ids
that `pkg/messagix` sends with a request, and `FrameRequestID`, which reads the request id of a live frame, without
changing what is sent. The complete source of the modified library, under the same AGPL-3.0-or-later license, is on that
branch and in the module archive listed in THIRD-PARTY.md.

The program imports the packages `pkg/messagix` and `pkg/messagix/securehistory` from the fork as a Go module
dependency, verified by go.sum. Parts of `session.go` follow the structure of the upstream `pkg/connector` flows
(`connectE2EE`, `FullReconnect`, `restoreHistory`); comments there that say "the connector" mean that upstream
package, not the Kluro connector.

## Copyright holders named in the pinned source

Counted from the `// Copyright (C)` headers of the .go files of the fork at a349dea558a1, which are the same as those of
upstream 2e7f46f1597a:

- Copyright (C) 2024 Tulir Asokan (11 header lines)
- Copyright (C) 2025 Tulir Asokan (1 header line)
- Copyright (C) 2026 Tulir Asokan (51 header lines)
- Copyright (C) 2026 Killian Lelong (10 header lines)

Upstream ships no NOTICE file. Its LICENSE is the GNU AGPL version 3, copied in this repository as `LICENSE`, and
`THIRD-PARTY-LICENSES/go.mau.fi_mautrix-meta@.../` holds the LICENSE and LICENSE.exceptions unmodified. The folder is
named for the required module version, as for the other replaced module (github.com/imroc/req/v3). The fork's LICENSE and
LICENSE.exceptions are byte-identical to upstream's.

## Upstream LICENSE.exceptions

The upstream grants extra rights to Beeper and Element only (embedding in their clients and server bundles). Kluro
receives no exception: this program and every binary of it are conveyed under AGPL-3.0-or-later alone.

## Other copyleft components

go.mau.fi/libsignal (GPL-3.0) and the MPL-2.0 modules (whatsmeow, go.mau.fi/util, go.mau.fi/zeroconfig,
maunium.net/go/mautrix) are listed in THIRD-PARTY.md with their license texts.
