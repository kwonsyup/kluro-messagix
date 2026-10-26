# Upstream attribution

kluro-messagix is a program built on **mautrix-meta** (https://github.com/mautrix/meta), an AGPL-3.0-or-later
project, at the exact version pinned in go.mod:

    go.mau.fi/mautrix-meta v0.2610.1-0.20261024105111-2e7f46f1597a   (upstream commit 2e7f46f1597a)

It imports the upstream packages `pkg/messagix` and `pkg/messagix/securehistory` as a Go module dependency (unmodified,
fetched from the Go module proxy and verified by go.sum). Parts of `session.go` follow the structure of the upstream
`pkg/connector` flows (`connectE2EE`, `FullReconnect`, `restoreHistory`); comments there that say "the connector" mean
that upstream package, not the Kluro connector.

## Copyright holders named in the pinned upstream source

Counted from the `// Copyright (C)` headers of the pinned upstream .go files:

- Copyright (C) 2024 Tulir Asokan (11 header lines)
- Copyright (C) 2025 Tulir Asokan (1 header line)
- Copyright (C) 2026 Tulir Asokan (51 header lines)
- Copyright (C) 2026 Killian Lelong (10 header lines)

Upstream ships no NOTICE file. Its LICENSE is the GNU AGPL version 3, copied in this repository as `LICENSE`, and
`THIRD-PARTY-LICENSES/go.mau.fi_mautrix-meta@.../` holds the upstream LICENSE and LICENSE.exceptions unmodified.

## Upstream LICENSE.exceptions

The upstream grants extra rights to Beeper and Element only (embedding in their clients and server bundles). Kluro
receives no exception: this program and every binary of it are conveyed under AGPL-3.0-or-later alone.

## Other copyleft components

go.mau.fi/libsignal (GPL-3.0) and the MPL-2.0 modules (whatsmeow, go.mau.fi/util, go.mau.fi/zeroconfig,
maunium.net/go/mautrix) are listed in THIRD-PARTY.md with their license texts.
