# Notice

kluro-messagix is a small program that drives the Facebook Messenger protocol
for Kluro. It is licensed under the GNU Affero General Public License, version
3 or later (see LICENSE), because it is built on, and links, packages of
**mautrix-meta** (https://github.com/mautrix/meta, AGPL-3.0-or-later), pinned at
commit `2e7f46f1597af0e538e96c35e918b91987a8bb40`:

- `pkg/messagix` (Messenger lightspeed and encrypted-chat client)
- `pkg/messagix/securehistory` (encrypted history archive)

It also uses go.mau.fi/whatsmeow (MPL-2.0) for the encrypted-chat transport.

The program is a separate process. The Kluro connector (a different program,
under its own terms) talks to it only through the JSON-lines protocol in
`protocol.go` on the child's stdin and stdout. The connector keeps its own copy
of the message type definitions, which mirrors the ones in this program's
`protocol.go`; this program's licence covers this program, and not the
connector or any other Kluro software.
The complete corresponding source of this program is published at the same
place as any binary of it.
