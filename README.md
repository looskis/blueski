<p align="center">
  <img src="assets/blueski-icon.png" alt="Blueski logo" width="128" height="128">
</p>

# Blueski

Blueski is a small, AppleScript-only macOS daemon for sending and receiving
Messages. It exposes a loopback HTTP API and CLI, watches the local Messages
database for inbound messages and delivery state, journals every event locally,
and can forward signed webhooks.

There is no injected library, private Messages framework, or native helper.
Outbound messages always travel through `osascript` and Messages.app.

## Requirements

- macOS 12 or newer
- Messages.app signed into an Apple ID
- Automation permission to control Messages.app
- Full Disk Access to read `~/Library/Messages/chat.db`

Blueski binds only to `127.0.0.1`. The local API is unauthenticated by default;
`blueski publish` enables bearer authentication before exposing it through its
supervised ngrok tunnel. Do not configure another proxy without equivalent
authentication.

## Install

Build from source:

```sh
cargo install --locked --path .
blueski setup
```

Install the current release with Homebrew:

```sh
brew install looskis/tap/blueski
blueski setup
```

`blueski setup` installs and starts a per-user LaunchAgent, verifies the
canonical port, and walks through Full Disk Access and Messages Automation.
When installed as an app bundle it relaunches setup through LaunchServices so
the grants attach to Blueski's stable application identity. The command waits
for the grants and finishes with machine-readable readiness JSON.

Homebrew builds are ad-hoc signed, so macOS may require those grants again after
an upgrade or reinstall. Run `blueski setup` again if `blueski status` reports a
missing permission.

After setup, `launchd` starts Blueski at login and restarts it after crashes.
Every agent-facing CLI command checks the fast health endpoint and asks the OS
supervisor to start Blueski when necessary. If Homebrew Services already owns
the daemon, Blueski reuses that supervisor instead of installing a second one.

To fully remove a Homebrew installation, stop both current and legacy
supervisors before uninstalling the formula:

```sh
blueski uninstall
brew services stop blueski 2>/dev/null || true
brew uninstall looskis/tap/blueski
```

Configuration and message history remain in `~/.config/blueski` so a reinstall
can reuse them. Remove that directory separately only if you want to erase the
local Blueski state.

For local development, `scripts/bundle.sh` creates `dist/Blueski.app` with the
Blueski icon and bundle identity. Set `SIGN_ID` to a stable signing identity if
you want TCC grants to survive builds.

## Use

```sh
blueski status
blueski doctor
blueski up
blueski send --to "+14155551234" --text "hello"
blueski send --chat-id "iMessage;-;chat-guid" --text "reply to this chat"
blueski send "+14155551234" "positional form"
blueski events --since 0
blueski events --follow
blueski publish --domain blueski.example.com
blueski unpublish
blueski down
```

`blueski down` unloads all Blueski supervisors. The next `up`, `status`, `send`,
or `events` command loads or starts it again. `blueski doctor` is the explicit
slow path for live AppleScript and protected-file permission probes.

`blueski publish` installs a second LaunchAgent for ngrok and generates an API
bearer token before the tunnel is started. Its final JSON includes the public
URL and `Authorization` value. Remote callers must send that header on every
request. `blueski unpublish` removes the tunnel and disables the token.

### HTTP API

The canonical OpenAPI 3.1 contract is checked in as [`openapi.json`](openapi.json)
and served by every running node at `GET /openapi.json`. It documents the REST
endpoints, conditional bearer authentication, schemas, examples, and the signed
outbound webhook operation.

Send a message:

```http
POST /messages
Content-Type: application/json

{
  "to": "+14155551234",
  "text": "hello",
  "protocol": "imessage",
  "client_ref": "example-123"
}
```

Use exactly one of `to` or `chat_id`. `chat_id` is the stable `chat.guid`
returned on events and works for direct and group conversations. A successful
queue operation returns `202 Accepted` with a generated `message_id` only after
the complete job and its `message.queued` event have committed to `state.db`.
The response includes `"idempotent": true` when `client_ref` was supplied and
`false` when the request is intentionally non-idempotent.

`client_ref` is an installation-scoped idempotency key when it is non-null.
Repeating a request with the same value returns the original `message_id` and
does not dispatch another message only when the target kind, target value,
protocol, and text are also identical. Reusing the key with any of those fields
changed returns `409 Conflict`. Existing correlation-only `client_ref` values
do not enter this namespace: the first request accepted after upgrading to
0.2.0 establishes the fingerprinted binding for that value.

Other endpoints:

- `GET /openapi.json` — machine-readable OpenAPI 3.1 contract
- `GET /healthz` — immediate, side-effect-free liveness and product identity
- `GET /status` — cached daemon and permission state; never invokes AppleScript
- `GET /messages?since=<cursor>&limit=<n>` — latest journal entry per message
- `GET /messages/:id` — complete durable lifecycle for one message
- `GET /events?since=<cursor>&limit=<n>` — durable event journal
- `GET /events/stream?since=<cursor>` — newline-delimited live event stream

Events include `message.queued`, `message.sent`, `message.failed`,
`message.received`, and `message.status`. Each configured `[[webhooks]]`
destination receives the same events with an `X-Blueski-Signature` HMAC-SHA256
header. The legacy `webhook_url` and `hmac_secret` fields continue to load as
one destination named `legacy`.

Every webhook body is the same journaled envelope returned by `GET /events`,
including `installation_id`, its local cursor `id`, `created_at`, optional
`chat_id`, and optional `provider_message_id`. The signature covers those exact
raw JSON bytes. Each enabled destination has a bounded ordered worker, its own
retry loop, and a distinct secret, so a slow endpoint does not delay another.

Inbound events include optional `destination_caller_id`, copied directly from
Messages `message.destination_caller_id`: the local phone number or email
address that received the message. Formatting is preserved, and missing/blank
values are omitted rather than inferred from chat routing hints. Consumers
can filter this field to subscribe to a particular receiving address. Older
journal entries are unchanged and may lack it.

Each inbound provider message produces one actionable `message.received`
event, after BlueSki has attempted chat classification. The event includes
`thread_kind`, nullable `participant_count`, `membership_complete`,
`classification_basis`, and `classification_conflict`. BlueSki never emits a
pre-classification inbound webhook followed by a second actionable copy.
Membership that cannot be resolved safely is published as `unclassified`;
contradictory direct/group evidence is also `unclassified` with
`classification_conflict=true`.

Lifecycle semantics:

- `message.sent` means Messages.app accepted the AppleScript send.
- `message.status` with `status=sent` means BlueSki resolved and durably bound
  the Apple message GUID and Messages chat GUID.
- Later `message.status` events report `delivered` and `read`.
- If AppleScript succeeds before BlueSki can bind the provider row, the send
  remains `sent_unbound` and reconciliation retries every 30 seconds. BlueSki
  never blindly resends and never turns a delayed binding into terminal loss.

Inbound rows are durably captured before the Messages receive watermark
advances. Rows missing their handle, chat GUID, or body stay in the enrichment
queue and are not published as `message.received`. The receive worker re-reads
them until complete, quarantines and surfaces overdue rows in `/status`, and
survives daemon restarts without advancing past them silently. Historical
incomplete events from older versions are not replayed automatically. Journal
entries are replayable by cursor. Webhook delivery is best effort; consumers
recover gaps through `/events?since=<cursor>` and deduplicate deliveries by
`(installation_id, event.id)`. If a crash causes the same inbound provider
message to be journaled again, deduplicate its semantic processing by
`(installation_id, provider_message_id, event kind/status)`.

## Configuration and state

The first command creates `~/.config/blueski/config.toml`:

```toml
installation_id = "bsinst_0123456789abcdef0123456789abcdef"
port = 8788
api_token = "bs_..." # generated only by `blueski publish`

[[webhooks]]
id = "looski-local"
url = "http://127.0.0.1:3001/webhooks/blueski"
secret = "generate-a-unique-secret"
enabled = true

[[webhooks]]
id = "audit-service"
url = "https://events.example.com/blueski"
secret = "generate-a-different-secret"
enabled = true
```

Webhook IDs and secrets must be nonempty, IDs and secrets must be unique, and
at most 16 destinations may be configured. Plain HTTP is accepted only for
loopback destinations; remote destinations require HTTPS. Configuring both
legacy `webhook_url` and `[[webhooks]]` is an error. `/status` reports only each
destination's ID, enabled state, last success time, and safe error category; it
never returns webhook URLs, secrets, or signatures.

State lives in the same directory:

- `state.json` — last observed Messages row
- `state.db` — durable events and outbound correlation
- `daemon.pid` — informational PID for the launchd-owned process

LaunchAgent logs are written to `blueski.log` and `blueski.err.log` in the same
state directory.

## Development and verification

```sh
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
bash -n scripts/*.sh
```

For a real outbound test, copy `.env.example` to `.env`, set
`TEST_RECIPIENT`, and run `scripts/smoke.sh`. This sends two UUID-tagged real
messages: the first by handle, then a second using the exact resolved
`chat.guid`. It verifies `client_ref` retry idempotency, provider correlation,
matching inbound self-send events, and the chat identity round trip. Set
`BLUESKI_E2E_USE_SUPERVISOR=1` to exercise the signed launchd-managed app and
its macOS permissions.

## Homebrew release checklist

The canonical formula lives in `looskis/homebrew-tap`, includes stable and head
builds, and delegates supervision to `blueski setup`. For each new version:

1. Tag `vX.Y.Z`; the release workflow uploads a deterministic source archive
   and SHA-256 file.
2. Update the release asset URL and SHA-256 in
   `looskis/homebrew-tap/Formula/blueski.rb`.
3. Run `brew audit --strict --online blueski`,
   `brew install --build-from-source blueski`, and `brew test blueski`.

Homebrew recommends a separate `homebrew-` repository for short tap names; see
the [official tap guide](https://docs.brew.sh/How-to-Create-and-Maintain-a-Tap).

## License

MIT

### Inbound attachments

`GET /messages/{provider_message_id}/attachments` returns inbound message scope (`message_id`, `chat_id`, `destination`, `peer`, `protocol`) and up to nine attachment descriptors (`id`, `mime_type`, `bytes`). `GET /messages/{provider_message_id}/attachments/{id}` downloads only an attachment joined to that inbound message. Both use the existing API authentication policy and `Cache-Control: no-store`. No filesystem paths are exposed or accepted. Downloads are restricted to canonical paths under Messages' Attachments directory, regular files, and 20 MiB. Missing, not-yet-downloaded, oversized, or inaccessible files return a generic error. Consumers must enforce their own receiving-address/thread policy before requesting attachments. Photo-only inbound messages are journaled with `[Attachment]` when there is no decodable text.

## Default sender for new conversations

`blueski default-sender` opens Messages → Settings → iMessage and returns the
current selection plus available phone/email addresses. To change it:

```sh
blueski default-sender +16469921008
```

The equivalent daemon API is `GET /settings/default-sender` and
`POST /settings/default-sender` with `{"address":"+16469921008"}`. It uses the
same bearer authentication as other endpoints. Success returns
`{"selected":"+16469921008","available":["+16469921008"],"scope":"new_conversations"}`
(the actual list contains all available addresses).

This uses AppleScript UI automation. Enable **Accessibility** for Blueski in
System Settings → Privacy & Security, and allow its Automation access to
Messages and System Events. A logged-in interactive session is required.
The implementation targets the English Messages settings UI inspected on this
Mac; UI changes/localization can cause a clear error instead of a selection.
It selects only an exact normalized available address and verifies the result.
No messages are sent by this command.

This setting is persistent and shared by all applications using Messages.
It controls new conversations; replies to existing chats keep their routing.
It is not a per-message `from` guarantee, and changing it then sending is not
an atomic operation. Avoid switching it around concurrent sends. A timeout
can occur after a selection changes: read the current value before retrying.
No signing, Full Disk Access, or receiving-address configuration is changed.
