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

Blueski binds only to `127.0.0.1`. It is not an authenticated network service
and should not be exposed through a proxy without adding authentication.

## Install

Build from source:

```sh
cargo install --locked --path .
blueski setup
blueski install
```

Until the first versioned release is published, the included head-only Homebrew
formula can be tested from a checkout:

```sh
brew install --HEAD --formula ./Formula/blueski.rb
blueski setup
brew services start blueski
```

`blueski install` installs its own per-user LaunchAgent. Do not use it at the
same time as `brew services`; choose one supervisor.

For local development, `scripts/bundle.sh` creates `dist/Blueski.app`. Set
`SIGN_ID` to a stable signing identity if you want TCC grants to survive builds.

## Use

```sh
blueski status
blueski up
blueski send --to "+14155551234" --text "hello"
blueski send "+14155551234" "positional form"
blueski events --since 0
blueski events --follow
blueski down
```

The CLI automatically starts the daemon for `send` and `events` if it is not
already running.

### HTTP API

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

Use exactly one of `to` or `chat_id`. A successful queue operation returns
`202 Accepted` with a generated `message_id`.

Other endpoints:

- `GET /status` — health, permissions, and `transport: "applescript"`
- `GET /events?since=<cursor>&limit=<n>` — durable event journal
- `GET /events/stream?since=<cursor>` — newline-delimited live event stream

Events include `message.queued`, `message.sent`, `message.failed`,
`message.received`, and `message.status`. If `webhook_url` is configured, the
same events are POSTed with an `X-Blueski-Signature` HMAC-SHA256 header.

## Configuration and state

The first command creates `~/.config/blueski/config.toml`:

```toml
port = 8787
webhook_url = "https://example.com/blueski/events" # optional
hmac_secret = "replace-me"
```

State lives in the same directory:

- `state.json` — last observed Messages row
- `state.db` — durable events and outbound correlation
- `daemon.pid` — process started by `blueski up`

LaunchAgent logs are written to `/tmp/blueski.log` and `/tmp/blueski.err`.

## Development and verification

```sh
cargo fmt --check
cargo test --locked
cargo clippy --all-targets --all-features -- -D warnings
bash -n scripts/*.sh
```

For a real outbound test, copy `.env.example` to `.env`, set
`TEST_RECIPIENT`, and run `scripts/smoke.sh`. This sends a real message and
requires working Messages permissions.

## Homebrew release checklist

`Formula/blueski.rb` is a usable head formula and includes a `brew services`
definition. For a versioned release:

1. Tag `vX.Y.Z`; the release workflow uploads a deterministic source archive
   and SHA-256 file.
2. Add the release asset URL and SHA-256 to the formula.
3. Copy the formula to `raz-team/homebrew-tap/Formula/blueski.rb`.
4. Run `brew audit --strict --online blueski`,
   `brew install --build-from-source blueski`, and `brew test blueski`.

Homebrew recommends a separate `homebrew-` repository for short tap names; see
the [official tap guide](https://docs.brew.sh/How-to-Create-and-Maintain-a-Tap).

## License

MIT
