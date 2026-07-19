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

- `GET /healthz` — immediate, side-effect-free liveness and product identity
- `GET /status` — cached daemon and permission state; never invokes AppleScript
- `GET /messages?since=<cursor>&limit=<n>` — latest journal entry per message
- `GET /messages/:id` — complete durable lifecycle for one message
- `GET /events?since=<cursor>&limit=<n>` — durable event journal
- `GET /events/stream?since=<cursor>` — newline-delimited live event stream

Events include `message.queued`, `message.sent`, `message.failed`,
`message.received`, and `message.status`. If `webhook_url` is configured, the
same events are POSTed with an `X-Blueski-Signature` HMAC-SHA256 header.

## Configuration and state

The first command creates `~/.config/blueski/config.toml`:

```toml
port = 8788
webhook_url = "https://example.com/blueski/events" # optional
hmac_secret = "replace-me"
api_token = "bs_..." # generated only by `blueski publish`
```

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
`TEST_RECIPIENT`, and run `scripts/smoke.sh`. This sends a real message and
requires working Messages permissions.

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
