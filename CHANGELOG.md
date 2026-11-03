# Changelog

## Unreleased

### Push keep-alive

- Add `blueski keepalive install|uninstall|status`. `install` (root) applies
  kernel TCP keep-alive probes and loads a LaunchDaemon that reapplies them at
  boot, so a NAT that drops apsd's idle push session is noticed within about
  three minutes instead of apsd's own 20–30 minute keep-alive. Messages sent in
  that window previously arrived late in a burst on reconnect.
- `blueski doctor` reports the effective values under `push_keepalive`.

## 0.2.0 — 2026-09-09

### Receiving addresses and sender settings

- Preserve `destination_caller_id` in inbound webhooks and recovered journal
  events so consumers can route by receiving phone number or email.
- Add `blueski default-sender [ADDRESS]` and `GET/POST /settings/default-sender`
  for Messages' app-wide default for new conversations. This uses UI scripting,
  requires Accessibility/Automation permission, and does not override existing
  chat routing. Live selection verification remains pending on the development
  Mac because macOS still rejects Accessibility access.
- Expose message-scoped attachment metadata and downloads, with bounded reads,
  path containment, and the existing API authentication.
- Capture attachment-only inbound messages and incomplete rows for later
  enrichment; expose pending/quarantined receive counts in `/status`.
- Retry send correlation after delayed provider binding without blindly
  resending a message or treating a temporarily missing binding as terminal.

### Durable delivery

- Journal events before fan-out to independently signed webhook destinations.
- Support cursor recovery, durable send idempotency, and independent webhook
  retries. Legacy single-webhook configuration remains supported.


### Inbound chat classification

- `message.received` now carries `thread_kind`, `participant_count`,
  `membership_complete`, `classification_basis`, and
  `classification_conflict`.
- Direct chats require complete membership with exactly one remote participant,
  no group metadata, and a direct-form Messages chat GUID.
- Group chats are identified by multiple remote participants, room metadata, or
  a group-form Messages chat GUID.
- Contradictory evidence fails closed as `unclassified`.
- BlueSki emits one actionable inbound webhook only after classification has
  been attempted. It does not emit separate pre- and post-classification
  `message.received` events.
- These fields describe Messages.app provider state only. BlueSki does not
  receive, store, or reconcile Looski user or chat-session identifiers.
