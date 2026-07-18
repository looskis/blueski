#!/usr/bin/env bash
# Live smoke test: up -> send one real message -> wait for message.sent -> down.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
: "${TEST_RECIPIENT:?set TEST_RECIPIENT in .env}"

BIN="${BIN:-./target/debug/blueski}"
TEXT="${TEST_MESSAGE:-blueski live smoke test $(date -u +%Y-%m-%dT%H:%M:%SZ)}"

cleanup() {
  "$BIN" down >/dev/null 2>&1 || true
}
trap cleanup EXIT

"$BIN" up >/dev/null
RESPONSE="$("$BIN" send --to "$TEST_RECIPIENT" --text "$TEXT" --client-ref "live-smoke")"
MESSAGE_ID="$(printf '%s' "$RESPONSE" | sed -n 's/.*"message_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
test -n "$MESSAGE_ID"

for _ in $(seq 1 240); do
  EVENTS="$("$BIN" events --since 0)"
  if printf '%s\n' "$EVENTS" | grep "\"message_id\":\"$MESSAGE_ID\"" | grep '"event":"message.status"' | grep -Eq '"status":"(delivered|read)"'; then
    echo "message delivered $MESSAGE_ID"
    exit 0
  fi
  sleep 0.5
done

echo "delivery was not observed for $MESSAGE_ID" >&2
exit 1
