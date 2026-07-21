#!/usr/bin/env bash
# Live E2E: send by handle, prove client_ref idempotency, observe the matching
# inbound self-send, then repeat through the exact resolved chat GUID.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] && set -a && . ./.env && set +a
: "${TEST_RECIPIENT:?set TEST_RECIPIENT in .env}"

BIN="${BIN:-./target/debug/blueski}"
RUN_UUID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
TEXT="${TEST_MESSAGE:-blueski live smoke test} [$RUN_UUID]"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$RUN_UUID"
E2E_STATE_DIR="$(mktemp -d "${TMPDIR:-/tmp}/blueski-e2e.XXXXXX")"
USE_SUPERVISOR="${BLUESKI_E2E_USE_SUPERVISOR:-0}"
DAEMON_PID=""

cleanup() {
  if [ -n "$DAEMON_PID" ]; then
    kill "$DAEMON_PID" >/dev/null 2>&1 || true
    wait "$DAEMON_PID" 2>/dev/null || true
  fi
  if [ -n "$E2E_STATE_DIR" ]; then
    rm -rf "$E2E_STATE_DIR"
  fi
}
trap cleanup EXIT

if [ "$USE_SUPERVISOR" = "1" ]; then
  rm -rf "$E2E_STATE_DIR"
  E2E_STATE_DIR=""
  "$BIN" up >/dev/null
else
  export BLUESKI_CONFIG_DIR="$E2E_STATE_DIR"
  export BLUESKI_PORT="${BLUESKI_E2E_PORT:-$((20000 + $$ % 20000))}"
  "$BIN" run >"$E2E_STATE_DIR/daemon.log" 2>"$E2E_STATE_DIR/daemon.err.log" &
  DAEMON_PID=$!
  for _ in $(seq 1 40); do
    if curl -fsS "http://127.0.0.1:$BLUESKI_PORT/healthz" >/dev/null; then
      break
    fi
    if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
      sed -n '1,160p' "$E2E_STATE_DIR/daemon.err.log" >&2
      exit 1
    fi
    sleep 0.25
  done
  curl -fsS "http://127.0.0.1:$BLUESKI_PORT/healthz" >/dev/null
fi
HANDLE_CLIENT_REF="live-smoke-handle-$RUN_ID"
RESPONSE="$("$BIN" send --to "$TEST_RECIPIENT" --text "$TEXT" --client-ref "$HANDLE_CLIENT_REF")"
MESSAGE_ID="$(printf '%s' "$RESPONSE" | sed -n 's/.*"message_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
test -n "$MESSAGE_ID"

# Retrying an accepted client_ref must return the original id without sending
# another copy. This exercises the durable HTTP acceptance idempotency path.
RETRY_RESPONSE="$("$BIN" send --to "$TEST_RECIPIENT" --text "$TEXT" --client-ref "$HANDLE_CLIENT_REF")"
RETRY_MESSAGE_ID="$(printf '%s' "$RETRY_RESPONSE" | sed -n 's/.*"message_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
if [ "$RETRY_MESSAGE_ID" != "$MESSAGE_ID" ]; then
  echo "client_ref retry returned a different message id" >&2
  exit 1
fi

CHAT_ID=""
PROTOCOL=""
INBOUND_MESSAGE_ID=""
for _ in $(seq 1 240); do
  EVENTS="$("$BIN" events --since 0)"
  STATUS_LINE="$(printf '%s\n' "$EVENTS" | grep "\"message_id\":\"$MESSAGE_ID\"" | grep '"event":"message.status"' | grep '"status":"sent"' | tail -1 || true)"
  INBOUND_LINE="$(printf '%s\n' "$EVENTS" | grep '"event":"message.received"' | grep -F "\"text\":\"$TEXT\"" | tail -1 || true)"
  if [ -n "$STATUS_LINE" ] && printf '%s' "$STATUS_LINE" | grep -q '"provider_message_id"'; then
    CHAT_ID="$(printf '%s' "$STATUS_LINE" | sed -n 's/.*"chat_id":"\([^"]*\)".*/\1/p')"
    PROTOCOL="$(printf '%s' "$STATUS_LINE" | sed -n 's/.*"protocol":"\([^"]*\)".*/\1/p')"
    INBOUND_MESSAGE_ID="$(printf '%s' "$INBOUND_LINE" | sed -n 's/.*"message_id":"\([^"]*\)".*/\1/p')"
    [ -n "$CHAT_ID" ] && [ -n "$PROTOCOL" ] && [ -n "$INBOUND_MESSAGE_ID" ] && break
  fi
  sleep 0.5
done

if [ -z "$CHAT_ID" ] || [ -z "$INBOUND_MESSAGE_ID" ]; then
  echo "provider/chat binding and matching inbound event were not both observed for $MESSAGE_ID" >&2
  exit 1
fi

CHAT_TEXT="$TEXT (chat-guid round trip)"
CHAT_RESPONSE="$("$BIN" send --chat-id "$CHAT_ID" --text "$CHAT_TEXT" --protocol "$PROTOCOL" --client-ref "live-smoke-chat-$RUN_ID")"
CHAT_MESSAGE_ID="$(printf '%s' "$CHAT_RESPONSE" | sed -n 's/.*"message_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p')"
test -n "$CHAT_MESSAGE_ID"

for _ in $(seq 1 240); do
  EVENTS="$("$BIN" events --since 0)"
  CHAT_STATUS_LINE="$(printf '%s\n' "$EVENTS" | grep "\"message_id\":\"$CHAT_MESSAGE_ID\"" | grep '"event":"message.status"' | grep '"status":"sent"' | grep "\"chat_id\":\"$CHAT_ID\"" | tail -1 || true)"
  CHAT_INBOUND_LINE="$(printf '%s\n' "$EVENTS" | grep '"event":"message.received"' | grep -F "\"text\":\"$CHAT_TEXT\"" | tail -1 || true)"
  if [ -n "$CHAT_STATUS_LINE" ] && [ -n "$CHAT_INBOUND_LINE" ]; then
    CHAT_INBOUND_MESSAGE_ID="$(printf '%s' "$CHAT_INBOUND_LINE" | sed -n 's/.*"message_id":"\([^"]*\)".*/\1/p')"
    echo "round trip correlated handle=$MESSAGE_ID inbound=$INBOUND_MESSAGE_ID chat=$CHAT_MESSAGE_ID chat_inbound=$CHAT_INBOUND_MESSAGE_ID uuid=$RUN_UUID"
    exit 0
  fi
  sleep 0.5
done

echo "chat-guid round trip was not observed for $CHAT_MESSAGE_ID" >&2
exit 1
