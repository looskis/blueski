#!/usr/bin/env python3
"""Print a redacted summary from the daemon's /debug/chatdb endpoint."""

import json
import sys
import time
import urllib.error
import urllib.request


URL = "http://127.0.0.1:8787/debug/chatdb"


def fetch():
    started = time.time()
    with urllib.request.urlopen(URL, timeout=10) as response:
        payload = json.load(response)
    return payload, round((time.time() - started) * 1000)


def main():
    try:
        payload, elapsed_ms = fetch()
    except (OSError, urllib.error.URLError, TimeoutError) as exc:
        print(f"failed to read {URL}: {exc}", file=sys.stderr)
        return 1

    columns = payload.get("columns", {})
    print(f"elapsed_ms: {elapsed_ms}")
    print(f"message_columns: {len(columns.get('message', []))}")
    print(f"chat_columns: {len(columns.get('chat', []))}")
    print(f"attachment_columns: {len(columns.get('attachment', []))}")
    print(f"recent_messages: {len(payload.get('recent_messages', []))}")
    print(f"recent_chats: {len(payload.get('recent_chats', []))}")
    print(f"recent_attachments: {len(payload.get('recent_attachments', []))}")

    print("\nrecent chats")
    for chat in payload.get("recent_chats", [])[:8]:
        print(
            "- rowid={chat_rowid} service={service_name} participants={participant_count} "
            "last_message={last_message_rowid} attachments_recent={attachment_count} "
            "guid={guid}".format(**chat)
        )

    print("\nrecent attachments")
    for attachment in payload.get("recent_attachments", [])[:8]:
        print(
            "- rowid={rowid} mime={mime_type} bytes={total_bytes} outgoing={is_outgoing} "
            "messages={message_rowids} chats={chat_rowids}".format(**attachment)
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
