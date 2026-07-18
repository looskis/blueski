#!/usr/bin/env python3
"""Tiny local webhook sink for testing. Prints each POST (headers + JSON body)
to stdout. Not part of the daemon — just a receiver to watch events land."""
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 9000


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        n = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(n).decode("utf-8", "replace")
        sig = self.headers.get("X-Blueski-Signature", "")
        print(f"\n--- webhook  sig={sig[:16]}… ---\n{body}", flush=True)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *a):
        pass  # silence default access logging


if __name__ == "__main__":
    print(f"webhook sink listening on http://127.0.0.1:{PORT}/", flush=True)
    HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
