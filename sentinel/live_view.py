"""UI-2A loopback synthetic preview. No provider, observer or recorder imports.

Run in the foreground: python -E -s -B -m sentinel.live_view
This deliberately serves a finite generated WebM, not a live stream bridge.
"""
from __future__ import annotations

import argparse
import hmac
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import math
from pathlib import Path
import secrets
import socket
from socketserver import TCPServer
import threading
import time


class PreviewSession:
    """Serialized, timer-driven lifecycle; no background capture or workers."""

    def __init__(self, *, clock=time.monotonic, startup_limit=3.0, session_limit=60.0):
        if not all(math.isfinite(v) and 0.2 <= v <= 60 for v in (startup_limit, session_limit)):
            raise ValueError("invalid preview limits")
        self.clock = clock
        self.startup_limit = startup_limit
        self.session_limit = session_limit
        self.state = "disconnected"
        self.reason = "manual"
        self.session = None
        self.revision = 0
        self.closed = False
        self.ready = True  # Synthetic readiness only; no transport/backend hook.
        self.deadline = self.ready_at = 0.0

    def tick(self):
        now = self.clock()
        if self.state == "connecting":
            if now >= self.deadline:
                self.fail("startup-timeout")
            elif self.ready and now >= self.ready_at:
                self.state = "connected"
                self.deadline = now + self.session_limit
        elif self.state == "connected" and now >= self.deadline:
            self.stop(self.session, reason="session-expired")
        elif self.state == "stopping" and now >= self.deadline:
            self.state = "disconnected"

    def snapshot(self):
        self.tick()
        return {"state": self.state, "reason": self.reason, "session": self.session,
                "revision": self.revision, "session_limit": self.session_limit}

    def connect(self, revision):
        self.tick()
        if self.closed or revision != self.revision or self.state not in {"disconnected", "error"}:
            return False
        self.revision += 1
        self.session = secrets.token_urlsafe(24)
        self.state, self.reason = "connecting", "manual"
        now = self.clock()
        self.ready_at, self.deadline = now + 0.15, now + self.startup_limit
        return True

    def stop(self, session, *, reason="manual"):
        if self.state in {"disconnected", "stopping"} and session is None:
            return True
        if session is None or session != self.session:
            return False
        self.session = None  # Revoke media before reporting stopping.
        self.revision += 1
        self.state, self.reason = "stopping", reason
        self.deadline = self.clock() + 0.15
        return True

    def fail(self, reason="preview-failed"):
        self.session = None
        self.revision += 1
        self.state, self.reason = "error", reason

    def close(self):
        self.closed = True
        self.session = None
        self.revision += 1
        self.state, self.reason = "disconnected", "shutdown"


class PreviewServer(HTTPServer):
    request_queue_size = 8
    allow_reuse_address = False

    def server_bind(self):
        if hasattr(socket, "SO_EXCLUSIVEADDRUSE"):
            self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_EXCLUSIVEADDRUSE, 1)
        # HTTPServer.server_bind calls getfqdn(), which may perform reverse DNS.
        # The numeric loopback authority needs no resolver or external traffic.
        TCPServer.server_bind(self)
        self.server_name = "127.0.0.1"
        self.server_port = self.socket.getsockname()[1]

    def __init__(self, port=8765, *, session=None):
        self.session = session or PreviewSession()
        self.csrf = secrets.token_urlsafe(32)
        root = Path(__file__).resolve().parents[1]
        # Explicit asset allowlist, loaded once. No request-derived filesystem reads.
        self.assets = {"/" + name: (root / "ui" / name).read_bytes()
                       for name in ("index.html", "app.js", "live.js", "styles.css")}
        self.assets["/index.html"] = self.assets["/index.html"].replace(
            b"media-src blob:; connect-src 'none'", b"media-src 'self' blob:; connect-src 'self'")
        self.media = (root / "sentinel" / "synthetic_preview.webm").read_bytes()
        if not (0 < len(self.media) < 65536 and self.media.startswith(bytes.fromhex("1a45dfa3"))):
            raise ValueError("invalid synthetic fixture")
        super().__init__(("127.0.0.1", port), PreviewHandler)
        self.authority = f"127.0.0.1:{self.server_port}"
        self.origin = "http://" + self.authority

    def service_actions(self):
        self.session.tick()

    def server_close(self):
        self.session.close()
        super().server_close()


class PreviewHandler(BaseHTTPRequestHandler):
    # One request per connection, one serialized handler, no request worker pool.
    protocol_version = "HTTP/1.0"
    server_version = "Sentinel"
    sys_version = ""

    def handle(self):
        def expire():
            try:
                self.connection.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

        # Absolute request lifetime defeats trickle reads as well as idle clients.
        timer = threading.Timer(2.0, expire)
        self.connection.settimeout(2.0)
        timer.start()
        try:
            super().handle()
        except (OSError, ValueError):
            pass
        finally:
            timer.cancel()
            timer.join()

    def log_message(self, *_args):
        pass  # No paths, headers, capabilities or request diagnostics in logs.

    def send_error(self, code, message=None, explain=None):
        self.reply(code, b"Request rejected\n", "text/plain")

    def reply(self, code, body=b"", content_type="application/json"):
        self.send_response(code)
        for key, value in {
            "Content-Type": content_type, "Content-Length": str(len(body)),
            "Cache-Control": "no-store", "X-Content-Type-Options": "nosniff",
            "X-Frame-Options": "DENY", "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": "frame-ancestors 'none'; form-action 'none'; base-uri 'none'",
            "Cross-Origin-Resource-Policy": "same-origin", "Connection": "close",
        }.items():
            self.send_header(key, value)
        self.end_headers()
        self.wfile.write(body)

    def authorized(self, *, mutation=False):
        # Reject duplicate security/framing headers, alternate authority spellings,
        # absolute request targets, opaque/null origins and cross-site requests.
        for name in ("Host", "Origin", "Content-Length", "Content-Type", "X-Sentinel-CSRF", "Sec-Fetch-Site"):
            if len(self.headers.get_all(name, [])) > 1:
                return False
        if self.headers.get("Host") != self.server.authority:
            return False
        origin = self.headers.get("Origin")
        if origin is not None and origin != self.server.origin:
            return False
        if self.headers.get("Sec-Fetch-Site") not in (None, "same-origin", "none"):
            return False
        if self.headers.get("Transfer-Encoding") is not None:
            return False
        if mutation:
            token = self.headers.get("X-Sentinel-CSRF", "")
            if (origin != self.server.origin or not token.isascii()
                    or not hmac.compare_digest(token, self.server.csrf)):
                return False
        return True

    def do_GET(self):
        if not self.authorized():
            return self.reply(403)
        state = self.server.session.snapshot()
        if self.path == "/api/status":
            return self.reply(200, json.dumps({**state, "csrf": self.server.csrf}).encode())
        if self.path.startswith("/api/preview/"):
            if state["state"] != "connected" or self.path != "/api/preview/" + str(state["session"]):
                return self.reply(410)
            # Small finite representation: no ranges, endless body or producer.
            return self.reply(200, self.server.media, "video/webm")
        path = "/index.html" if self.path == "/" else self.path
        if path not in self.server.assets:
            return self.reply(404)
        kind = "text/html" if path.endswith(".html") else "text/css" if path.endswith(".css") else "text/javascript"
        self.reply(200, self.server.assets[path], kind + "; charset=utf-8")

    def do_POST(self):
        if not self.authorized(mutation=True):
            return self.reply(403)
        if self.headers.get("Content-Type") != "application/json":
            return self.reply(415)
        length = self.headers.get("Content-Length", "")
        if not length.isascii() or not length.isdecimal() or not 1 <= int(length) <= 128:
            return self.reply(400)
        try:
            body = json.loads(self.rfile.read(int(length)))
        except (ValueError, UnicodeError):
            return self.reply(400)
        controller = self.server.session
        if self.path == "/api/connect":
            if type(body) is not dict or set(body) != {"revision"} or type(body["revision"]) is not int:
                return self.reply(400)
            accepted = controller.connect(body["revision"])
        elif self.path == "/api/disconnect":
            if (type(body) is not dict or set(body) != {"session"}
                    or body["session"] is not None and type(body["session"]) is not str):
                return self.reply(400)
            controller.tick()
            accepted = controller.stop(body["session"])
        else:
            return self.reply(404)
        self.reply(200 if accepted else 409, json.dumps(controller.snapshot()).encode())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    with PreviewServer(args.port) as server:
        print(f"Synthetic preview only: {server.origin} (Ctrl+C to stop)", flush=True)
        try:
            server.serve_forever(poll_interval=0.05)
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
