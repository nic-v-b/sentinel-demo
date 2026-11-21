"""Synthetic-only lifecycle and real loopback HTTP boundary tests."""
import concurrent.futures
import hashlib
import http.client
import json
from pathlib import Path
import socket
import threading
import time
import unittest
from unittest.mock import patch

from sentinel.live_view import PreviewServer, PreviewSession


class SessionTests(unittest.TestCase):
    def setUp(self):
        self.now = 0.0
        self.session = PreviewSession(clock=lambda: self.now, session_limit=1)

    def advance(self, seconds):
        self.now += seconds
        return self.session.snapshot()

    def test_manual_connect_stop_and_reconnect(self):
        self.assertEqual(self.session.snapshot()["state"], "disconnected")
        self.assertIsNone(self.session.session)
        self.assertTrue(self.session.connect(0))
        first = self.session.session
        self.assertEqual(self.session.state, "connecting")
        self.assertEqual(self.advance(.16)["state"], "connected")
        self.assertTrue(self.session.stop(first))
        self.assertEqual(self.session.state, "stopping")
        self.assertIsNone(self.session.session)
        self.assertEqual(self.advance(.16)["state"], "disconnected")
        self.assertTrue(self.session.connect(self.session.revision))
        self.assertNotEqual(self.session.session, first)

    def test_repeated_actions_and_stale_commands_cannot_replace_or_stop_session(self):
        for _ in range(20):
            revision = self.session.revision
            self.assertTrue(self.session.connect(revision))
            token = self.session.session
            self.assertFalse(self.session.connect(revision))
            self.assertFalse(self.session.stop("stale"))
            self.assertEqual(self.session.session, token)
            self.assertTrue(self.session.stop(token))
            self.assertTrue(self.session.stop(None))
            self.assertFalse(self.session.connect(revision))
            self.advance(.16)

    def test_cancel_connect_prevents_late_ready(self):
        self.session.connect(0)
        self.session.stop(self.session.session)
        self.assertEqual(self.advance(.16)["state"], "disconnected")
        self.assertEqual(self.advance(10)["state"], "disconnected")
        self.assertIsNone(self.session.session)

    def test_startup_timeout_error_cleanup_and_recovery(self):
        self.session.ready = False
        self.session.connect(0)
        state = self.advance(3)
        self.assertEqual((state["state"], state["reason"], state["session"]), ("error", "startup-timeout", None))
        self.session.ready = True
        self.assertTrue(self.session.connect(state["revision"]))
        self.assertEqual(self.advance(.16)["state"], "connected")

    def test_absolute_session_timeout_cannot_be_extended_by_connect(self):
        self.session.connect(0)
        self.advance(.16)
        deadline = self.session.deadline
        for _ in range(10):
            self.assertFalse(self.session.connect(self.session.revision))
            self.assertEqual(self.session.deadline, deadline)
        self.assertEqual(self.advance(1)["state"], "stopping")
        self.assertEqual(self.session.reason, "session-expired")
        self.assertIsNone(self.session.session)
        self.assertEqual(self.advance(.16)["state"], "disconnected")

    def test_failure_and_shutdown_release_every_state(self):
        for state in ("disconnected", "connecting", "connected", "stopping", "error"):
            with self.subTest(state=state):
                session = PreviewSession()
                session.state, session.session = state, "synthetic"
                session.close()
                self.assertEqual(session.state, "disconnected")
                self.assertIsNone(session.session)
                self.assertFalse(session.connect(session.revision))
        self.session.connect(0)
        self.session.fail()
        self.assertIsNone(self.session.session)
        self.assertEqual(self.session.state, "error")

    def test_invalid_limits(self):
        for value in (0, -1, float("inf"), float("nan"), 61):
            with self.assertRaises(ValueError):
                PreviewSession(session_limit=value)


class HttpTests(unittest.TestCase):
    def setUp(self):
        with patch("socket.getfqdn", side_effect=AssertionError("DNS must not be used")):
            self.server = PreviewServer(0)
        self.thread = threading.Thread(target=self.server.serve_forever, kwargs={"poll_interval": .01})
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.thread.join(4)
        self.server.server_close()
        self.assertFalse(self.thread.is_alive())
        self.assertIsNone(self.server.session.session)

    def request(self, path="/api/status", *, method="GET", body=None, headers=None):
        connection = http.client.HTTPConnection("127.0.0.1", self.server.server_port, timeout=5)
        try:
            connection.request(method, path, body=body, headers=headers or {})
            response = connection.getresponse()
            return response.status, dict(response.getheaders()), response.read()
        finally:
            connection.close()

    def post(self, path, body, **overrides):
        headers = {"Origin": self.server.origin, "Content-Type": "application/json", "X-Sentinel-CSRF": self.server.csrf}
        headers.update(overrides)
        return self.request(path, method="POST", body=json.dumps(body), headers=headers)

    def wait_state(self, expected):
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            data = json.loads(self.request()[2])
            if data["state"] == expected:
                return data
            time.sleep(.01)
        self.fail("synthetic state transition timed out")

    def test_bind_static_allowlist_file_fallback_and_headers(self):
        self.assertEqual(self.server.server_address[0], "127.0.0.1")
        code, headers, body = self.request("/")
        self.assertEqual(code, 200)
        self.assertIn(b"connect-src 'self'", body)
        original = Path("ui/index.html").read_bytes()
        self.assertIn(b"connect-src 'none'", original)
        self.assertIn(b"media-src blob:", original)
        self.assertEqual(headers["Cache-Control"], "no-store")
        self.assertEqual(headers["Cross-Origin-Resource-Policy"], "same-origin")
        self.assertEqual(headers["X-Frame-Options"], "DENY")
        self.assertNotIn("Access-Control-Allow-Origin", headers)
        for path in ("/.env", "/../README.md", "/%2e%2e/README.md", "/sentinel/live_view.py", "/?file=README.md"):
            self.assertEqual(self.request(path)[0], 404)
        self.assertEqual(self.request("http://example.invalid/")[0], 403)
        self.assertEqual(self.server.session.state, "disconnected")

    def test_host_origin_fetch_metadata_and_csrf_rejection(self):
        for host in ("localhost", "127.0.0.1", "example.invalid", self.server.authority + ".invalid"):
            self.assertEqual(self.request(headers={"Host": host})[0], 403)
        for origin in ("null", "file://", "https://example.invalid", "http://localhost:8765", self.server.origin + "/"):
            self.assertEqual(self.request(headers={"Origin": origin})[0], 403)
            self.assertEqual(self.post("/api/connect", {"revision": 0}, Origin=origin)[0], 403)
        for site in ("cross-site", "same-site"):
            self.assertEqual(self.request(headers={"Sec-Fetch-Site": site})[0], 403)
        for headers in ({}, {"Origin": self.server.origin}, {"X-Sentinel-CSRF": self.server.csrf}):
            self.assertEqual(self.request("/api/connect", method="POST", body='{"revision":0}', headers=headers)[0], 403)
        self.assertEqual(self.post("/api/connect", {"revision": 0}, **{"X-Sentinel-CSRF": "wrong"})[0], 403)
        self.assertEqual(self.server.session.state, "disconnected")

    def test_strict_body_and_method_rejections(self):
        for body in ({}, [], {"revision": True}, {"revision": 0, "extra": 0}, {"revision": "0"}):
            self.assertEqual(self.post("/api/connect", body)[0], 400)
        self.assertEqual(self.post("/api/connect", {"revision": 0}, **{"Content-Type": "text/plain"})[0], 415)
        self.assertEqual(self.post("/api/connect", {"revision": 0}, **{"Content-Length": "129"})[0], 400)
        self.assertEqual(self.request("/api/connect")[0], 404)
        self.assertEqual(self.request("/api/connect", method="OPTIONS")[0], 501)
        self.assertEqual(self.request("/api/connect", method="PUT")[0], 501)
        self.assertEqual(self.post("/api/disconnect", {"session": 1})[0], 400)

    def test_duplicate_and_transfer_headers_fail_closed(self):
        for extra in ("Host: example.invalid\r\n", "Origin: null\r\nOrigin: null\r\n", "Transfer-Encoding: chunked\r\n"):
            with socket.create_connection(self.server.server_address, timeout=4) as client:
                client.sendall((f"GET /api/status HTTP/1.1\r\nHost: {self.server.authority}\r\n{extra}\r\n").encode())
                self.assertIn(b"403", client.recv(4096).split(b"\r\n")[0])

    def test_real_http_media_boundary_revocation_and_stale_commands(self):
        self.assertEqual(self.request("/api/preview/unknown")[0], 410)
        self.server.session.ready = False
        code, _, body = self.post("/api/connect", {"revision": 0})
        self.assertEqual(code, 200)
        data = json.loads(body)
        self.assertEqual(data["state"], "connecting")
        session = data["session"]
        self.assertEqual(self.request("/api/preview/" + session)[0], 410)
        self.server.session.ready = True
        self.wait_state("connected")
        code, headers, media = self.request("/api/preview/" + session)
        self.assertEqual(code, 200)
        self.assertEqual(headers["Content-Type"], "video/webm")
        self.assertLess(len(media), 65536)
        self.assertEqual(hashlib.sha256(media).hexdigest(),
                         "93c15025aba93e64d0d8aaa4793be4e9f0d5f25e0eb3fc48d70d7a8afb83a2fc")
        self.assertEqual(self.post("/api/disconnect", {"session": session})[0], 200)
        self.assertEqual(self.request("/api/preview/" + session)[0], 410)
        self.wait_state("disconnected")
        revision = self.server.session.revision
        self.assertEqual(self.post("/api/connect", {"revision": revision})[0], 200)
        self.assertEqual(self.post("/api/disconnect", {"session": session})[0], 409)
        self.assertIsNotNone(self.server.session.session)

    def test_concurrent_connects_admit_exactly_one_then_concurrent_stops(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            codes = list(pool.map(lambda _: self.post("/api/connect", {"revision": 0})[0], range(6)))
        self.assertEqual(sorted(codes), [200, 409, 409, 409, 409, 409])
        session = self.server.session.session
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            codes = list(pool.map(lambda _: self.post("/api/disconnect", {"session": session})[0], range(6)))
        self.assertEqual(codes.count(200), 1)
        self.assertEqual(codes.count(409), 5)
        self.assertIsNone(self.server.session.session)
        self.wait_state("disconnected")

    def test_abandoned_request_has_absolute_deadline_and_service_recovers(self):
        with socket.create_connection(self.server.server_address, timeout=4) as client:
            client.sendall(b"GET /")
            started = time.monotonic()
            self.assertEqual(client.recv(1), b"")
            self.assertLess(time.monotonic() - started, 3.5)
        self.assertEqual(self.request()[0], 200)

    def test_timeout_and_shutdown_revoke_http_media(self):
        self.server.session.session_limit = .2
        self.post("/api/connect", {"revision": 0})
        state = self.wait_state("connected")
        self.wait_state("disconnected")
        self.assertEqual(self.request("/api/preview/" + state["session"])[0], 410)
        self.server.session.close()
        self.assertEqual(self.post("/api/connect", {"revision": self.server.session.revision})[0], 409)


if __name__ == "__main__":
    unittest.main()
