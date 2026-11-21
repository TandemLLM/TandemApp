"""A chunked body is measured against its route's cap as it arrives.

`_fleet_too_big` weighed only the `Content-Length`, so a /v1/fleet/* request with
`Transfer-Encoding: chunked` passed the 4 MiB pre-check and then read on up to the
global 512 MiB cap: the route's cap was a promise about a header, not a limit on
the read. `read_body` now takes the route's cap and `_read_chunked` stops at it,
chunk by chunk -- the answer is the same 413, written when the cap is crossed, and
the connection closes because the rest of the body never will be read.

    a chunked fleet body over 4 MiB       413 at the cap, not after 5 MiB arrived
    a chunked fleet body under the cap    reads, as before
    a chunked wheel upload over 4 MiB     its own 32 MiB cap, so no 413
    a Content-Length over the cap         413 before the body arrives (unchanged)
    read_body(cap=...) at the module      the smaller of the cap and MAX_BODY_BYTES wins

Run: python tests/test_proxy_fleet_chunked.py
"""

from __future__ import annotations

import io
import os
import socket
import sys
import threading
from email.message import Message

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from tandemapp import http_body  # noqa: E402
from proxy_util import Proxy  # noqa: E402

FLEET = "/v1/fleet/plugins"
UPLOAD = "/v1/fleet/plugins/upload"


def _headers(**kw):
    m = Message()                                  # what BaseHTTPRequestHandler carries
    for k, v in kw.items():
        m[k] = v
    return m


def _chunked(total: int, chunk: int = 1024) -> bytes:
    out = bytearray()
    left = total
    while left > 0:
        n = min(chunk, left)
        out += b"%x\r\n" % n + b"a" * n + b"\r\n"
        left -= n
    return bytes(out) + b"0\r\n\r\n"


def _refused(raw: bytes, code: int, cap=None, **hdrs):
    try:
        http_body.read_body(io.BytesIO(raw), _headers(**hdrs), cap=cap)
    except http_body.BodyError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code} ({exc.message})"
        return exc
    raise AssertionError(f"expected BodyError {code}, got a body")


# --------------------------------------------------------------- the module's cap

def test_a_chunked_body_over_the_cap_is_refused_at_the_cap():
    exc = _refused(_chunked(2048), 413, cap=1024, **{"Transfer-Encoding": "chunked"})
    assert "1024" in exc.message                    # the cap it hit, not the global one
    # one chunk that lands exactly on the cap still reads
    assert len(http_body.read_body(io.BytesIO(_chunked(1024)),
                                   _headers(**{"Transfer-Encoding": "chunked"}),
                                   cap=1024)) == 1024


def test_a_content_length_over_the_cap_is_refused_before_the_body():
    exc = _refused(b"", 413, cap=1024, **{"Content-Length": "2048"})
    assert "1024" in exc.message


def test_the_smaller_of_the_cap_and_the_global_limit_wins():
    saved = http_body.MAX_BODY_BYTES
    http_body.MAX_BODY_BYTES = 100                   # the global is the tighter one
    try:
        _refused(b"", 413, cap=10 * 1024, **{"Content-Length": "150"})
        _refused(_chunked(150), 413, cap=10 * 1024, **{"Transfer-Encoding": "chunked"})
    finally:
        http_body.MAX_BODY_BYTES = saved


# ------------------------------------------------------------------ through the proxy

def _status_line(s: socket.socket) -> str:
    buf = b""
    while b"\r\n" not in buf:
        got = s.recv(4096)
        if not got:
            raise AssertionError(f"connection closed, no status line: {buf!r}")
        buf += got
    return buf.split(b"\r\n", 1)[0].decode()


def _chunked_post(port: int, path: str, total: int, chunk: int = 65536) -> str:
    """One chunked POST with the body sent from its own thread: the server stops
    reading at the cap and closes, and the rest of the body then has nowhere to go."""
    s = socket.create_connection(("127.0.0.1", port), timeout=10)
    err: list = []

    def send():
        try:
            # the Host names the loopback bind: local mode is the owner only for a request
            # that says this server's own address (SEC-28, no rebinding)
            s.sendall(f"POST {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\n"
                      f"Content-Type: application/json\r\n"
                      f"Transfer-Encoding: chunked\r\n\r\n".encode())
            left = total
            while left > 0:
                n = min(chunk, left)
                s.sendall(b"%x\r\n" % n + b"a" * n + b"\r\n")
                left -= n
            s.sendall(b"0\r\n\r\n")
        except OSError as exc:                       # the server stopped at the cap
            err.append(exc)

    t = threading.Thread(target=send, daemon=True)
    t.start()
    try:
        status = _status_line(s)
    finally:
        t.join(10)
        s.close()
    return status


def test_a_chunked_fleet_body_over_4_mib_is_a_413_mid_read():
    with Proxy() as px:
        status = _chunked_post(px.port, FLEET, 5 * 1024 * 1024)
        assert status.startswith("HTTP/1.1 413"), status


def test_a_chunked_fleet_body_under_the_cap_still_reads():
    with Proxy() as px:
        status = _chunked_post(px.port, FLEET, 2)    # b"aa", two bytes in one chunk
        assert not status.startswith("HTTP/1.1 413"), status


def test_a_chunked_upload_uses_the_wheels_cap_not_the_fleet_one():
    # 5 MiB is over the fleet's 4 MiB and under a wheel's 32 MiB: the upload route
    # reads it and answers about the upload itself, not with a fleet 413
    with Proxy() as px:
        status = _chunked_post(px.port, UPLOAD, 5 * 1024 * 1024)
        assert not status.startswith("HTTP/1.1 413"), status


def test_a_content_length_fleet_body_over_4_mib_is_still_a_413_up_front():
    with Proxy() as px:
        s = socket.create_connection(("127.0.0.1", px.port), timeout=5)
        try:
            s.sendall(f"POST {FLEET} HTTP/1.1\r\nHost: 127.0.0.1:{px.port}\r\n"
                      f"Content-Type: application/json\r\n"
                      f"Content-Length: {5 * 1024 * 1024}\r\n\r\n".encode())
            assert _status_line(s).startswith("HTTP/1.1 413")
        finally:
            s.close()


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  {name:64s} ok")
            passed += 1
    print(f"{passed} passed")