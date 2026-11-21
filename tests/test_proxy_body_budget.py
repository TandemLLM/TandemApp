"""The box's budget for bodies read at once, and the cap that bounds the threads.

The per-request cap is 512 MiB and the server is a thread per request, so a few
dozen authenticated clients each promising a big body used to size their own
allocation together -- on a box that co-hosts the engines, that is how an
authenticated request flood becomes memory exhaustion. `tandemapp/http_body.py`
now keeps one budget for all of them: `MAX_INFLIGHT_BODY_BYTES` (2 GiB,
QSE_MAX_INFLIGHT_BODY_MB), claimed before the first byte of a read and released
when the read ends, so a client dribbling a promised body holds its reservation
the whole time it dribbles. Past the budget a request is refused unread with 503.
The thread-count half is `max_inflight`: a backend made without one now gets 64
instead of no limit at all (`none` still says no limit, out loud).

    a body over the budget                 503, unread
    a slow body holding the budget         the next body is the one refused
    the budget after the read ends         back to zero, the next body reads
    a chunked body past the budget         503 mid-stream, the budget back to zero
    a backend made without a cap           max_inflight 64; an explicit none stays none

Run: python tests/test_proxy_body_budget.py
"""

from __future__ import annotations

import io
import os
import socket
import sys
import tempfile
import threading
import time
from email.message import Message

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from tandemapp import http_body  # noqa: E402
from proxy_util import Proxy  # noqa: E402

MIB = 1 << 20


def _headers(**kw):
    m = Message()                                  # what BaseHTTPRequestHandler carries
    for k, v in kw.items():
        m[k] = v
    return m


def _refused(rfile, code: int, **hdrs):
    try:
        http_body.read_body(rfile, _headers(**hdrs))
    except http_body.BodyError as exc:
        assert exc.code == code, f"expected {code}, got {exc.code} ({exc.message})"
        return exc
    raise AssertionError(f"expected BodyError {code}, got a body")


class _Hold:
    """A body that arrives when the test says so: read() waits for the gate."""

    def __init__(self, data: bytes, gate: threading.Event):
        self.data, self.gate = data, gate

    def read(self, n: int) -> bytes:
        self.gate.wait(10)
        return self.data[:n]


def _chunked(total: int, chunk: int = 60) -> bytes:
    out = bytearray()
    left = total
    while left > 0:
        n = min(chunk, left)
        out += b"%x\r\n" % n + b"a" * n + b"\r\n"
        left -= n
    return bytes(out) + b"0\r\n\r\n"


# ---------------------------------------------------------------- the module's budget

def test_a_promised_body_over_the_budget_is_refused_unread():
    saved = http_body.MAX_INFLIGHT_BODY_BYTES
    http_body.MAX_INFLIGHT_BODY_BYTES = 100
    try:
        exc = _refused(io.BytesIO(b"x" * 150), 503, **{"Content-Length": "150"})
        assert "100" in exc.message
        assert http_body._reading == 0             # a refusal reserves nothing
    finally:
        http_body.MAX_INFLIGHT_BODY_BYTES = saved


def test_a_read_holds_its_reservation_until_it_ends():
    saved = http_body.MAX_INFLIGHT_BODY_BYTES
    http_body.MAX_INFLIGHT_BODY_BYTES = 150
    gate = threading.Event()
    got: list = []
    try:
        def slow():                                # 100 bytes, arriving on the gate's word
            got.append(http_body.read_body(_Hold(b"x" * 100, gate),
                                           _headers(**{"Content-Length": "100"})))
        t = threading.Thread(target=slow, daemon=True)
        t.start()
        deadline = time.time() + 10
        while http_body._reading < 100 and time.time() < deadline:
            time.sleep(0.01)
        assert http_body._reading == 100, "the slow read never reserved its bytes"
        # the second body is the one refused: the first has promised the budget away
        _refused(io.BytesIO(b"y" * 100), 503, **{"Content-Length": "100"})
        gate.set()
        t.join(10)
        assert got == [b"x" * 100]
        assert http_body._reading == 0             # released when the read ended
        assert http_body.read_body(io.BytesIO(b"z" * 100),
                                   _headers(**{"Content-Length": "100"})) == b"z" * 100
    finally:
        http_body.MAX_INFLIGHT_BODY_BYTES = saved


def test_a_chunked_body_past_the_budget_is_refused_mid_stream():
    saved = http_body.MAX_INFLIGHT_BODY_BYTES
    http_body.MAX_INFLIGHT_BODY_BYTES = 100        # two 60-byte chunks do not fit
    try:
        _refused(io.BytesIO(_chunked(120)), 503, **{"Transfer-Encoding": "chunked"})
        assert http_body._reading == 0             # the first chunk's claim came back
        assert http_body.read_body(io.BytesIO(_chunked(60)),
                                   _headers(**{"Transfer-Encoding": "chunked"})) == b"a" * 60
    finally:
        http_body.MAX_INFLIGHT_BODY_BYTES = saved


# ------------------------------------------------------------------ through the proxy

def _status_line(s: socket.socket) -> str:
    buf = b""
    while b"\r\n" not in buf:
        got = s.recv(4096)
        if not got:
            raise AssertionError(f"connection closed, no status line: {buf!r}")
        buf += got
    return buf.split(b"\r\n", 1)[0].decode()


def test_a_slow_body_holds_the_budget_and_the_next_request_gets_503():
    saved = http_body.MAX_INFLIGHT_BODY_BYTES
    http_body.MAX_INFLIGHT_BODY_BYTES = MIB        # one 1 MiB body is the whole budget
    try:
        with Proxy() as px:
            a = socket.create_connection(("127.0.0.1", px.port), timeout=10)
            try:
                # A promises 1 MiB and dribbles: the reservation is the promise's size,
                # taken before the bytes, held while they crawl in
                a.sendall(b"POST /v1/dashboard/session HTTP/1.1\r\nHost: x\r\n"
                          b"Content-Type: application/json\r\nContent-Length: 1048576\r\n\r\n")
                a.sendall(b"a" * (64 * 1024))
                deadline = time.time() + 10
                while http_body._reading < MIB and time.time() < deadline:
                    time.sleep(0.01)
                assert http_body._reading == MIB, "the dribbling body never reserved its bytes"
                code, _, body = px.post("/v1/dashboard/session", {"token": "wrong"})
                assert code == 503, f"a second body was read past the budget: {code} {body!r}"
                assert b"server_busy" in body
                a.sendall(b"a" * (MIB - 64 * 1024))   # A finishes: its own read was never refused
                assert not _status_line(a).startswith("HTTP/1.1 503")
            finally:
                a.close()
            # the budget came back with the read: the same small request now answers normally
            code, _, _ = px.post("/v1/dashboard/session", {"token": "wrong"})
            assert code != 503, code
    finally:
        http_body.MAX_INFLIGHT_BODY_BYTES = saved


# ------------------------------------------------------------- the threads' other half

def test_a_backend_made_without_a_cap_gets_64():
    from proxy import config as C
    db = C.ConfigDB(os.path.join(tempfile.mkdtemp(prefix="pxbudget-"), "config.sqlite3"),
                    env={}, use_keychain=False).open()
    try:
        db.create_backend({"id": "plain", "kind": "openai", "url": "http://h:1/v1"}, who="t")
        assert db.backend("plain")["max_inflight"] == 64
        db.create_backend({"id": "said", "kind": "openai", "url": "http://h:2/v1",
                           "max_inflight": None}, who="t")
        assert db.backend("said")["max_inflight"] is None
    finally:
        db.close()


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  {name:64s} ok")
            passed += 1
    print(f"{passed} passed")