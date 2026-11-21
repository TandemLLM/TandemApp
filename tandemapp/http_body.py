"""The request-body framing both servers share (HTTP, issue #9).

One Content-Length spine and one chunked parser, stdlib only: `server/app.py` and
`proxy/app.py` used to carry the same framing line for line, and a fix had to land
twice. `read_body(rfile, headers)` answers the body whole or not at all; anything
unreadable or too big is a BodyError whose connection must close (a promise left
half-read is the next request parsed from the middle of a body).

`MAX_BODY_BYTES` is looked up per request through this module global, so a test or
an embedder can set it; QSE_MAX_BODY_MB overrides at import.

Two budgets sit under the per-request cap, because a cap one client sizes is not a
limit: `read_body(rfile, headers, cap=...)` takes the route's own cap when the route
has one and measures a chunked body against it as the chunks arrive -- a body with no
Content-Length used to pass every pre-read check and then read up to the global cap.
And `MAX_INFLIGHT_BODY_BYTES` (QSE_MAX_INFLIGHT_BODY_MB, 2 GiB) is the whole box's
budget for bodies being read at this moment: a 512 MiB promise costs its bytes before
it is kept, and a few dozen authenticated clients together used to out-buffer a box
that co-hosts the engines. Past the budget a request is refused with 503, unread.
"""

from __future__ import annotations

import os
import re
import threading

MAX_BODY_BYTES = int(os.environ.get("QSE_MAX_BODY_MB", 512)) * (1 << 20)

# The bytes of request bodies this server reads at once, all clients together. Sized
# for a box that co-hosts the engines: four bodies of the 512 MiB per-request cap is
# the worst case, a rounding error next to what a loaded engine holds, and small
# enough that no flood of promises can push the box into swap. Looked up per request
# through this module global, as MAX_BODY_BYTES is; QSE_MAX_INFLIGHT_BODY_MB overrides
# at import.
MAX_INFLIGHT_BODY_BYTES = int(os.environ.get("QSE_MAX_INFLIGHT_BODY_MB", 2048)) * (1 << 20)

_budget = threading.Lock()
_reading = 0                       # bytes reserved by the reads in progress


def _reserve(n: int) -> None:
    """Claim `n` bytes of the box's budget for the duration of one read. The claim is
    made before the first byte arrives, so a slow client dribbling a promised body
    holds its reservation the whole time it dribbles -- which is the point."""
    global _reading
    with _budget:
        if _reading + n > MAX_INFLIGHT_BODY_BYTES:
            raise BodyError(503, "server_busy",
                            f"the bodies being read now already use {_reading} of the "
                            f"{MAX_INFLIGHT_BODY_BYTES} bytes this server reads at once")
        _reading += n


def _release(n: int) -> None:
    global _reading
    with _budget:
        _reading -= n


class BodyError(Exception):
    """The request's framing is unreadable or too big: a 400/413 with this code and message.
    The body behind it was not read, so the connection closes (handle_one_request)."""

    def __init__(self, code: int, type_: str, message: str):
        super().__init__(message)
        self.code, self.type, self.message = code, type_, message


def conflicting_framing(headers) -> bool:
    """RFC 9112 section 6.1: a request that carries BOTH Transfer-Encoding: chunked
    and Content-Length is answered and then the connection is closed, because the two
    framings disagree about where the body ends and only closing resolves it. The
    body is read as chunked (Transfer-Encoding outranks Content-Length); the close
    is what makes the disagreement harmless."""
    return ("chunked" in (headers.get("Transfer-Encoding") or "").lower()
            and headers.get("Content-Length") is not None)


def read_body(rfile, headers, cap=None) -> bytes:
    """The request's body, consumed whole or not at all. An answer written with the
    body still unread is a desync one request away: every path out of here has read
    to the end of the framing, and BodyError closes the connection. `cap` is the
    route's own limit when it has one (a fleet route's 4 MiB): the smaller of it and
    MAX_BODY_BYTES wins, and a chunked body is measured against it chunk by chunk --
    a body with no Content-Length has no promise a pre-read check could weigh."""
    limit = MAX_BODY_BYTES if cap is None else min(cap, MAX_BODY_BYTES)
    if "chunked" in (headers.get("Transfer-Encoding") or "").lower():
        return _read_chunked(rfile, limit)       # Transfer-Encoding outranks Content-Length
    n = _content_length(headers, limit)          # validated before a single byte is read
    _reserve(n)                                  # the promise costs its bytes up front
    try:
        return rfile.read(n)
    finally:
        _release(n)


def _content_length(headers, limit) -> int:
    """The promised body size, validated before a single byte of it is read. This header
    is the client's and it sizes a pre-auth allocation: 4294967296 was once a
    multi-GB read on a handler thread, and -1 was rfile.read(-1) -- read-until-EOF, a
    permanently hung thread on a keep-alive connection."""
    raw = headers.get("Content-Length")
    if raw is None or not raw.strip():
        return 0                                 # an absent header is no body, as before
    if not _DIGITS.fullmatch(raw):
        # int() would take "+5", "1_0", " 5 " and "٥" (an Arabic-Indic five). The size
        # sizes a pre-auth allocation, so it is ASCII digits only -- and exactly that,
        # with no surrounding whitespace either.
        raise BodyError(400, "invalid_request_error",
                        "Content-Length: not a number") from None
    n = int(raw)
    if n < 0:
        raise BodyError(400, "invalid_request_error", "Content-Length: negative")
    if n > limit:
        raise BodyError(413, "too_large",
                        f"a request body is at most {limit} bytes")
    return n


# A chunk size is hex digits, nothing else. int(x, 16) also accepted 0x10, 1_0 and +5;
# a size line longer than the readline cap would then be re-read as further chunks, so
# seventy zeros and a '1' parsed as the last chunk and the rest of the line became a
# trailer. An unterminated (too long) size line is a 400, as the stdlib does.
_CHUNK_SIZE = re.compile(rb"[0-9a-fA-F]+")

# A Content-Length is ASCII digits only. int() also takes "+5", "1_0", " 5 " and "٥"
# (an Arabic-Indic five), and the value sizes a pre-auth allocation.
_DIGITS = re.compile(r"[0-9]+")


def _read_chunked(rfile, limit) -> bytes:
    """A chunked body, RFC 9112 7.1: size line, that many bytes, CRLF, until the zero
    chunk, then the trailers to their empty line. A framing error mid-stream leaves the
    stream at an unknown offset, and only a closed connection says so. The route's cap
    and the box's budget are both weighed chunk by chunk: a chunked body carries no
    promise a pre-read check could measure, so the read stops at the cap -- it does
    not finish first and then complain about what it buffered."""
    out = bytearray()
    reserved = 0
    try:
        while True:
            line = rfile.readline(64)
            if line and not line.endswith(b"\n"):
                raise BodyError(400, "invalid_request_error",
                                "transfer-encoding: chunk size line too long")
            token = line.split(b";", 1)[0].strip()   # extensions cut; hex digits only
            if not _CHUNK_SIZE.fullmatch(token):
                raise BodyError(400, "invalid_request_error", "transfer-encoding: bad chunk size")
            size = int(token, 16)
            if size == 0:
                for _ in range(100):                 # the trailers, to a blank line; capped
                    t = rfile.readline(1024)         # at the stdlib's 100 header lines
                    if t and not t.endswith(b"\n"):
                        # A trailer longer than the cap comes back in two parts: the lone
                        # "\n" would count as the blank line and the real one would stay
                        # on the wire for the next request.
                        raise BodyError(400, "invalid_request_error",
                                        "transfer-encoding: trailer line too long")
                    if not t or t in (b"\r\n", b"\n"):
                        break
                else:
                    raise BodyError(400, "invalid_request_error",
                                    "transfer-encoding: too many trailer lines")
                break
            if len(out) + size > limit:
                raise BodyError(413, "too_large", f"a request body is at most "
                                                  f"{limit} bytes")
            _reserve(size + 2)                       # the chunk and its CRLF, as read
            reserved += size + 2
            chunk = rfile.read(size + 2)
            if len(chunk) != size + 2 or not chunk.endswith(b"\r\n"):
                raise BodyError(400, "invalid_request_error", "transfer-encoding: truncated "
                                                              "chunk")
            out += chunk[:size]
    finally:
        _release(reserved)
    return bytes(out)
