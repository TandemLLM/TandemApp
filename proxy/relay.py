"""One request through to a backend and back, streamed or not, measured on the way.

THE BYTES. The response's bytes go to the client unchanged. Beside them the adapter reads every
SSE event for the ledger. When the client did not ask for `stream_options.include_usage`, the
proxy asks for it (unless the backend's `inject_usage` is off) and drops that one usage-only frame
before the client, so the client gets what the backend sent, less the frame it did not ask for.
(OpenAI then also writes `"usage": null` into each chunk; that is the backend's byte, passed on.)
The request body goes upstream as the client sent it unless the model id or the usage request
changes it; then it is the client's JSON with those fields set.

TRANSLATION (SRV-877): a kind whose server speaks another protocol (`adapter.translates`, the
Anthropic-compatible kind) is the exception. Its request is the adapter's translation (a field
with no equal is a 400 before anything goes out), its path the adapter's, and its answer, stream
and errors are turned into the client's protocol first, so the adapter's reader, the hooks, the
harness, the cost comment and the client all see the OpenAI shape. The ledger reads the
upstream's own usage (`Stamps.native`: the cache writes have no OpenAI field). The answer names the
client's fields the request went without in `X-Tandem-Dropped`.

TIMEOUTS per backend (local: connect 5 s, first byte 600 s, idle gap 120 s; cloud: 10, 120, 60),
each its own `finish_reason`: `timeout_connect`, `timeout_first_byte`, `timeout_idle`. Our engine
(`tandemllm`) may think for longer than either before its next byte (a long prefill, reasoning
it does not stream, a request without a stream): when a wait runs out, the relay asks the
engine's board (`/v1/dashboard/parallel`) whether it still holds the request under the relay's
lane id, and waits another span while it does (SRV-733). An engine that dropped the request, or
does not answer the question, ends the wait as before.

RETRIES only before any byte went to the client: a connect error, a 429, 502 or 503 (and what
an adapter adds: 529 for the Anthropic-compatible kind). At most 2
across the route's fallbacks. A fallback is tried at once (another backend); the same backend is
tried again after 1 s, then 2 s, with jitter, or after its `Retry-After` up to 30 s. After a byte
went out a failure is an `error` frame to the client, then `[DONE]`. A 429 that survives the tries
goes to the client with its `Retry-After`.

THE COST, FOR A CLIENT THAT ASKS. A streamed request with `X-Tandem-Cost: 1` (chatd sends it) gets
one SSE comment just before `data: [DONE]`: `: tandem-cost {"cost_micros": N, "backend": id}`, the
price of the request (every request the harness sent for it) as its ledger row will have it, N
null without a price. A comment is ignored by every SSE reader; without the header the bytes are
the backend's.

OUR ENGINE BEHIND IT (SRV-733, proxy/adapters/generic.py TandemLLM): the client's headers go on
and the answer's come back as the engine would have taken and sent them; a request this proxy let
in without a key is the engine's `owner`, as one sent straight to the engine is
(`X-Tandem-User`); the cost comment is the engine's own, which prices what it relays.

A TOKEN'S SPEND LIMIT (SRV-873, proxy/tokenlimits.py): a stream of an API token with a spend limit
is watched every few frames (`rec.watch`). When the limit is passed it ends as an error ends a
stream: an error frame (`spend_limit_reached`) then `[DONE]`, or the Messages API's `error` event;
the backend's connection is closed and the row says finish `limit`. No other stream is watched.

THE CLIENT LEAVES: the upstream connection is closed within a quarter second (the backend stops
generating), and the row says `abandoned`.

CONCURRENCY: a thread per request (the server's), no lock held across a request, and per backend
at most `max_inflight` requests at once (64 by default; `none` is no limit and the proxy adds
none of its own beyond that): past the cap the request goes to a fallback when one has room,
else a 503 with `Retry-After: 1`. The engine's queue, lock and `max_queue` are never
touched.

MAINTENANCE (VIS-250): a backend on a drained node (`svc.drain_of`, proxy/fleetops.py) takes no
new request: the route's fallbacks are tried, and with none left the client gets a 503
`node_draining` with `Retry-After: 30`. A request already relayed there runs to its end. A backend
whose engine host restarts for a settings change (`svc.restart_drain_of`, SRV-845) is drained the
same way until it is back.

THE HOOKS (SRV-849, proxy/hooks.py): with the request's hook turn (`rec.hk`; None while no hook is
enabled, and nothing here changes) request.before_engine runs on the final body before each try
(its patches kept for a retry or a fallback), stream.chunk's observers see each event, a whole
answer passes tool_call.detected and response.after_engine before the client gets a byte (one
retry if a hook asks), a failure that may be tried again asks request.error for a fallback, and a
streamed answer's own stages observe after it ended.

THE FLEET (SRV-731, proxy/deployments.py): a backend on a fleet node (`svc.locate`) names the node
and its deployment in the answer, `X-Tandem-Node` (the node's alias) and `X-Tandem-Deployment`,
and in the ledger row (`node`, its id, and `deployment`). A deployment's members are its route's
first fallbacks. A constrained request (`route.no_replay`) goes to the next one only when the
member never took it (a connect failure): after its body went out, the member's answer is the
client's, a 429 or 503 included.
"""

from __future__ import annotations

import json
import random
import re
import select
import socket
import threading
import time

from proxy import http1, passthrough
from proxy import stash as stash_mod
from proxy.adapters import ENDPOINTS, Stamps
from proxy.adapters import anthropic as anthropic_adapter
from proxy.adapters.base import CHAT, COMPLETIONS
from proxy.measure import RelayRecord
from proxy.translate.anthropic import TranslateError
from tandemapp.parallel import _fetch_json

RETRYABLE = (429, 502, 503)      # the default; an adapter may add its own (`retryable`)
WATCH_FRAMES = 8                         # SRV-873: a watched stream's check, every N frames
MESSAGES_PATHS = (anthropic_adapter.MESSAGES,)  # SRV-881: a stream that passes through
MAX_RETRIES = 2
RETRY_AFTER_CAP = 30.0
DRAIN_RETRY_S = 30
# SRV-733: our engine's board, asked whether it still holds a quiet request
BOARD = "/v1/dashboard/parallel?since=9999999999"


_DONE = re.compile(rb"(?:^|\n)data: ?\[DONE\]")


class LimitReached(Exception):
    """SRV-873: a watched stream passed its token's spend limit (`refusal`)."""

    def __init__(self, refusal):
        super().__init__(refusal.message)
        self.refusal = refusal


class CostTap:
    """The client's stream writer, with the request's cost (`cost()`, bytes) written once just
    before the first `data: [DONE]` (see THE COST above)."""

    def __init__(self, w, cost):
        self.w, self.cost, self.sent = w, cost, False

    def write(self, data: bytes):
        if not self.sent:
            m = _DONE.search(data)
            if m is not None:
                self.sent = True
                i = m.start() + (1 if data[m.start():m.start() + 1] == b"\n" else 0)
                data = data[:i] + self.cost() + data[i:]
        return self.w.write(data)

    def flush(self) -> None:
        self.w.flush()


class Inflight:
    """Per backend: the requests relayed now, and the cap."""

    def __init__(self):
        self._n: dict[str, int] = {}
        self._lock = threading.Lock()

    def take(self, bid: str, cap: int | None) -> bool:
        """Count one more request on `bid`; False when a cap is set and reached. None: no cap."""
        with self._lock:
            n = self._n.get(bid, 0)
            if cap is not None and n >= cap:
                return False
            self._n[bid] = n + 1
            return True

    def give(self, bid: str) -> None:
        with self._lock:
            self._n[bid] = max(0, self._n.get(bid, 0) - 1)

    def count(self, bid: str) -> int:
        return self._n.get(bid, 0)

    def total(self) -> int:
        return sum(self._n.values())


def client_gone(handler) -> bool:
    """The client's socket reads as end of file (it left). No socket: it stays."""
    sock = getattr(handler, "connection", None)
    if sock is None:
        return False
    try:
        r, _, _ = select.select([sock], [], [], 0)
        if not r:
            return False
        return not sock.recv(1, socket.MSG_PEEK)
    except (OSError, ValueError):
        return True


def _error_body(message: str, kind: str, code=None) -> dict:
    err = {"message": message, "type": kind}
    if code is not None:
        err["code"] = code
    return {"error": err}


class Relay:
    """The relay of one proxy: its backends' pools, in-flight counts and the record sink."""

    def __init__(self, service):
        self.svc = service                 # proxy.app.Service: config, table, adapters, account
        self.inflight = Inflight()
        self._pools: dict[str, tuple] = {}
        self._lock = threading.Lock()
        self.sleep = time.sleep
        self.fetch = _fetch_json
        # SRV-881: each translated answer's own blocks, so a tool loop keeps its thinking
        self.stash = stash_mod.Stash()

    def engine_holds(self, backend: dict, rid: str) -> bool:
        """Our engine still holds the request the relay filed under `rid` (a lane of its board,
        queued, prefilling or generating). False when it does not, or does not say."""
        base = backend["url"].rstrip("/")
        if base.endswith("/v1"):
            base = base[:-3]
        hdrs = self.bearer(backend)
        doc = self.fetch(base + BOARD, hdrs) if hdrs else self.fetch(base + BOARD)
        lanes = doc.get("lanes") if isinstance(doc, dict) else None
        return any(isinstance(x, dict) and x.get("relay_id") == rid for x in lanes or [])

    def bearer(self, backend: dict) -> dict | None:
        """The board read's Authorization: the backend's key, or its engine host's HTTP token
        (SRV-839, ConfigDB.key_for); None without one."""
        return self.bearer_of(backend["id"])

    def bearer_of(self, bid: str) -> dict | None:
        try:
            key = self.svc.cfg.key_for(bid)
        except Exception:                                          # noqa: BLE001
            return None
        return {"Authorization": f"Bearer {key}"} if key else None

    def pool(self, backend: dict) -> http1.Pool:
        key = (backend["url"], backend["timeouts"]["connect"])
        with self._lock:
            got = self._pools.get(backend["id"])
            if got is None or got[0] != key:
                if got is not None:
                    got[1].close()
                got = (key, http1.Pool(backend["url"], backend["timeouts"]["connect"]))
                self._pools[backend["id"]] = got
            return got[1]

    def drop_pool(self, bid: str) -> None:
        with self._lock:
            got = self._pools.pop(bid, None)
        if got is not None:
            got[1].close()

    # ----------------------------------------------------------------- the request
    def serve(self, handler, path: str, raw: bytes, body: dict, route, rec: RelayRecord) -> None:
        """Relay one request. Writes the answer on `handler` and fills `rec`; the caller writes
        the ledger row."""
        stream = bool(body.get("stream")) and path != "/v1/embeddings"
        rec.public_model = route.public
        rec.deployment = getattr(route, "deployment", None)
        cands = route.candidates()
        tries = 0
        last = None                         # (status, reason, headers, body bytes) to relay
        first_bid = cands[0][0]
        i = 0
        while i < len(cands):
            bid, upstream_model = cands[i]
            backend = self.svc.table.backend_for(bid)
            if backend is None:
                i += 1
                continue
            adapter = self.svc.adapter(backend)
            if path not in adapter.routes():
                if path in MESSAGES_PATHS and last is not None:
                    # SRV-881: a fallback a hook picked that does not speak the Messages API is
                    # not tried; the failure before it stands
                    i += 1
                    continue
                last = (404, "Not Found", [], json.dumps(_error_body(
                    f"backend '{bid}' ({backend['kind']}) does not serve {path}",
                    "invalid_request_error", "unsupported_route")).encode())
                i += 1
                continue
            drained = self.svc.drain_of(bid) or self.svc.restart_drain_of(bid)
            if drained is not None:
                rec.upstream_status = None
                last = (503, "Service Unavailable", [("Retry-After", str(DRAIN_RETRY_S))],
                        json.dumps(_error_body(f"{drained} is drained for maintenance and takes "
                                               f"no new requests", "server_busy",
                                               "node_draining")).encode())
                i += 1
                continue
            loc = self.svc.locate(bid, upstream_model)
            rec.node, rec.node_alias = (loc[0], loc[1]) if loc else (None, None)
            rec.deployment = getattr(route, "deployment", None) or (loc[2] if loc else None)
            if not self.inflight.take(bid, backend.get("max_inflight")):
                rec.upstream_status = None
                last = (503, "Service Unavailable", [("Retry-After", "1")], json.dumps(
                    _error_body(f"backend '{bid}' has {backend['max_inflight']} requests in "
                                f"flight (max_inflight)", "server_busy", "max_inflight")).encode())
                i += 1
                continue
            try:
                outcome = self._attempt(handler, path, raw, body, backend, adapter,
                                        upstream_model, stream, rec)
            finally:
                self.inflight.give(bid)
            rec.backend, rec.backend_kind, rec.backend_model = bid, backend["kind"], upstream_model
            if bid != first_bid:
                rec.fallback_from = first_bid
            if outcome[0] == "done":
                rec.retries = tries
                return
            if outcome[0] == "hook_retry":
                continue                     # response.after_engine asked once: the same try
            # a failure before any byte reached the client
            last = outcome[1]
            retry_after = outcome[2]
            hk = getattr(rec, "hk", None)
            if hk is not None:
                try:
                    fb = hk.error(last[0], outcome[3], bid, upstream_model, tries, cands[i + 1:])
                except Exception as exc:                           # noqa: BLE001
                    fb = None
                    self.svc.log(f"[hooks] request.error failed: {type(exc).__name__}")
                if fb is not None and self.svc.table.backend_for(fb[0]) is not None:
                    pick = (fb[0], fb[1] or upstream_model)
                    cands = cands[:i + 1] + [pick] + [c for c in cands[i + 1:] if c != pick]
            if tries >= MAX_RETRIES or (getattr(route, "no_replay", False)
                                        and outcome[3] != "connect"):
                break
            tries += 1
            self.svc.count_retry(bid, outcome[3])
            if i + 1 < len(cands):
                i += 1                       # a fallback, at once
                continue
            wait = retry_after if retry_after is not None else (1.0 if tries == 1 else 2.0)
            wait = min(RETRY_AFTER_CAP, wait) * (1.0 + random.uniform(0, 0.25))
            if client_gone(handler):
                rec.finish_reason = "abandoned"
                rec.status = 499
                rec.retries = tries
                return
            self.sleep(wait)
        rec.retries = tries
        status, reason, headers, data = last or (502, "Bad Gateway", [], json.dumps(
            _error_body("no backend could take the request", "upstream_error")).encode())
        rec.status = status
        if rec.finish_reason is None:
            rec.finish_reason = "refused" if status in (429, 503) else "error"
        if rec.backend is None:
            rec.backend = first_bid
            b = self.svc.table.backend_for(first_bid)
            rec.backend_kind = b["kind"] if b else None
            rec.backend_model = cands[0][1]
        self._write_plain(handler, status, headers, data, rec)

    def _attempt(self, handler, path, raw, body, backend, adapter, upstream_model, stream, rec):
        """One try on one backend. ("done",) when the client got an answer (good or not);
        ("retry", (status, reason, headers, bytes), retry_after, reason) when nothing went out
        and the failure may be tried again."""
        try:
            key = self.svc.cfg.key_for(backend["id"])
        except Exception as exc:                                   # noqa: BLE001 (SecretError)
            rec.error_type = "key_unreadable"
            return ("retry", (502, "Bad Gateway", [], json.dumps(_error_body(
                f"backend '{backend['id']}': {exc}", "upstream_error", "key_unreadable"))
                .encode()), None, "key")
        hx = getattr(rec, "hx", None)
        if hx is not None:
            # the harness (P4): the notes, the thinking switch, the side cap, the compaction
            # section for this backend's kind; the client's bytes when nothing changes
            body2, changed = hx.upstream(body, adapter, upstream_model)
            if changed:
                body, raw = body2, json.dumps(body2, ensure_ascii=False).encode()
        hk = getattr(rec, "hk", None)
        if hk is not None:
            # SRV-849: request.before_engine on the final body; the client's bytes when no hook
            # changed it
            try:
                body2, changed, deny = hk.before_engine(body, backend["id"], upstream_model)
            except Exception as exc:                               # noqa: BLE001
                body2, changed, deny = body, False, None
                self.svc.log(f"[hooks] request.before_engine failed, sent as is: "
                             f"{type(exc).__name__}")
            if deny is not None:
                self.svc.refuse(handler, rec, deny)
                return ("done",)
            if changed:
                body, raw = body2, json.dumps(body2, ensure_ascii=False).encode()
        native = passthrough.native(adapter, path)
        try:
            up_body, injected = adapter.prepare(path, raw, body, upstream_model,
                                                max_output=self._max_output(backend["id"],
                                                                            upstream_model),
                                                **self._stash_arg(handler, adapter, path,
                                                                  backend, upstream_model, rec))
        except TranslateError as exc:
            # SRV-877: a field with no equal upstream; nothing went out
            rec.error_type = "untranslatable"
            rec.finish_reason = "error"
            self._write_plain(handler, 400, [], json.dumps(exc.body()).encode(), rec)
            return ("done",)
        rec.dropped = adapter.dropped(body) if _translating(adapter, path) else None
        if getattr(rec, "thinking_dropped", 0):
            rec.dropped = list(rec.dropped or []) + ["thinking"]
        if native:
            # SRV-881: the client's own headers that go on, the backend's key for its own
            headers = passthrough.headers(key, handler.headers, adapter.opts())
        else:
            headers = adapter.headers(key) + adapter.forward(handler.headers, key)
        if hx is not None:
            headers = hx.headers(adapter, headers)
        ours = backend.get("kind") == "tandemllm"
        if ours:
            # the parallel view: our engine files the request under this id, so the proxy's lane
            # reads the engine's own live count, prefill and state (server/parallel.py peers)
            headers = headers + [("X-Tandem-Relay-Id", rec.request_id)]
            # 2026-10-05: whose memory the engine reads and writes (server/app.py `_memory_user`
            # takes it from a trusted proxy address only); SRV-733: a request let in without a
            # key is the owner's, as the engine takes one sent to it straight
            headers = headers + [("X-Tandem-User", str(getattr(rec, "user", None) or "owner"))]
        collect = getattr(self.svc, "collect", None)
        if collect is not None:                     # P6: an id the engine's plugin reports back
            up_body, headers = collect.tag(backend, path, body, up_body, headers, rec)
        pool = self.pool(backend)
        upath = adapter.upstream_path(pool, path)
        to = backend["timeouts"]
        poll = (lambda: client_gone(handler))
        conn = None
        for fresh in (False, True):
            try:
                conn = pool.get(fresh=fresh)
                rec.connect_ms += conn.connect_ms
                if ours:
                    conn.busy = lambda: self.engine_holds(backend, rec.request_id)
                conn.send("POST", upath, headers, up_body)
                if rec.t_sent is None:
                    rec.stamp_sent()
                status, reason, uh = conn.read_head(float(to["first_byte"]), poll)
                break
            except http1.Stale:
                continue
            except http1.Aborted:
                if conn is not None:
                    conn.close()
                rec.finish_reason, rec.status = "abandoned", 499
                rec.client_gone_at = time.perf_counter()
                return ("done",)
            except http1.Timeout as t:
                rec.error_type = f"timeout_{t.which}"
                kind = "timeout_" + t.which
                if t.which == "connect":
                    return ("retry", (504, "Gateway Timeout", [], json.dumps(_error_body(
                        f"backend '{backend['id']}' did not accept a connection in "
                        f"{to['connect']} s", "upstream_timeout", kind)).encode()), None, "connect")
                rec.finish_reason = "timeout"
                self._write_plain(handler, 504, [], json.dumps(_error_body(
                    f"backend '{backend['id']}' sent nothing in {to['first_byte']} s",
                    "upstream_timeout", kind)).encode(), rec)
                return ("done",)
            except http1.UpstreamError as exc:
                rec.error_type = "connect_error"
                return ("retry", (502, "Bad Gateway", [], json.dumps(_error_body(
                    f"backend '{backend['id']}': {exc}", "upstream_error", "connect_error"))
                    .encode()), None, "connect")
        else:
            return ("retry", (502, "Bad Gateway", [], json.dumps(_error_body(
                f"backend '{backend['id']}' closed the connection", "upstream_error"))
                .encode()), None, "connect")
        rec.upstream_status = status
        hmap = {k.lower(): v for k, v in uh}
        if adapter.retryable(status):
            try:
                data = conn.read_all(float(to["idle"]), poll)
                pool.put(conn)
            except (http1.Timeout, http1.Closed, http1.UpstreamError, http1.Aborted):
                data = b""
                conn.close()
            ra = None
            try:
                ra = float(hmap.get("retry-after", ""))
            except ValueError:
                ra = None
            keep = [(k, v) for k, v in uh if adapter.passes(k)]
            ctype = hmap.get("content-type")
            out_status = status
            if _translating(adapter, path):
                # the client's protocol: an OpenAI error, 529 read as 503
                out_status, data, _ = adapter.answer(status, data, int(time.time()))
                ctype = "application/json"
            if ctype:
                keep.append(("Content-Type", ctype))
            return ("retry", (out_status, reason, keep, data), ra, str(status))
        sse = "text/event-stream" in hmap.get("content-type", "")
        if stream and sse and status == 200:
            self._stream(handler, path, conn, pool, uh, adapter, injected, backend, rec, poll,
                         headers)
        elif self._whole(handler, path, conn, pool, status, uh, adapter, backend, rec,
                         poll) == "retry":
            return ("hook_retry",)
        return ("done",)

    # ----------------------------------------------------------------- answers
    def _start(self, handler, status: int, uh, backend: dict, adapter, stream: bool,
               length=None, rec=None) -> None:
        handler._px_written = True
        handler.send_response(status)
        ctype = next((v for k, v in uh if k.lower() == "content-type"), "application/json")
        handler.send_header("Content-Type", ctype)
        for k, v in uh:
            if adapter.passes(k):
                handler.send_header(k, v)
        handler.send_header("X-Tandem-Backend", backend["id"])
        if rec is not None and getattr(rec, "dropped", None):
            handler.send_header("X-Tandem-Dropped", ",".join(rec.dropped))
        if rec is not None and getattr(rec, "node_alias", None):
            handler.send_header("X-Tandem-Node", rec.node_alias)
        if rec is not None and getattr(rec, "deployment", None):
            handler.send_header("X-Tandem-Deployment", rec.deployment)
        if stream:
            handler.send_header("Cache-Control", "no-cache")
            handler.send_header("X-Accel-Buffering", "no")
            handler.send_header("Connection", "close")
            handler.close_connection = True
        else:
            handler.send_header("Content-Length", str(length or 0))
        handler.end_headers()

    def _stream(self, handler, path, conn, pool, uh, adapter, injected, backend, rec, poll,
                headers=None):
        st = Stamps()
        to = backend["timeouts"]
        hx = getattr(rec, "hx", None)
        self._start(handler, 200, uh, backend, adapter, True, rec=rec)
        w = handler.wfile
        if (handler.headers.get("X-Tandem-Cost") or "").strip() == "1" and \
                backend.get("kind") != "tandemllm":
            w = CostTap(w, lambda: self._cost_line(st, adapter, path, backend, rec))
        try:
            ended = self._pump(conn, path, adapter, st, injected, rec, w, to, poll, hx)
            if ended and _cut_short(path, st):
                # SRV-741: a body without a length (our engine's stream) ends at the close, so an
                # engine that died or a link that broke mid-answer looks like an end: no [DONE],
                # no finish and no error frame is the early close
                raise http1.Closed()
            if hx is not None:
                self._settle(handler, path, backend, adapter, st, rec, w, to, poll, hx, pool,
                             conn, headers)
            else:
                pool.put(conn)
            rec.finish_reason = _finish(st)
            if hx is not None and hx.stop == "argloop":
                rec.finish_reason = "stop"
            if st.error:
                rec.error_type = "upstream_stream_error"
        except LimitReached as lim:
            conn.close()                       # the backend stops generating
            rec.finish_reason = "limit"
            rec.error_type = lim.refusal.code
            self._limit_end(w, lim.refusal, path)
        except http1.Aborted:
            conn.close()
            rec.finish_reason = "abandoned"
            rec.client_gone_at = time.perf_counter()
        except (BrokenPipeError, ConnectionResetError, OSError):
            conn.close()                       # the client's socket failed: it left
            rec.finish_reason = "abandoned"
            rec.client_gone_at = time.perf_counter()
        except http1.Timeout as t:
            conn.close()
            rec.finish_reason = "timeout"
            rec.error_type = f"timeout_{t.which}"
            self._stream_error(w, f"backend '{backend['id']}' sent nothing for {to['idle']} s",
                               rec.error_type, path)
        except (http1.Closed, http1.UpstreamError):
            conn.close()
            rec.finish_reason = "error"
            rec.error_type = "upstream_closed"
            self._stream_error(w, f"backend '{backend['id']}' closed the stream early",
                               "upstream_closed", path)
        self._finish(st, adapter, path, backend, rec)
        rec.thinking = st.reasoning
        rec.tool_calls = len(st.tools)
        hk = getattr(rec, "hk", None)
        if hk is not None:
            try:
                # a stream that passed through (SRV-881) has only its Messages usage
                hk.stream_done(200, rec.finish_reason,
                               st.usage if st.usage is not None else st.native)
            except Exception:                                      # noqa: BLE001
                pass

    def _pump(self, conn, path, adapter, st, injected, rec, w, to, poll, hx) -> bool:
        """Copy one upstream stream to the client, event by event, the adapter reading each and
        the harness (when it runs) deciding what goes out now. True when it ended on its own;
        False when the harness stopped it (the connection is then closed)."""
        buf = bytearray()
        hk = getattr(rec, "hk", None)
        if hk is not None and not hk.watches_stream():
            hk = None
        watch = getattr(rec, "watch", None)          # SRV-873: a token's spend limit
        seen = rec.frames
        # SRV-877: a translating kind's events become the client's protocol here, before the
        # ledger's reader, the hooks, the harness and the client see them
        trs = adapter.stream_translator(int(time.time()), self._keeper(rec)) \
            if _translating(adapter, path) else None
        for piece in conn.body(float(to["idle"]), poll):
            t_arr = time.perf_counter()
            buf += piece
            out = bytearray()
            if trs is not None:
                buf = _translate(trs, buf, st, False)
            while True:
                end, sep = _event_end(buf)
                if end < 0:
                    break
                event = bytes(buf[:end + sep])
                del buf[:end + sep]
                token, drop = adapter.read_frame(path, event[:end], st, injected)
                if token:
                    rec.stamp_token(t_arr, st.kind, st.tool_name, st.tool_chars)
                if hk is not None and not drop:
                    try:
                        hk.see(event)            # SRV-849: stream.chunk's observers
                    except Exception:                              # noqa: BLE001
                        hk = None                # never the stream's failure
                if hx is None:
                    if not drop:
                        out += event
                else:
                    out += hx.feed(event, event[:end], drop)
                    if hx.stop is not None:
                        break
            if trs is not None:
                buf = trs.rest + buf
            if out:
                w.write(out)
                w.flush()
                now = time.perf_counter()
                if rec.t_first_out is None and rec.t_first_frame is not None:
                    rec.t_first_out = now
            if hx is not None and hx.stop is not None:
                conn.close()                   # the backend stops generating
                return False
            if watch is not None and rec.frames - seen >= WATCH_FRAMES:
                seen = rec.frames
                bad = watch()
                if bad is not None:
                    raise LimitReached(bad)
        if trs is not None and buf:
            # a last event without its blank line: translated whole, then read as any other
            buf = _translate(trs, buf, st, True)
            out = bytearray()
            while True:
                end, sep = _event_end(buf)
                if end < 0:
                    break
                event = bytes(buf[:end + sep])
                del buf[:end + sep]
                _, drop = adapter.read_frame(path, event[:end], st, injected)
                if not drop:
                    out += event if hx is None else hx.feed(event, event[:end], drop)
            if out:
                w.write(bytes(out))
                w.flush()
            buf = bytearray()
        if buf and (hx is None or hx.stop is None):
            if hx is None:
                w.write(bytes(buf))            # a last event without its blank line
            else:
                w.write(hx.feed(bytes(buf), bytes(buf), False))
            w.flush()
        return True

    def _settle(self, handler, path, backend, adapter, st, rec, w, to, poll, hx, pool, conn,
                headers) -> None:
        """The harness's end of a stream: the first request's usage counted, then each second
        request it asks for (E1, a regenerate, the close of a reasoning loop) relayed as the
        continuation of the first answer, then what it still owes the client."""
        if hx.stop is None:
            pool.put(conn)
        hx.usages.append(adapter.read_usage(_usage(st), path) if _usage(st) is not None
                         else None)
        n = 0
        while n < 4:
            body = hx.follow_up()
            pending = hx._lines()
            if pending:
                w.write(pending)
                w.flush()
            if body is None:
                break
            n += 1
            st2 = Stamps()
            ok = self._follow(path, backend, adapter, body, st2, rec, w, to, poll, hx, headers)
            if not ok:
                hx.follow_failed("upstream")
                break
            u = adapter.read_usage(_usage(st2), path) if _usage(st2) is not None else None
            hx.usages.append(u)
            cost = adapter.price(u, self.svc.price_of(backend["id"], rec.backend_model or
                                                      rec.public_model), rec.ts) if u else None
            hx.extra_done(u, cost)
            if st2.tools:
                st.tools |= {("f", n, i) for i in st2.tools}
            if st2.reasoning:
                st.reasoning = True
            st.finish_reason = st2.finish_reason or st.finish_reason
            st.error = st2.error or st.error
        w.write(hx.tail())
        w.flush()

    def _follow(self, path, backend, adapter, body, st, rec, w, to, poll, hx, headers) -> bool:
        """One second request, streamed through the harness as the continuation of the first
        answer. False when it could not be sent or did not answer 200 with a stream."""
        try:
            key = self.svc.cfg.key_for(backend["id"])
        except Exception:                                          # noqa: BLE001
            return False
        body = dict(body, stream=True)
        raw = json.dumps(body, ensure_ascii=False).encode()
        model = rec.backend_model or body.get("model")
        try:
            up_body, injected = adapter.prepare(path, raw, body, model,
                                                max_output=self._max_output(backend["id"], model))
        except TranslateError:
            return False
        hdrs = adapter.headers(key) + (headers[len(adapter.headers(key)):] if headers else [])
        pool = self.pool(backend)
        try:
            conn = pool.get()
            conn.send("POST", adapter.upstream_path(pool, path), hdrs, up_body)
            status, _, uh = conn.read_head(float(to["first_byte"]), poll)
        except (http1.UpstreamError, http1.Timeout, http1.Closed, http1.Aborted, OSError):
            return False
        hmap = {k.lower(): v for k, v in uh}
        if status != 200 or "text/event-stream" not in hmap.get("content-type", ""):
            try:
                conn.read_all(float(to["idle"]), poll)
                pool.put(conn)
            except Exception:                                      # noqa: BLE001
                conn.close()
            return False
        ended = self._pump(conn, path, adapter, st, injected, rec, w, to, poll, hx)
        if ended:
            pool.put(conn)
        return True

    def _stream_error(self, w, message: str, code: str, path: str = CHAT) -> None:
        try:
            if path in MESSAGES_PATHS:
                # SRV-881: a stream that passes through ends as the Messages API ends one
                w.write(("event: error\ndata: " + json.dumps(
                    {"type": "error", "error": {"type": "api_error", "message": message}})
                    + "\n\n").encode())
            else:
                w.write(("data: " + json.dumps(_error_body(message, "upstream_error", code))
                         + "\n\ndata: [DONE]\n\n").encode())
            w.flush()
        except OSError:
            pass

    @staticmethod
    def _limit_end(w, refusal, path: str) -> None:
        """SRV-873: the end of a stream its token's spend limit stopped, in its protocol."""
        try:
            if path in MESSAGES_PATHS:
                w.write(("event: error\ndata: " + json.dumps(
                    {"type": "error", "error": {"type": "permission_error",
                                                "message": refusal.message}}) + "\n\n").encode())
            else:
                w.write(("data: " + json.dumps(refusal.body()) + "\n\ndata: [DONE]\n\n")
                        .encode())
            w.flush()
        except OSError:
            pass

    def _whole(self, handler, path, conn, pool, status, uh, adapter, backend, rec, poll):
        st = Stamps()
        to = backend["timeouts"]
        try:
            data = conn.read_all(float(to["idle"]), poll)
            pool.put(conn)
        except http1.Aborted:
            conn.close()
            rec.finish_reason, rec.status = "abandoned", 499
            rec.client_gone_at = time.perf_counter()
            return
        except http1.Timeout as t:
            conn.close()
            rec.finish_reason, rec.error_type = "timeout", f"timeout_{t.which}"
            self._write_plain(handler, 504, [], json.dumps(_error_body(
                f"backend '{backend['id']}' stopped sending for {to['idle']} s",
                "upstream_timeout", rec.error_type)).encode(), rec)
            return
        except (http1.Closed, http1.UpstreamError):
            conn.close()
            rec.finish_reason, rec.error_type = "error", "upstream_closed"
            self._write_plain(handler, 502, [], json.dumps(_error_body(
                f"backend '{backend['id']}' closed the connection early", "upstream_error",
                "upstream_closed")).encode(), rec)
            return
        ctype = next((v for k, v in uh if k.lower() == "content-type"), "")
        if _translating(adapter, path):
            # SRV-877: the answer (or the error) in the client's protocol before anyone reads it
            status, data, st.native = adapter.answer(status, data, int(time.time()),
                                                     self._keeper(rec))
            ctype = "application/json"
            uh = [(k, v) for k, v in uh if k.lower() not in ("content-type", "content-length")] \
                + [("Content-Type", ctype)]
        if "json" in ctype:
            try:
                adapter.read_body(path, json.loads(data), st)
            except ValueError:
                pass
        elif "event-stream" in ctype:
            # a stream the client did not ask for: read its frames for the counts
            for ev in data.replace(b"\r\n", b"\n").split(b"\n\n"):
                adapter.read_frame(path, ev, st, False)
        hk = getattr(rec, "hk", None)
        if hk is not None and status == 200 and "json" in ctype:
            # SRV-849: tool_call.detected and response.after_engine before any byte goes out
            try:
                data2, deny, again = hk.answer(data, status, path)
            except Exception as exc:                               # noqa: BLE001
                data2, deny, again = data, None, False
                self.svc.log(f"[hooks] the answer's stages failed, sent as is: "
                             f"{type(exc).__name__}")
            if again:
                return "retry"
            if deny is not None:
                self.svc.refuse(handler, rec, deny)
                self._finish(st, adapter, path, backend, rec)
                rec.thinking = st.reasoning
                rec.tool_calls = len(st.tools)
                return None
            if data2 is not data:
                data = data2
                uh = [(k, v) for k, v in uh if k.lower() != "content-length"]
        try:
            self._start(handler, status, uh, backend, adapter, False, len(data), rec=rec)
            handler.wfile.write(data)
            handler.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            rec.finish_reason = "abandoned"
        if status >= 400 and rec.finish_reason is None:
            rec.finish_reason = "error"
            rec.error_type = rec.error_type or f"upstream_{status}"
        if rec.finish_reason is None:
            rec.finish_reason = _finish(st)
        rec.status = status
        self._finish(st, adapter, path, backend, rec)
        rec.thinking = st.reasoning
        rec.tool_calls = len(st.tools)

    def _finish(self, st: Stamps, adapter, path, backend, rec) -> None:
        hx = getattr(rec, "hx", None)
        if hx is not None and hx.usages:
            # the harness's turn: every request it sent, summed (null when one did not report)
            u = adapter.reconcile(hx.total_usage() or adapter.read_usage(None, path), st)
        else:
            u = adapter.reconcile(adapter.read_usage(_usage(st), path), st)
        rec.set_usage(u)
        rec.cost_micros = adapter.price(u, self.svc.price_of(backend["id"], rec.backend_model
                                                              or rec.public_model), rec.ts)
        rec.system_fingerprint = st.fingerprint
        rec.upstream_request = st.response_id

    def _cost_line(self, st: Stamps, adapter, path, backend, rec) -> bytes:
        """The cost comment: the usage so far (the harness's requests summed, else the stream's
        own) at the model's price, as `_finish` prices the ledger row."""
        hx = getattr(rec, "hx", None)
        if hx is not None and hx.usages:
            u = hx.total_usage() or adapter.read_usage(None, path)
        else:
            u = adapter.read_usage(_usage(st), path)
        cost = adapter.price(u, self.svc.price_of(backend["id"], rec.backend_model
                                                  or rec.public_model), rec.ts)
        return (": tandem-cost " + json.dumps({"cost_micros": cost, "backend": backend["id"]})
                + "\n\n").encode()

    def _stash_arg(self, handler, adapter, path, backend, upstream_model, rec) -> dict:
        """The thinking stash's restore for a translated chat request (SRV-881); none
        otherwise. Each try has its own: the blocks go to their own backend and model only."""
        rec.stash_scope = None
        rec.thinking_dropped = 0
        if not (_translating(adapter, path) and path == CHAT):
            return {}
        who = getattr(self.svc, "principal", None)
        p = who(handler) if callable(who) else None
        if p is not None:
            # accounts on: the gate took the credential; a token's id (kept by a rotation), else
            # the signed-in principal's
            tok = stash_mod.owner_of(p)
        else:
            tok = stash_mod.token_of(handler.headers)
            if tok == "anon":
                # a Messages API client's request reaches here without its credential: its key's
                # hash names it (proxy/messages.py, the ledger's client id)
                tok = getattr(rec, "client_id", None) or "anon"
        scope = (tok, backend["id"], upstream_model)

        def restore(msgs):
            out, n = self.stash.restore(scope, msgs)
            rec.stash_scope, rec.stash_at, rec.thinking_dropped = scope, len(out), n
            return out
        return {"stash": restore}

    def _keeper(self, rec):
        scope = getattr(rec, "stash_scope", None)
        if scope is None:
            return None
        at = rec.stash_at
        return lambda content: self.stash.keep(scope, content, at)

    def _max_output(self, bid: str, model: str | None) -> int | None:
        """The model's output limit: the operator's, else the provider's list (None unknown)."""
        try:
            row = self.svc.table.snapshot()["by_backend"].get(bid, {}).get(model)
        except Exception:                                          # noqa: BLE001
            return None
        if not isinstance(row, dict):
            return None
        v = row.get("max_output_set") or row.get("max_output")
        return v if isinstance(v, int) and v > 0 else None

    def _write_plain(self, handler, status: int, headers, data: bytes, rec) -> None:
        rec.status = status
        handler._px_written = True
        try:
            handler.send_response(status)
            ctype = next((v for k, v in headers if k.lower() == "content-type"),
                         "application/json")
            handler.send_header("Content-Type", ctype)
            for k, v in headers:
                # the relay's own (a Retry-After), or a backend's that its adapter passed
                if k.lower() != "content-type":
                    handler.send_header(k, v)
            if getattr(rec, "deployment", None):
                handler.send_header("X-Tandem-Deployment", rec.deployment)
            handler.send_header("Content-Length", str(len(data)))
            handler.end_headers()
            handler.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError, OSError):
            handler.close_connection = True


FINISH = {"stop": "stop", "length": "length", "tool_calls": "tool_calls",
          "function_call": "tool_calls", "content_filter": "content_filter", "error": "error",
          "eos": "stop", "abort": "error"}


def _usage(st):
    """The usage object the ledger reads: a translated answer's own (SRV-877), else the one the
    client got."""
    return st.native if st.native is not None else st.usage


def _translate(trs, buf: bytearray, st, last: bool) -> bytearray:
    """The whole upstream events in `buf` in the client's protocol; an unfinished event waits in
    `trs.rest` (with `last`, it is translated as it is)."""
    out = bytearray()
    while True:
        end, sep = _event_end(buf)
        if end < 0:
            break
        out += trs.feed(bytes(buf[:end]))
        del buf[:end + sep]
    if last and buf.strip():
        out += trs.feed(bytes(buf))
        buf = bytearray()
    trs.rest = bytearray(buf)
    st.native = trs.usage
    return out


def _finish(st) -> str:
    """The ledger's finish reason from the backend's: OpenAI's set, `function_call` read as
    `tool_calls`; an unknown word is `stop` when the stream ended cleanly."""
    if st.error:
        return "error"
    return FINISH.get(st.finish_reason or "stop", "stop")


def _cut_short(path: str, st) -> bool:
    """A chat or completion stream that ended without `[DONE]`, a finish reason or an error;
    a Messages stream (SRV-881) without `message_stop`, a stop reason or an error."""
    return path in (CHAT, COMPLETIONS) + MESSAGES_PATHS and not st.done and \
        st.finish_reason is None and st.error is None


def _translating(adapter, path: str) -> bool:
    """The adapter translates this request (SRV-877); a Messages API request to a Messages API
    backend passes through instead (SRV-881)."""
    return adapter.translates and not passthrough.native(adapter, path)


def _event_end(buf: bytearray) -> tuple[int, int]:
    """(index of the blank line that ends the first SSE event, its length), or (-1, 0)."""
    a = buf.find(b"\n\n")
    b = buf.find(b"\r\n\r\n")
    if a < 0 and b < 0:
        return -1, 0
    if b >= 0 and (a < 0 or b < a):
        return b, 4
    return a, 2


def endpoint_of(path: str) -> str:
    return ENDPOINTS.get(path, path.rsplit("/", 1)[-1])
