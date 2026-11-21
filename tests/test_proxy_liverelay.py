"""The live relay: the engine's live figures into the controller's live view.

A fake engine (stdlib http.server) streams `/v1/dashboard/live` and answers chat requests;
the relay (proxy/liverelay.py) keeps its rows, its `now`, its sample and its engine block
fresh; a `LiveRegistry` built the way the controller builds one (`blocks=None,
activity=False`) carries them into its in-flight rows, its history sample and its `now` —
joined by the answer's id, the engine's request id (SRV-733) — and drops them once the
engine goes quiet for longer than the TTL. The hooks default off: a registry without them
reads exactly as it did before, and a hook that raises never costs the tick its row.

The prefill phase: a new engine names its live request id in the response header at stream
open (`X-Tandem-Request-Id`), which proxy/relay.py stamps before the pump starts, so the
row joins through the whole prefill; an old engine's id arrives only with the first frame,
and there the sole-prefill heuristic joins the request when the backend has exactly one
prefill row open — never when two could be it.

Run: python tests/test_proxy_liverelay.py
"""

from __future__ import annotations

import http.client
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from proxy_util import Proxy, run  # noqa: E402

from proxy import app as proxy_app  # noqa: E402
from proxy import liverelay  # noqa: E402
from proxy.measure import RelayRecord  # noqa: E402
from tandemapp import live as live_mod  # noqa: E402

ENGINE_ID = "chatcmpl-fake0123456789abcdef01"
ENGINE_ID2 = "chatcmpl-fake1123456789abcdef01"
HDR_ID = "chatcmpl-hdr0123456789abcdef01"
HDR_ID2 = "chatcmpl-hdr1123456789abcdef01"
UNKNOWN_ID = "chatcmpl-000000000000000000000000"

#: one live event of the fake engine: one request decoding, its rates, its engine block
EVENT = {
    "contract_version": "1.2", "seq": 1,
    "engine": {"model": "fake-model", "version": "0.0.0", "draining": False,
               "kv": {"length": 100, "max_len": 4096},
               "memory": {"mem_available_gib": 9.0, "rss_gib": 8.0}, "store": None},
    "counts": {"in_flight": 1, "queued": 0, "prefilling": 0, "waiting_memory": 0,
               "decoding": 1, "completed_1m": 0, "served": 1, "errors": 0, "refused": 0},
    "now": {"decode_tps": 41.0, "prefill_tps": None, "prefilling": False,
            "tokens_per_block": None, "last_prefill_ms_ago": None,
            "prefill_live_tps": 600.0, "prefill_live_count": 1,
            "decode_live_tps": 42.5, "decode_warming": 0},
    "requests": [{"request_id": ENGINE_ID, "phase": "decode", "status": 200,
                  "decode_live_now_tps": 42.5, "decode_live_avg_tps": 40.0,
                  "decode_warmup": {"tokens": 30, "reached": True}}],
    "sample": {"t": 0.0, "decode_tps": 41.0, "prefill_tps": 500.0, "running": 1,
               "waiting": 0, "tokens": 100, "decode_live_tps": 42.5,
               "prefill_live_tps": 600.0},
    "recent": None, "sample_last": None,
}


class _Engine(ThreadingHTTPServer):
    """The fake engine: it streams live events until told to go quiet (the connection
    stays open, like a link that stopped ticking) or to drop it (a restart). `rows` are
    the in-flight rows its live stream carries; a chat answer is streamed with
    `first_delay_s` of silence before its first frame (the prefill) and, when `chat_id`
    is set, names it in `X-Tandem-Request-Id` at stream open (a new engine, SRV-733)."""

    daemon_threads = True

    def __init__(self, handler):
        super().__init__(("127.0.0.1", 0), handler)
        self.quiet = threading.Event()
        self.drop = threading.Event()
        self.rows = list(EVENT["requests"])
        self.chat_id = None
        self.first_delay_s = 0.0


def _chunk(cid, delta, finish):
    doc = {"id": cid, "object": "chat.completion.chunk", "created": 0,
           "model": "fake-model",
           "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]}
    if finish == "stop":
        doc["usage"] = {"prompt_tokens": 10, "completion_tokens": 2, "total_tokens": 12}
    return b"data: " + json.dumps(doc).encode() + b"\n\n"


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):  # noqa: N802
        if self.path.startswith("/v1/models"):
            body = json.dumps({"object": "list",
                               "data": [{"id": "fake-model", "object": "model"}]}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if not self.path.startswith("/v1/dashboard/live"):
            self.send_response(404)
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        seq = 0
        try:
            while True:
                if self.server.drop.is_set():
                    return                       # the stream dies: the connection closes
                if not self.server.quiet.is_set():
                    seq += 1
                    doc = dict(EVENT, seq=seq, requests=list(self.server.rows))
                    self.wfile.write(b"event: live\nid: %d\ndata: %s\n\n"
                                     % (seq, json.dumps(doc).encode()))
                    self.wfile.flush()
                time.sleep(0.2)
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def do_POST(self):  # noqa: N802
        if not self.path.startswith("/v1/chat/completions"):
            self.send_response(404)
            self.end_headers()
            return
        self.rfile.read(int(self.headers.get("Content-Length") or 0))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        if self.server.chat_id:
            # a new engine: its live request id, named at stream open (SRV-733 sibling)
            self.send_header("X-Tandem-Request-Id", self.server.chat_id)
        self.end_headers()
        cid = self.server.chat_id or "chatcmpl-fake-chat"
        try:
            time.sleep(self.server.first_delay_s)     # the prefill: no frame yet
            for frame in (_chunk(cid, {"content": "ok"}, None),
                          _chunk(cid, {}, "stop"), b"data: [DONE]\n\n"):
                self.wfile.write(frame)
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass

    def log_message(self, *args):
        pass


class _Cfg:
    def __init__(self, url):
        self._url = url
        self.default_backend = "e1"

    def backends(self):
        return [{"id": "e1", "kind": "tandemllm", "enabled": True, "url": self._url}]

    def key_for(self, bid):
        return None


class _Svc:
    def __init__(self, cfg):
        self.cfg = cfg


def _app_enrich(relay):
    """The controller's own enrich hook (proxy/app.py `_live_enrich`) aimed at this relay:
    the tests drive the same code the live view runs, heuristic join included."""
    def enrich(rec):
        had = "liverelay" in proxy_app.STATE
        saved = proxy_app.STATE.get("liverelay")
        proxy_app.STATE["liverelay"] = relay
        try:
            return proxy_app._live_enrich(rec)
        finally:
            if had:
                proxy_app.STATE["liverelay"] = saved
            else:
                proxy_app.STATE.pop("liverelay", None)
    return enrich


def _start(rows=None):
    """(engine, relay, registry): the fake engine on its port, the relay watching it,
    and a registry built the way proxy/app.py builds its own — its engine block the
    controller's nulls plus the relay's `engine_block()`, exactly as `_live_engine`, and
    its enrich hook the controller's own `_live_enrich`. `rows` replaces the in-flight
    rows the engine's live stream carries."""
    eng = _Engine(_Handler)
    if rows is not None:
        eng.rows = rows
    threading.Thread(target=eng.serve_forever, kwargs={"poll_interval": 0.05},
                     daemon=True).start()
    url = f"http://127.0.0.1:{eng.server_address[1]}/v1"
    relay = liverelay.LiveRelay(_Svc(_Cfg(url)), ttl_s=1.0, sync_s=0.2)
    relay.start()

    def engine_block():
        out = {"model": None, "version": "test", "draining": False,
               "kv": {"length": 0, "max_len": 0},
               "memory": {"mem_available_gib": None, "rss_gib": None}, "store": None}
        out.update(relay.engine_block() or {})
        return out

    reg = live_mod.LiveRegistry(blocks=lambda: None, last_prefill=lambda: None,
                                engine=engine_block, hz=live_mod.DEFAULT_HZ,
                                activity=False, queue_timeout=lambda: 0.0,
                                enrich=_app_enrich(relay), live_rates=relay.rates)
    return eng, relay, reg


def _decoding_rec(request_id="px-1", upstream=ENGINE_ID):
    """A relayed request mid-decode, as the relay stamps it (proxy/measure.py)."""
    rec = RelayRecord(request_id, "chat", True)
    rec.backend = "e1"
    rec.upstream_request = upstream
    rec.stamp_sent()
    rec.stamp_token(time.perf_counter(), "content")
    return rec


def _row(snap, request_id):
    return next(r for r in snap["requests"] if r["request_id"] == request_id)


def _wait(fn, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        got = fn()
        if got:
            return got
        time.sleep(0.05)
    return None


def test_relayed_row_carries_the_engine_rates_and_goes_stale():
    eng, relay, reg = _start()
    try:
        rec = _decoding_rec()
        reg.register(rec)
        assert _wait(lambda: relay.row_fields(rec)), "the relay never went fresh"
        snap = reg.snapshot()
        row = _row(snap, "px-1")
        assert row["decode_live_now_tps"] == 42.5, row
        assert row["decode_live_avg_tps"] == 40.0, row
        assert row["decode_warmup"] == {"tokens": 30, "reached": True}, row
        # the controller's own `now` cannot measure the prefill rate: the engine's stands in
        assert snap["now"]["prefill_live_tps"] == 600.0, snap["now"]
        # the engine block: the model and the KV window only the engine knows
        assert snap["engine"]["model"] == "fake-model", snap["engine"]
        assert snap["engine"]["kv"]["max_len"] == 4096, snap["engine"]
        # the history sample: the chart series the controller cannot sample itself
        reg.tick()
        snap = reg.snapshot()
        assert snap["sample"]["decode_live_tps"] == 42.5, snap["sample"]
        assert snap["sample"]["prefill_live_tps"] == 600.0, snap["sample"]
        assert snap["sample"]["decode_tps"] == 41.0, snap["sample"]
        assert snap["sample"]["prefill_tps"] == 500.0, snap["sample"]
        # quiet longer than the TTL: every figure drops back to what the controller measured
        eng.quiet.set()
        assert _wait(lambda: relay.row_fields(rec) is None and relay.rates() is None
                     and relay.engine_block() is None, timeout=4.0), "the figures went stale"
        snap = reg.snapshot()
        row = _row(snap, "px-1")
        assert row["decode_live_now_tps"] is None, row
        assert snap["now"]["prefill_live_tps"] is None, snap["now"]
        assert snap["engine"]["model"] is None, snap["engine"]
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_no_join_no_enrichment():
    """A request the engine has no row for — no answer id yet (still prefilling), or one
    the engine does not know — keeps the row exactly as the controller measured it."""
    eng, relay, reg = _start()
    try:
        fresh = _decoding_rec()
        reg.register(fresh)
        assert _wait(lambda: relay.row_fields(fresh)), "the relay never went fresh"
        unknown = _decoding_rec("px-2", "chatcmpl-000000000000000000000000")
        reg.register(unknown)
        reg.register(_decoding_rec("px-3", upstream=None))
        snap = reg.snapshot()
        assert _row(snap, "px-1")["decode_live_now_tps"] == 42.5
        rows = [r for r in snap["requests"] if r["request_id"] != "px-1"]
        assert len(rows) == 2 and all(r["decode_live_now_tps"] is None for r in rows), rows
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_reconnect_after_the_engine_drops_the_stream():
    eng, relay, reg = _start()
    try:
        rec = _decoding_rec()
        reg.register(rec)
        assert _wait(lambda: relay.row_fields(rec)), "the relay never went fresh"
        eng.drop.set()
        assert _wait(lambda: relay.row_fields(rec) is None, timeout=4.0), \
            "the dropped stream kept its figures"
        eng.drop.clear()
        assert _wait(lambda: relay.row_fields(rec), timeout=8.0), "the relay did not reconnect"
        assert reg.snapshot()["requests"][0]["decode_live_now_tps"] == 42.5
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_hooks_default_off_and_a_raising_hook_costs_nothing():
    """A registry built without the hooks — the engine's own build, and the controller's
    before this module — reads exactly as it did: nulls, no enrichment, no fill."""
    reg = live_mod.LiveRegistry(blocks=lambda: None, last_prefill=lambda: None,
                                engine=lambda: {}, activity=False,
                                queue_timeout=lambda: 0.0)
    rec = _decoding_rec()
    reg.register(rec)
    snap = reg.snapshot()
    assert _row(snap, "px-1")["decode_live_now_tps"] is None
    assert snap["now"]["prefill_live_tps"] is None
    # a hook that raises never costs the tick its row
    def boom(_rec):
        raise RuntimeError("the relay is on fire")
    reg.enrich_fn = boom
    reg.live_rates_fn = boom
    snap = reg.snapshot()
    assert _row(snap, "px-1")["decode_live_now_tps"] is None
    assert snap["now"]["prefill_live_tps"] is None


def _prefill_row(rid, tps_live, at_ms=2000.0, eta_ms=1500.0):
    """One engine live row in the prefill phase, with the activity figures the engine's
    chunk marks give it (tandemapp/activity.py `prefill_of`)."""
    return {"request_id": rid, "phase": "prefill", "status": 200,
            "decode_live_now_tps": None, "decode_live_avg_tps": None,
            "decode_warmup": None,
            "activity": {"prefill": {"tps_live": tps_live, "tps_live_avg": tps_live,
                                     "at_ms": at_ms, "eta_ms": eta_ms}}}


def _prefilling_rec(request_id="px-1", upstream=None):
    """A relayed request mid-prefill: sent upstream, no answer id yet, no first token."""
    rec = RelayRecord(request_id, "chat", True)
    rec.backend = "e1"
    rec.backend_kind = "tandemllm"
    rec.upstream_request = upstream
    rec.stamp_sent()
    return rec


def test_the_sole_prefill_row_joins_a_request_still_prefilling():
    """An old engine (no header): the answer id arrives only with the first frame, so a
    request mid-prefill has nothing to join with. Exactly one prefill row open on the
    backend means it can only be this request's: its prefill figures join the row."""
    eng, relay, reg = _start(rows=[_prefill_row(ENGINE_ID, 777.0)])
    try:
        rec = _prefilling_rec()
        reg.register(rec)
        assert _wait(lambda: relay.sole_prefill("e1")), "the relay never went fresh"
        row = _row(reg.snapshot(), "px-1")
        assert row["phase"] == "prefill", row
        assert row["prefill_live_tps"] == 777.0, row
        assert row["prefill_elapsed_ms"] == 2000.0, row
        assert row["prefill_eta_ms"] == 1500.0, row
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_two_prefill_rows_are_ambiguous_and_stay_null():
    """Two prefill rows open: either could be the request's, so neither is — the row
    stays exactly what the controller measured."""
    eng, relay, reg = _start(rows=[_prefill_row(ENGINE_ID, 777.0),
                                   _prefill_row(ENGINE_ID2, 888.0)])
    try:
        rec = _prefilling_rec()
        reg.register(rec)
        assert _wait(lambda: relay.rates()), "the relay never went fresh"
        assert relay.sole_prefill("e1") is None, "two prefill rows were not ambiguous"
        row = _row(reg.snapshot(), "px-1")
        assert row.get("prefill_live_tps") is None, row
        assert row.get("prefill_elapsed_ms") is None, row
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_the_header_join_wins_over_the_heuristic():
    """With an id stamped from the response header the join is exact and the heuristic is
    never consulted: a header id the engine has no row for stays null (never guess, even
    with one prefill row open), and the header id the engine does have joins exactly."""
    eng, relay, reg = _start(rows=[_prefill_row(ENGINE_ID, 777.0)])
    try:
        missed = _prefilling_rec("px-1", UNKNOWN_ID)
        joined = _prefilling_rec("px-2", ENGINE_ID)
        reg.register(missed)
        reg.register(joined)
        assert _wait(lambda: relay.sole_prefill("e1")), "the relay never went fresh"
        snap = reg.snapshot()
        assert _row(snap, "px-1").get("prefill_live_tps") is None, snap
        assert _row(snap, "px-2")["prefill_live_tps"] == 777.0, snap
    finally:
        relay.stop()
        eng.shutdown()
        eng.server_close()


def test_the_header_stamps_the_join_before_the_first_frame():
    """A new engine names its live request id in the response header at stream open
    (`X-Tandem-Request-Id`): proxy/relay.py stamps it before the pump starts, so the row
    joins to the engine's prefill row through the whole prefill. The engine keeps two
    prefill rows open, where the sole-prefill heuristic could not guess — so the figure
    the row shows before the first frame can only come from the header."""
    eng = _Engine(_Handler)
    eng.rows = [_prefill_row(HDR_ID, 777.0), _prefill_row(HDR_ID2, 888.0)]
    eng.chat_id = HDR_ID
    eng.first_delay_s = 2.0
    threading.Thread(target=eng.serve_forever, kwargs={"poll_interval": 0.05},
                     daemon=True).start()
    port = eng.server_address[1]
    try:
        env = {"TANDEM_BACKENDS": "e1", "TANDEM_BACKEND_E1_KIND": "tandemllm",
               "TANDEM_BACKEND_E1_URL": f"http://127.0.0.1:{port}/v1",
               "TANDEM_DEFAULT_BACKEND": "e1"}
        with Proxy(env=env) as px:
            px.sync()
            assert _wait(lambda: px.svc.liverelay.rates()), "the relay never went fresh"
            first: list[float] = []

            def fire():
                c = http.client.HTTPConnection("127.0.0.1", px.port, timeout=30)
                body = {"model": "fake-model", "stream": True,
                        "messages": [{"role": "user", "content": "hello"}]}
                c.request("POST", "/v1/chat/completions", json.dumps(body),
                          {"Content-Type": "application/json"})
                r = c.getresponse()
                while True:
                    line = r.readline()
                    if not line:
                        break
                    if line.startswith(b"data: "):
                        first.append(time.time())
                        break
                while r.read(4096):
                    pass
                c.close()

            t = threading.Thread(target=fire, daemon=True)
            t.start()
            got = _wait(lambda: next((x for x in px.svc.live.snapshot()["requests"]
                                      if x.get("prefill_live_tps") == 777.0), None),
                        timeout=6.0)
            assert got, "the header-stamped row never joined the engine's prefill row"
            assert got["phase"] == "prefill", got
            assert not first, "the join waited for the first frame"
            t.join(15.0)
    finally:
        eng.shutdown()
        eng.server_close()


if __name__ == "__main__":
    run(globals())
