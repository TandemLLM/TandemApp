"""The standalone proxy: the dashboard and the OpenAI routes in front of any backend, no torch.

    python -m proxy --config-db ~/.tandemllm/config.sqlite3 --port 8080

serves on one port:

    POST /v1/chat/completions, /v1/completions, /v1/embeddings, /v1/responses
                                   relayed to the backend the model routes to (proxy/route.py)
    POST /v1/messages, /v1/messages/count_tokens
                                   the Messages API, translated to and from the chat shape the
                                   pipeline runs on (proxy/messages.py, SRV-879), or passed
                                   through to a Messages API backend (SRV-881)
    GET  /v1/models, /v1/models/{id}
                                   each fleet deployment once under its API id (SRV-731,
                                   proxy/deployments.py), then every backend's real models,
                                   `owned_by` its id; in the Messages API's shape with
                                   `anthropic-version` (SRV-881)
    GET  /dashboard/               the dashboard (dashboard/dist, served from disk)
    GET  /v1/dashboard/*           summary, usage, requests, system, live, logs, metrics, session
    *    /v1/config/*              the Backends and Setup screens (proxy/config_api.py)
    *    /v1/fleet/*               TandemApp's controller: the fleet registry, its nodes'
                                   figures and the saved layouts (proxy/fleet.py, SRV-723);
                                   each node's Setup Guide relayed (proxy/fleetsetup.py)
    GET  /v1/fleet/ledger/{summary,usage,requests}
                                   this controller's own ledger, as /v1/dashboard/* answers it
                                   (VIS-254: a page whose /v1/dashboard/* is an engine's reads
                                   the fleet's ledger here)
    POST /v1/fleet/gateway/chat/completions
                                   /v1/chat/completions through this gateway for a page whose
                                   /v1/* is an engine's (VIS-254, the Playground's targets;
                                   `X-Tandem-Pin: <node>` pins a deployment's member)
    GET  /v1/fleet/chat, POST /v1/fleet/chat/{start,stop,restart}
                                   chatd on this controller's host with `--chat own`
                                   (proxy/appchat.py, SRV-841); `external` (the default): someone
                                   else runs it and /v1/chat-app/* goes to QSE_CHAT_URL; `off`
                                   (OPS-288): no chatd, every chat route answers 503 chat_off
    GET  /v1/fleet/app/parts, POST /v1/fleet/app/parts/{chat,conductor}
                                   what this TandemApp runs, switched from the page (OPS-288,
                                   proxy/appparts.py): Chat and the conductor on or off, with a
                                   plan and its confirm; the page's switch wins over the install's
    *    /v1/settings/*            one settings interface over the stores (SRV-845,
                                   proxy/settings.py, contracts/settings-v1): the scopes app,
                                   backends, conductor, engine, hooks and plugins, each with
                                   schema, get, plan, apply, versions and restore; change sets
                                   across scopes;
                                   the engine restarts they schedule; the audit. Reads as the
                                   dashboard, plans and writes as the configuration
    *    /v1/fleet/hooks[/...]     the remote hooks around the relay: list, create, change, test,
                                   their calls (proxy/hookapi.py, SRV-849)
    *    /v1/fleet/decision-providers[/...]
                                   who answers the conductor's questions: list, create, change,
                                   test, health, the agreement report (proxy/providerapi.py,
                                   SRV-851)
    *    /v1/fleet/plugins[/...]   the plugin registry: list, inspect a source, install from a
                                   folder, a wheel or a git commit, enable, disable, verify,
                                   remove (proxy/pluginapi.py, SRV-857); nothing of a plugin runs
    *    /v1/fleet/plugin-trust[/...]  the trusted publisher keys and the signature policy
                                   (proxy/pluginapi.py, SEC-18)
    *    /chat-engine/...          the engine chatd calls: this gateway, its models, library and
                                   conductor, and the chat engine's facts, live rows, cache
                                   routes and tokenizer (proxy/chatengine.py, SRV-843)
    GET  /metrics                  the proxy's families in the Prometheus text format
    GET  /health                   {"status": "ok"}; the full body for the admin or the machine
    *    /v1/conductor/*           the conductor's documents, journal and sessions (P4, with
                                   --conductor-config; the engine-only screens answer 404)
    GET  /v1/library[/...]         the package library for the model's scripts (P4)
    POST /v1/laya/ask              Laya, when the `decider` role names a Jev endpoint (P4)
    *    /s/<token>/v1/library..., /s/<token>/v1/laya...
                                   the same, attributed to the session the token names

The engine's own server mounts the same service with `--backends` (server/app.py); this entry
point is for everyone without a Spark. The dashboard's engine-only panels (caches, the
conductor, speculation, memory) answer 404 here and the views say so.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urlsplit

from proxy import config as proxy_config
from proxy import config_api
from proxy import deployments
from proxy import fleet as fleet_mod
from proxy import hostcollect
from proxy import nodesource
from proxy import route as route_mod
from proxy.service import PROXY_PATHS, Service
from tandemapp import auth as auth_mod
from tandemapp import chat_forward
from tandemapp import dashboard_api
from tandemapp import live as live_mod
from tandemapp import parallel as parallel_mod
from tandemapp import logbuf
from tandemapp import metrics as metrics_mod
from tandemapp import static

STATE: dict = {}
GATEWAY = "/v1/fleet/gateway/chat/completions"
# SRV-733: on this server's /v1/models answers, so a page behind a reverse proxy knows whether its
# own /v1 (the data plane) reaches this controller or an engine (dashboard lib/fleet-state.ts)
GATEWAY_HEADER = ("X-Tandem-Gateway", "tandem-app")
# a /v1/fleet/* request body (not the gateway's chat body) is read only up to this: the body is
# read before the route's guard, so a bigger one is refused unread (an import is a few KiB)
FLEET_BODY_MAX = 4 * 1024 * 1024
# SRV-605: the request phase has a timeout, the answer has none. A client that connects and sends
# nothing, stops halfway through its request, or keeps a connection idle between requests gives
# the thread back after this many seconds (`--idle-timeout`, TANDEM_IDLE_TIMEOUT_S; 0 for none).
# Handler.handle_one_request arms it on the client's socket before each request line and
# send_response lifts it as the answer starts, so a stream (a relayed answer, the live view, the
# logs, a chatd SSE, the chat WebSocket; chat_forward lifts it before chatd's head) is never cut
# by it, however long its upstream is quiet or the client is slow to read.
IDLE_TIMEOUT_S = 60.0

# The body framing -- BodyError, the cap, the Content-Length spine, the chunked parser --
# is tandemapp/http_body.py, the same code the standalone server calls: the two servers
# carried it line for line and a fix had to land twice.
from tandemapp import http_body  # noqa: E402

BodyError = http_body.BodyError


def messages_wants_models(headers) -> bool:
    from proxy import messages as messages_mod
    return messages_mod.wants_models(headers)


def version() -> str:
    """VERSION in a checkout, the distribution's version when installed (proxy/_version.py)."""
    from proxy import _version
    return _version.release()


def _live_engine() -> dict:
    """The live stream's engine block for a server with no engine: the proxy's in-flight
    count."""
    svc: Service = STATE["svc"]
    return {"model": None, "version": STATE.get("version", ""),
            "draining": bool(STATE.get("draining")), "kv": {"length": 0, "max_len": 0},
            "memory": {"mem_available_gib": None, "rss_gib": None}, "store": None}


def _live_status() -> dict:
    svc: Service = STATE["svc"]
    n = svc.relay.inflight.total()
    return {"status": "draining" if STATE.get("draining") else ("busy" if n else "ok"),
            "running": n, "waiting": 0, "lane": 0,
            "uptime_s": round(time.time() - svc.started, 1),
            "last_request_at": None, "backends": svc.backends_view()}


def _hosts() -> list | None:
    """`system.hosts`: this machine first, then the backends' remote hosts. The fleet's sampler
    reads the same collector (every 2 s, so its 5-minute charts have a sample per point)."""
    svc: Service = STATE["svc"]
    args = STATE.get("args") or {}
    return hostcollect.hosts_for(
        STATE, svc.cfg.backends(),
        hostcollect.engine_entry(args.get("port"), "this proxy", backend="proxy"),
        paths=[os.path.dirname(svc.cfg.path)], fast_period=fleet_mod.SAMPLE_S - 0.25)


def _system() -> dict:
    svc: Service = STATE["svc"]
    led = STATE.get("ledger")
    li = led.info() if led is not None else None
    hosts = _hosts()
    mem = dashboard_api.meminfo()
    if hosts and mem["unified_total_bytes"] is None:          # no /proc here (macOS)
        mem["unified_total_bytes"] = hosts[0]["memory"]["total_bytes"]
        mem["unified_available_bytes"] = hosts[0]["memory"]["available_bytes"]
    gpu = hostcollect.gpu_block(STATE) if hosts else None
    return {
        "engine": {"version": STATE.get("version", ""), "git_sha": "", "code_sha256": "",
                   "started_at": dashboard_api.iso_utc_s(svc.started),
                   "uptime_s": round(time.time() - svc.started, 1), "pid": os.getpid(),
                   "model": None, "base_model": None, "max_len": None, "drafter": None,
                   "tree": False, "reasoning_format": None, "reasoning_effort": None,
                   "status": _live_status()["status"]},
        "flags": {"args": dashboard_api.redact_args(STATE.get("args") or {}),
                  "env": dashboard_api.redact_env(dict(os.environ))},
        "memory": dict(mem, gpu_allocated_bytes=None,
                       gpu_reserved_bytes=None, gpu_max_allocated_bytes=None),
        "gpu": gpu or STATE.setdefault("gpu", dashboard_api.GpuSampler()).sample(),
        "caches": None,
        "queue": {"running": svc.relay.inflight.total(), "waiting": 0, "max_queue": None,
                  "queue_timeout_s": None, "request_timeout_s": None},
        "inflight": {"served": 0, "refused": 0, "errors": 0, "timeouts": 0, "abandoned": 0},
        "ledger": {"enabled": li is not None,
                   "rows": (li or {}).get("rows") or 0, "bytes": (li or {}).get("bytes") or 0,
                   "oldest": dashboard_api.iso_utc((li or {}).get("oldest_ms")),
                   "queue": (li or {}).get("queue") or 0,
                   "dropped": (li or {}).get("dropped") or 0},
        "disk": {"state_dir_free_bytes": dashboard_api.disk_free(os.path.dirname(
            STATE["svc"].cfg.path))},
        "proxy": {"mode": "standalone", "backends": svc.backends_view()},
        **({"hosts": hosts} if hosts is not None else {}),
    }


def dashboard() -> "dashboard_api.DashboardAPI":
    api = STATE.get("dashboard_api")
    if api is None:
        api = STATE["dashboard_api"] = dashboard_api.DashboardAPI(
            STATE.get("ledger"), live=_live_status, system=_system,
            backends=lambda: STATE["svc"].cfg.backends(), clients_path=clients_path(),
            tokens=lambda: STATE["accounts"].tokens.labels())
    return api


def clients_path() -> str:
    """The client names the usage views show (`{client id: name}`): TANDEM_CLIENTS_JSON, else
    clients.json beside the config database, else the engine's on this host as before (SRV-841: a
    host with no engine has none of it)."""
    got = os.environ.get("TANDEM_CLIENTS_JSON")
    if got:
        return got
    own = os.path.join(os.path.dirname(STATE["svc"].cfg.path), "clients.json")
    return own if os.path.isfile(own) else dashboard_api.CLIENTS_JSON


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TandemLLM-proxy"

    def log_message(self, fmt, *args):
        if STATE.get("verbose"):
            sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    def handle_one_request(self):
        self.connection.settimeout(self.timeout)          # the request phase (IDLE_TIMEOUT_S)
        self._body_done = False
        try:
            super().handle_one_request()
        finally:
            acc = STATE.get("accounts")
            if acc is not None:         # None only between a test's build and its server
                acc.done(self)          # SEC-24: no longer one of its session's open streams
        # a chunked body the handler never read stayed on the wire and the next request on
        # the keep-alive connection was parsed from the middle of it (the size line as a
        # request line). A body this handler never touched at all closes the connection,
        # the same decision the standalone server made.
        if (not self.close_connection and not self._body_done
                and "chunked" in (self.headers.get("Transfer-Encoding") or "").lower()):
            self.close_connection = True
        # RFC 9112 section 6.1: a request with BOTH Transfer-Encoding: chunked and
        # Content-Length is answered and then the connection is closed -- the two
        # framings disagree about where the body ends. The rule is the engine server's
        # (tandemapp/http_body.py), so both servers answer such a request the same way.
        if not self.close_connection and http_body.conflicting_framing(self.headers):
            self.close_connection = True

    def send_response(self, code, message=None):
        self.connection.settimeout(None)                  # the answer: no timeout
        super().send_response(code, message)

    _principal = None                # SEC-22: who asks (the gate's)

    def end_headers(self):
        # SEC-24: a session's renewed cookie (at most once a day), unless the answer sets its own
        cookie = self.__dict__.pop("_set_cookie", None)
        sent = getattr(self, "_headers_buffer", ())
        if cookie is not None and not any(line[:11].lower() == b"set-cookie:" for line in sent):
            self.send_header("Set-Cookie", cookie)
        super().end_headers()

    # ----------------------------------------------------------------- helpers
    def _json(self, code: int, payload, extra_headers: tuple = ()) -> None:
        raw = json.dumps(payload, ensure_ascii=False).encode()
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            for k, v in extra_headers:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _raw_json(self, code: int, raw: bytes, extra_headers: tuple = ()) -> None:
        try:
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            for k, v in extra_headers:
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    def _send_empty(self, code: int, headers: tuple = ()) -> None:
        try:
            self.send_response(code)
            for k, v in headers:
                self.send_header(k, v)
            self.send_header("Content-Length", "0")
            self.end_headers()
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    _body_done = False               # the request in hand has had its body read to the end

    def _read_raw(self) -> bytes:
        """The request's body, consumed whole or not at all -- tandemapp/http_body.py owns
        the framing, the same module the standalone server calls. An answer written with
        the body still unread is a desync one request away: every path out of here has
        read to the end of the framing, and BodyError (or handle_one_request) closes the
        connection."""
        try:
            raw = http_body.read_body(self.rfile, self.headers)
        except BodyError:
            self.close_connection = True     # the promised bytes, if they ever come,
            raise                            # would be read as the next request
        self._body_done = True
        return raw

    def _body_error(self, exc) -> None:
        """A framing/body refusal: 400 or 413 with the error's own message. The connection
        closes afterwards: the unread body would be the next request."""
        self._json(exc.code, {"error": {"type": exc.type, "message": exc.message}})

    def _fleet_too_big(self, path: str) -> bool:
        """A /v1/fleet/* body over FLEET_BODY_MAX: 413, unread, and the connection closed."""
        if not path.startswith("/v1/fleet/") or path == GATEWAY:
            return False
        try:
            n = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            n = -1
        cap = FLEET_BODY_MAX
        if path == "/v1/fleet/plugins/upload":
            from proxy import pluginapi          # SRV-857: a wheel's bytes, their own cap
            cap = pluginapi.UPLOAD_MAX
        if 0 <= n <= cap:
            return False
        self.close_connection = True
        self._json(413, {"error": {"type": "too_large", "message": f"a fleet request's body is "
                                   f"at most {FLEET_BODY_MAX // (1024 * 1024)} MiB"}})
        return True

    def _peer(self) -> str:
        return (self.client_address or ("",))[0]

    def _allowed(self, route: str) -> bool:
        verdict = STATE["auth"].decide(route, self._peer(), self.headers)
        if verdict == "ok":
            return True
        path = self.path.split("?")[0]
        if verdict == "absent":
            self._json(404, {"error": {"type": "not_found", "message": f"no route {path}"}})
        elif verdict == "forbidden":
            # SEC-22 (accounts only): signed in, but the role lacks the route kind's permission
            from proxy import authz
            perm = authz.KIND_PERMISSION.get(route)
            self._json(403, {"error": {"type": "forbidden", "code": "permission_denied",
                                       "permission": perm,
                                       "message": f"this needs the permission {perm}"}})
        else:
            self._json(401, {"error": {"type": "unauthorized",
                                       "message": "sign in, or send an API token"}},
                       extra_headers=(("WWW-Authenticate", 'Bearer realm="TandemLLM"'),))
        return False

    def _cross_site(self) -> bool:
        """A write from another site or origin (tandemapp/auth.py same_origin): 403, True. The
        configuration's and the conductor's writes take no cookie on an open controller or from
        this machine, and a form's POST needs no preflight (6.3b)."""
        bad = auth_mod.same_origin(self.headers)
        if bad is None:
            return False
        self._json(bad[0], {"error": {"type": bad[1], "code": bad[1], "message":
                                      "a request from another site or origin cannot change "
                                      "this server"}})
        return True

    def _reader_gone(self) -> bool:
        from proxy.relay import client_gone
        return client_gone(self)

    # ----------------------------------------------------------------- routes
    def _gate(self, method: str) -> bool:
        """SEC-22/SEC-24: the one rule in front of every route (proxy/accounts.py); True when
        it answered."""
        return STATE["accounts"].gate(self, method)

    def _auth_api(self, method: str, path: str, raw: bytes | None) -> bool:
        """`/v1/auth/*` (proxy/authapi.py)."""
        acc = STATE["accounts"]
        if not (path == "/v1/auth" or path.startswith("/v1/auth/")):
            return False
        from proxy import authapi
        authapi.handle(self, acc, method, path, raw)
        return True

    def do_GET(self):
        if self._gate("GET"):
            return
        if self._strip_session():
            return
        if chat_forward.route(self, "GET", STATE["auth"]):     # the Chat tab's chatd (SRV-343)
            return
        if self._chat_engine("GET"):
            return
        raw = self.path.split("?")[0]
        if raw == "/dashboard" or raw.startswith("/dashboard/"):
            return static.serve(self, self.path, STATE.get("dashboard_dir"))
        if raw.startswith(("/plugin-panels/", "/plugin-backends/")):
            # SRV-859: a running plugin's panels and backends (proxy/pluginweb.py)
            from proxy import pluginweb
            if raw.startswith(pluginweb.PANELS):
                return pluginweb.panel(self, STATE["svc"], raw)
            return pluginweb.backend(self, STATE["svc"], "GET", raw, None)
        path = raw.rstrip("/") or "/"
        if path == "/":
            return self._send_empty(302, (("Location", "/dashboard/"),))
        if self._auth_api("GET", path, None):
            return
        if path.startswith("/v1/dashboard/"):
            return self._dashboard_get(path)
        if path == "/v1/config" or path.startswith("/v1/config/"):
            return config_api.handle(self, STATE["svc"], "GET", path, None)
        if path.startswith("/v1/fleet/"):
            return self._fleet("GET", path, None)
        if path == "/v1/settings" or path.startswith("/v1/settings/"):
            return self._settings("GET", path, None)
        if path in ("/health", "/healthz", "/v1/health"):
            code = 503 if STATE.get("draining") else 200
            out = {"status": "draining" if STATE.get("draining") else "ok"}
            if STATE["auth"].decide("health_full", self._peer(), self.headers) == "ok":
                out["backends"] = STATE["svc"].backends_view()
                out["version"] = STATE.get("version")
                # OPS-246: the page's bundle this server serves (its content hash and manifest)
                out["dashboard"] = static.bundle_info(STATE.get("dashboard_dir"))
            # SEC-26: the page shows the accounts' sign-in (a header: the body of /health keeps
            # its keys)
            return self._json(code, out, extra_headers=(("X-Tandem-Auth", "accounts"),))
        if path == "/metrics":
            if not self._allowed("metrics"):
                return
            return self._metrics()
        if (path == "/v1/models" or path.startswith("/v1/models/")) and \
                messages_wants_models(self.headers):
            # SRV-881: a Messages API client's list, in its own shape (proxy/messages.py)
            from proxy import messages as messages_mod
            return messages_mod.models(self, STATE["svc"], path)
        if path == "/v1/models":
            ok, _ = STATE["svc"].authorize(self, (GATEWAY_HEADER,))
            if not ok:
                return
            # SRV-733: beside `data`, what a TandemLLM engine's own list holds (decision models)
            return self._json(200, {"object": "list",
                                    "data": STATE["svc"].visible_models(int(STATE["svc"].started)),
                                    **STATE["svc"].model_extras()},
                              extra_headers=(GATEWAY_HEADER,))
        if path.startswith("/v1/models/"):
            # the list above asks for a key; a single card was answering without one,
            # so a configured deployment leaked model ids and card metadata to strangers
            ok, _ = STATE["svc"].authorize(self, (GATEWAY_HEADER,))
            if not ok:
                return
            name = unquote(path[len("/v1/models/"):])
            card = STATE["svc"].visible_card(name, int(STATE["svc"].started))
            if card is None:
                # SRV-733: an id the default engine answers that no list names (its raw model's
                # alias, a decision model): the engine's own answer
                got = STATE["svc"].default_card(name)
                if got is not None and got[0] in (200, 404):
                    return self._raw_json(got[0], got[1], (GATEWAY_HEADER,))
                return self._json(404, {"error": {"message": f"no model {name!r}",
                                                  "type": "invalid_request_error",
                                                  "param": "model", "code": "model_not_found"}},
                                  extra_headers=(GATEWAY_HEADER,))
            return self._json(200, card, extra_headers=(GATEWAY_HEADER,))
        if path.startswith("/v1/conductor/"):
            return self._conductor("GET")
        if (path == "/v1/library" or path.startswith("/v1/library/")) and \
                STATE.get("conductor") is not None:
            from tandemapp import library_api
            return library_api.handle_get(self, STATE["conductor"])
        if path in ("/v1/laya", "/v1/laya/") and STATE.get("laya") is not None:
            from tandemapp import laya_api
            return laya_api.handle_get(self, STATE["laya"], STATE)
        return self._json(404, {"error": {"message": f"no route {path}", "type": "not_found"}})

    def _chat_engine(self, method: str) -> bool:
        """SRV-843: `/chat-engine/...` (proxy/chatengine.py), the engine chatd calls. The prefix
        comes off; a route the controller's own answers as it is goes on (False), the rest is
        answered here (True)."""
        from proxy import chatengine
        rest = chatengine.strip(self.path)
        if rest is None:
            return False
        self.path = rest
        path, _, query = rest.partition("?")
        path = path.rstrip("/") or "/"
        if chatengine.passes(method, path):
            return False
        chatengine.handle(self, STATE, method, path, query)
        return True

    def _conductor(self, method: str, body=None):
        cond = STATE.get("conductor")
        if cond is None:
            return self._json(404, {"error": {"type": "not_found",
                                              "message": "the conductor is off on this proxy "
                                                         "(--conductor-config off)"}})
        from tandemapp.conductor import routes as conductor_routes
        return conductor_routes.handle(self, cond, method, body, STATE["auth"])

    def _strip_session(self) -> bool:
        """`/s/<token>/v1/library...` and `/s/<token>/v1/laya...` (server/session_tokens.py):
        the prefix off, the session on the handler; True when answered here (a 404)."""
        from tandemapp import session_tokens as st_mod
        self._sess = self._stok = None
        path, tok = st_mod.strip(self.path)
        if tok is None:
            return False
        p = path.split("?")[0].rstrip("/")
        if not (p in ("/v1/library", "/v1/laya") or p.startswith("/v1/library/")
                or p.startswith("/v1/laya/")):
            self._json(404, {"error": {"message": f"no route {p} under a session address",
                                       "type": "not_found"}})
            return True
        self.path, self._stok = path, tok
        reg = STATE.get("session_tokens")
        self._sess = reg.key_for(tok) if reg is not None else None
        return False

    def do_POST(self):
        if self._gate("POST"):
            return
        if self._strip_session():
            return
        if chat_forward.route(self, "POST", STATE["auth"]):
            return
        if self._chat_engine("POST"):
            return
        path = self.path.split("?")[0].rstrip("/")
        t_arrival = time.perf_counter()
        if self._fleet_too_big(path):
            return
        from proxy import messages as messages_mod
        try:
            raw = self._read_raw()
        except BodyError as exc:
            if messages_mod.is_messages(path):
                return messages_mod.body_error(self, exc)
            return self._body_error(exc)
        if self._auth_api("POST", path, raw):
            return
        if messages_mod.is_messages(path):          # SRV-879: the Messages API, its own shapes
            return messages_mod.handle(self, STATE["svc"], path, raw, t_arrival)
        if path.startswith("/v1/collect/"):          # P6: a plugin's batch, its own token
            from proxy import collect_api
            return collect_api.handle(self, STATE["svc"], path, raw)
        if path.startswith("/plugin-backends/"):     # SRV-859: a plugin's backend (loopback)
            from proxy import pluginweb
            return pluginweb.backend(self, STATE["svc"], "POST", path, raw)
        if path == GATEWAY:
            return self._gateway(raw, t_arrival)
        if path.startswith("/v1/fleet/"):
            return self._fleet("POST", path, raw)
        if path == "/v1/settings" or path.startswith("/v1/settings/"):
            return self._settings("POST", path, raw)
        try:
            body = json.loads(raw or b"{}")
        except (ValueError, RecursionError) as exc:
            if path in PROXY_PATHS:
                # a row as the engine writes one for a body it cannot read (SRV-733: counted once
                # when the data plane comes here)
                STATE["svc"].account_refusal(self, path, None, 400)
            return self._json(400, {"error": {"message": f"bad json: {exc}",
                                              "type": "invalid_request_error"}})
        if path == "/v1/dashboard/session":
            return self._session("POST", body)
        if (path == "/v1/config" or path.startswith(("/v1/config/", "/v1/conductor/"))) and \
                self._cross_site():
            return
        if path == "/v1/config" or path.startswith("/v1/config/"):
            return config_api.handle(self, STATE["svc"], "POST", path, body)
        if path in PROXY_PATHS:
            return self._complete(path, raw, body, t_arrival)
        if path in deployments.CACHE_PATHS and STATE["svc"].cache_route is not None:
            return self._cache(path, body)
        if path.startswith("/v1/conductor/"):
            return self._conductor("POST", body)
        if path == "/v1/laya/ask" and STATE.get("laya") is not None:
            from tandemapp import laya_api
            return laya_api.handle_post(self, STATE["laya"], STATE, body)
        return self._json(404, {"error": {"message": f"no route {path}", "type": "not_found"}})

    def _complete(self, path, raw, body, t_arrival):
        svc: Service = STATE["svc"]
        ok, user = svc.authorize(self)
        if not ok:
            svc.account_refusal(self, path, body, 401, finish="refused",
                                error_type="invalid_api_key")
            return
        if not isinstance(body, dict):
            return self._json(400, {"error": {"message": "the body must be a JSON object",
                                              "type": "invalid_request_error"}})
        from proxy import deployments
        try:
            route = svc.resolve(body, self.headers)
        except route_mod.NotFound as exc:
            svc.account_refusal(self, path, body, 404, user=user, error_type="model_not_found")
            return self._json(404, exc.body())
        except deployments.Refused as exc:         # SRV-731: a stopped deployment, a Split
            svc.account_refusal(self, path, body, exc.status, user=user, finish="refused",
                                error_type=exc.code)
            return self._json(exc.status, exc.body(), extra_headers=exc.headers)
        svc.serve(self, path, raw, body, route, user, t_arrival=t_arrival)

    def _cache(self, path, body):
        """SRV-741: a cache route on the member of a deployment that holds the conversation,
        under the rule of the completions (the proxy's API keys when users exist)."""
        svc: Service = STATE["svc"]
        ok, _user = svc.authorize(self)
        if not ok:
            return
        try:
            # SRV-867: a token with a model list peeks and warms only those models
            svc.check_model(body.get("model") if isinstance(body, dict) else None)
            st, doc, hdrs = svc.cache_route(path, body, self.headers)
        except route_mod.NotFound as exc:
            return self._json(404, exc.body())
        except deployments.Refused as exc:
            return self._json(exc.status, exc.body(), extra_headers=exc.headers)
        return self._json(st, doc, extra_headers=hdrs)

    def _gateway(self, raw: bytes, t_arrival: float) -> None:
        """`POST /v1/fleet/gateway/chat/completions` (VIS-254): the page's own JSON POST, then the
        completion as /v1/chat/completions takes it (the proxy's API keys when users exist)."""
        bad = auth_mod.same_origin_json(self.headers)
        if bad:
            words = {"json_only": "the gateway takes a JSON body (Content-Type: application/json)",
                     "cross_site": "a request from another site cannot use the gateway",
                     "cross_origin": "a request from another origin cannot use the gateway"}
            return self._json(bad[0], {"error": {"type": "invalid_request_error", "code": bad[1],
                                                 "message": words.get(bad[1], bad[2])}})
        try:
            body = json.loads(raw or b"{}")
        except (ValueError, RecursionError) as exc:
            return self._json(400, {"error": {"message": f"bad json: {exc}",
                                              "type": "invalid_request_error"}})
        return self._complete("/v1/chat/completions", raw, body, t_arrival)

    def _reader(self) -> tuple:
        """SRV-873, SEC-28: what the ledger views show this reader, (own, me). `own`: None for
        all (the permission `users`), else its own tokens' ids (a token reads its owner's; one
        without an owner only itself). `me`: the reader's own names; any other account's
        username is shown as "another account"."""
        a = STATE.get("accounts")
        p = self._principal
        if a is None:
            return None, ()
        from proxy import authz
        if p is not None and authz.allow(p, "users", a.policy):
            return None, ()
        if p is None:
            return (), ()
        try:
            if p.kind in ("account", "local"):
                return tuple(r["id"] for r in a.tokens.mine(p.id)), (p.name,)
            if p.kind in ("token", "service"):
                row = a.tokens.get(p.id)
                if row is not None and row["owner_id"]:
                    acc = a.store.get(row["owner_id"])
                    return (tuple(r["id"] for r in a.tokens.mine(row["owner_id"])),
                            (acc["username"],) if acc is not None else ())
                return (p.id,), ()
        except Exception:                                          # noqa: BLE001
            pass
        return (), ()

    def _fleet_ledger(self, name: str) -> None:
        """`GET /v1/fleet/ledger/{summary,usage,requests}` (VIS-254): this controller's ledger
        under the dashboard's rule, the same answers as /v1/dashboard/*."""
        if not self._allowed("dashboard"):
            return
        api = dashboard()
        own, me = self._reader()
        fn = {"summary": api.summary, "usage": lambda q: api.usage(q, own, me),
              "requests": lambda q: api.requests(q, own, me)}.get(name)
        if fn is None:
            return self._json(404, {"error": {"type": "not_found",
                                              "message": f"no route /v1/fleet/ledger/{name}"}})
        q = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        try:
            return self._json(200, fn(q), extra_headers=(("Cache-Control", "no-store"),))
        except dashboard_api.ApiError as exc:
            return self._json(exc.status, exc.body())

    def _write(self, method: str):
        if self._gate(method):
            return
        if chat_forward.route(self, method, STATE["auth"]):
            return
        if self._chat_engine(method):
            return
        path = self.path.split("?")[0].rstrip("/")
        if self._fleet_too_big(path):
            return
        try:
            raw = self._read_raw()
        except BodyError as exc:
            return self._body_error(exc)
        if self._auth_api(method, path, raw):
            return
        if path.startswith("/v1/fleet/"):
            return self._fleet(method, path, raw)
        if path == "/v1/settings" or path.startswith("/v1/settings/"):
            return self._settings(method, path, raw)
        try:
            body = json.loads(raw) if raw else None
        except (ValueError, RecursionError) as exc:
            return self._json(400, {"error": {"message": f"bad json: {exc}",
                                              "type": "invalid_request_error"}})
        if method == "DELETE" and path == "/v1/dashboard/session":
            return self._session("DELETE")
        if (path == "/v1/config" or path.startswith(("/v1/config/", "/v1/conductor/"))) and \
                self._cross_site():
            return
        if path == "/v1/config" or path.startswith("/v1/config/"):
            return config_api.handle(self, STATE["svc"], method, path, body)
        if path.startswith("/v1/conductor/"):
            return self._conductor(method, body)
        return self._json(404, {"error": {"message": f"no route {path}", "type": "not_found"}})

    def do_PUT(self):
        return self._write("PUT")

    def do_PATCH(self):
        return self._write("PATCH")

    def do_DELETE(self):
        return self._write("DELETE")

    def _fleet(self, method: str, path: str, raw: bytes | None) -> None:
        """`/v1/fleet/*` (proxy/fleet.py): reads (an action's plan too) and a page's window
        layout under the dashboard's rule, writes (a node's alias or colour, an acknowledgement,
        a drain, a restart, a link test; discovery and the Add Spark session, read too) under
        the configuration's, pairing, release, forget, a lease's release and the wizard's
        connect, check and install under `fleet_trust` (server/auth.py), and the controller's
        own sign-in at /v1/fleet/session."""
        sub = path.rstrip("/").split("/")[3:]
        if method != "GET":
            # a write from another site or origin is refused before anything reads it: the
            # session cookie is SameSite=Strict, but trusted-local and an open controller need no
            # cookie, and a form's POST needs no preflight (6.3b)
            bad = auth_mod.same_origin(self.headers)
            if bad is not None:
                return self._json(bad[0], {"error": {"type": bad[1], "code": bad[1], "message": (
                    "a request from another site cannot change the fleet" if bad[1] ==
                    "cross_site" else "a request from another origin cannot change the fleet")}})
        if method == "GET" and len(sub) == 2 and sub[0] == "ledger":
            return self._fleet_ledger(sub[1])
        if len(sub) >= 4 and sub[0] == "nodes" and sub[2] == "setup":
            return self._fleet_setup(method, unquote(sub[1]), [unquote(x) for x in sub[3:]], raw)
        if path.rstrip("/") == "/v1/fleet/plugins/upload":
            # SRV-857: a wheel's bytes, not JSON (proxy/pluginapi.py)
            from proxy import pluginapi
            return pluginapi.handle_upload(self, STATE["svc"], method, raw)
        body = None
        if raw:
            try:
                body = json.loads(raw)
            except (ValueError, RecursionError) as exc:
                return self._json(400, {"error": {"type": "bad_request",
                                                  "message": f"bad json: {exc}"}})
        if path.rstrip("/") == "/v1/fleet/session":
            return self._session(method, body, fleet=True)
        parts = path.rstrip("/").split("/")[3:]
        if parts[:1] == ["hooks"]:
            # SRV-849: the hooks' admin API (proxy/hookapi.py), its own rights
            from proxy import hookapi
            return hookapi.handle(self, STATE["svc"], method, [unquote(x) for x in parts[1:]],
                                  body)
        if parts[:1] == ["decision-providers"]:
            # SRV-851: the decision providers' admin API (proxy/providerapi.py)
            from proxy import providerapi
            return providerapi.handle(self, STATE["svc"], method,
                                      [unquote(x) for x in parts[1:]], body)
        if parts[:1] == ["plugins"]:
            # SRV-857: the plugins' admin API (proxy/pluginapi.py), its own rights
            from proxy import pluginapi
            return pluginapi.handle(self, STATE["svc"], method, [unquote(x) for x in parts[1:]],
                                    body)
        if parts[:1] == ["plugin-trust"]:
            # SEC-18: the trusted publisher keys and the signature policy (proxy/pluginapi.py)
            from proxy import pluginapi
            return pluginapi.handle_keys(self, STATE["svc"], method,
                                         [unquote(x) for x in parts[1:]], body)
        if parts == ["app", "password"]:
            return self._app_password(method, body)
        if parts[:1] == ["app"] and method == "POST":
            bad = auth_mod.same_origin_json(self.headers)
            if bad is not None:
                return self._json(bad[0], {"error": {"type": bad[1], "message": bad[2]}})
        if parts[:1] == ["app"]:
            # 6.3b: the page's view follows the reads; the export (the whole registry, a backup)
            # and a plan need write rights, an import that writes pairing's
            route = ("dashboard" if method == "GET" and parts in (["app"], ["app", "parts"]) else
                     "fleet_trust" if parts == ["app", "import", "apply"] else "conductor_write")
        elif len(parts) >= 4 and parts[0] == "nodes" and parts[2] == "setup-confirm":
            # VIS-252: a Setup Guide write's plan, under the rule of the engine's own /v1/setup
            route = "setup"
        elif parts[:1] == ["discovery"]:
            # SRV-727: the LAN scan's consent is a write, and the list (~/.ssh/config's hosts,
            # what answers on the LAN) is read with a write's rights too
            route = "conductor_write"
        elif parts[:1] == ["add"]:
            # VIS-246: the wizard's session is read as discovery is; connecting, checking over
            # SSH and installing log in to a machine and pair it: pairing's rights
            route = "fleet_trust" if method == "POST" and (
                parts[-1:] in (["connect"], ["check"], ["install"])) else "conductor_write"
        elif parts[:1] == ["health"]:
            # SRV-835 (proxy/fleethealth.py): reading and checking again is a read; a fix needs
            # the rights of what it does (Start: the Setup Guide's, the token and an upgrade:
            # pairing's, chatd: the conductor's writes)
            from proxy import fleethealth
            route = fleethealth.route_for(method, parts, body)
        elif parts[:1] == ["engines"]:
            # SRV-827..833 (proxy/engineonboard.py): pairing and connecting an engine host change
            # who controls it, pairing's rights; the LAN search and an address probe, discovery's;
            # its mode and a model download, the Setup Guide's writes; a read, the dashboard's
            route = ("fleet_trust" if method == "POST" and parts[1:2] in (["pair"], ["connect"])
                     else "conductor_write" if parts[1:2] in (["discover"], ["probe"])
                     else "setup" if method != "GET" else "dashboard")
        elif method == "GET" or (method == "PUT" and parts[:1] == ["layouts"]) or \
                (method == "POST" and parts == ["placement"]):
            # SRV-731: the placement planner is a read (each node's memory estimate) in a POST
            route = "dashboard"
        elif parts[:1] in (["update"], ["controller"]):
            # OPS-234: the fleet update installs software on every node, a move hands every node
            # to another controller: pairing's rights
            route = "fleet_trust"
        elif (method == "POST" and parts[:1] == ["pair"]) or (method == "DELETE"
                                                              and parts[:1] == ["nodes"]) or \
                (method == "POST" and len(parts) == 4 and parts[2] == "actions"
                 and parts[3] in ("forget", "release-lease")):
            route = "fleet_trust"
        else:
            route = "conductor_write"
        if not self._allowed(route):
            return
        q = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        if parts[:2] == ["app", "parts"] and STATE.get("parts") is not None:
            # OPS-288: Chat and the conductor switched from the page (proxy/appparts.py)
            code, out = STATE["parts"].handle(method, parts, body,
                                              config_api._who(self, STATE["svc"]))
            return self._json(code, out, extra_headers=(("Cache-Control", "no-store"),))
        if parts[:1] == ["chat"]:
            # SRV-841: chatd on this controller's host (`--chat own`, proxy/appchat.py)
            from proxy import appchat
            chat = STATE.get("chat")
            code, out = (chat.handle(method, parts, config_api._who(self, STATE["svc"]))
                         if chat is not None else appchat.handle_off(method, parts)
                         if chat_forward.is_off() else appchat.handle_external(method, parts))
            return self._json(code, out, extra_headers=(("Cache-Control", "no-store"),))
        code, out = STATE["fleet"].handle(method, path, q, body,
                                          who=config_api._who(self, STATE["svc"]))
        if code == 200 and parts in (["app"], ["app", "permissions", "local-network"]):
            out["password"] = self._password_view()
        extra = (("Cache-Control", "no-store"),)
        if code == 200 and parts == ["app", "export"]:
            extra += (("Content-Disposition", 'attachment; filename="tandemapp-fleet-config.json"'),)
        return self._json(code, out, extra_headers=extra)

    # ----------------------------------------------------------------- settings (SRV-845)
    def _settings(self, method: str, path: str, raw: bytes | None) -> None:
        """`/v1/settings/*` (proxy/settings.py): reads under the dashboard's rule; a plan, an
        apply, a restore and a cancel under the configuration's (the admin from another machine),
        from the page itself (a JSON body, not another site or origin)."""
        st = STATE.get("settings")
        if st is None:
            return self._json(404, {"error": {"type": "not_found",
                                              "message": "no settings interface here"}})
        if method != "GET":
            bad = auth_mod.same_origin_json(self.headers)
            if bad is not None:
                return self._json(bad[0], {"error": {"type": bad[1], "message": bad[2]}})
        if not self._allowed("dashboard" if method == "GET" else "conductor_write"):
            return
        body = None
        if raw:
            try:
                body = json.loads(raw)
            except (ValueError, RecursionError) as exc:
                return self._json(400, {"error": {"type": "bad_request",
                                                  "message": f"bad json: {exc}"}})
        parts = [unquote(x) for x in path.rstrip("/").split("/")[3:]]
        q = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        # pairing's rights (a hook's full payload is granted only with them, SEC-16)
        trust = STATE["auth"].decide("fleet_trust", self._peer(), self.headers) == "ok"
        principal = STATE["accounts"].request_principal()
        if principal is not None and principal.via == "token" and parts[:1] in (["auth"],
                                                                                ["api"]):
            # SRV-871: never with a token, not even to read
            return self._json(403, {"error": {"type": "token_refused", "code": "token_refused",
                                              "message": f"the {parts[0]} settings take a "
                                                         f"signed-in account, never an API "
                                                         f"token"}},
                              extra_headers=(("Cache-Control", "no-store"),))
        code, out = st.handle(method, parts, q, body, config_api._who(self, STATE["svc"]),
                              trust=trust, principal=principal)
        return self._json(code, out, extra_headers=(("Cache-Control", "no-store"),))

    # ----------------------------------------------------------------- the old password route
    def _password_view(self) -> dict:
        """The answer's `password` of /v1/fleet/app (6.3b's shape, kept one release, SEC-28):
        `set` means a sign-in is needed, always now; `password` that an owner has one. The
        accounts' passwords change at PUT /v1/auth/password."""
        from proxy import adminpw
        return {"set": True, "password": not STATE["accounts"].store.setup_required(),
                "token": False, "set_at": None, "min_length": adminpw.MIN_LEN,
                "max_length": adminpw.MAX_LEN, "this_machine": False}

    def _app_password(self, method: str, body) -> None:
        """`/v1/fleet/app/password` (kept one release, SEC-28): GET its state under the reads'
        rule; a change is made at PUT /v1/auth/password (404 here)."""
        if method != "GET":
            from proxy import authapi
            return authapi.legacy_password(self)
        if not self._allowed("dashboard"):
            return
        return self._json(200, {"contract_version": fleet_mod.CONTRACT, **self._password_view()},
                          extra_headers=(("Cache-Control", "no-store"),))

    def _fleet_setup(self, method: str, nid: str, sub: list[str], raw: bytes | None) -> None:
        """`/v1/fleet/nodes/{id}/setup/<route>` (proxy/fleetsetup.py, VIS-252): the node's Setup
        Guide through its agent, under the rule of the engine's own /v1/setup; a POST is a
        same-origin JSON request, a write carries its plan's confirm (?confirm=, never passed on).
        A stream (a job's log, Wren's answer) is passed on as it arrives."""
        if not self._allowed("setup"):
            return
        if method not in ("GET", "POST"):
            return self._json(405, {"error": {"type": "method_not_allowed",
                                              "code": "method_not_allowed",
                                              "message": "the Setup Guide takes GET and POST"}})
        if method == "POST":
            bad = auth_mod.same_origin_json(self.headers)
            if bad is not None:
                return self._json(bad[0], {"error": {"type": bad[1], "code": bad[1],
                                                     "message": bad[2]}})
        pairs = parse_qsl(urlsplit(self.path).query, keep_blank_values=True)
        confirm = next((v for k, v in pairs if k == "confirm"), None)
        query = urlencode([(k, v) for k, v in pairs if k != "confirm"])
        headers = {"Accept": self.headers.get("Accept") or "application/json",
                   "Content-Type": self.headers.get("Content-Type"),
                   "Last-Event-ID": self.headers.get("Last-Event-ID")}
        try:
            st, ctype, extra, conn, r = STATE["fleet"].setup.open(
                nid, method, sub, query, raw, headers, confirm,
                config_api._who(self, STATE["svc"]))
        except fleet_mod.FleetError as exc:
            return self._json(exc.status, exc.body(), extra_headers=(("Cache-Control",
                                                                      "no-store"),))
        from proxy import nodeclient
        from proxy.relay import client_gone
        try:
            if "text/event-stream" not in ctype:
                data = r.read()
                conn.close()
                self.send_response(st)
                self.send_header("Content-Type", ctype)
                self.send_header("Cache-Control", "no-store")
                for k, v in extra:
                    self.send_header(k, v)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self.send_response(st)
            self.send_header("Content-Type", ctype)
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.send_header("Connection", "close")
            for k, v in extra:
                self.send_header(k, v)
            self.end_headers()
            self.close_connection = True

            def write(piece: bytes) -> None:
                self.wfile.write(piece)
                self.wfile.flush()
            nodeclient.pipe(r, conn, write, lambda: client_gone(self))
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True
        finally:
            conn.close()

    def _metrics(self) -> None:
        raw = STATE["registry"].render().encode()
        try:
            self.send_response(200)
            self.send_header("Content-Type", metrics_mod.CONTENT_TYPE)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    # ----------------------------------------------------------------- the dashboard's API
    def _dashboard_get(self, path: str) -> None:
        if path == "/v1/dashboard/session":
            return self._session("GET")
        if not self._allowed("dashboard"):
            return
        if path == "/v1/dashboard/metrics":
            return self._metrics()
        if path == "/v1/dashboard/logs/export":
            # the whole ring as one text file (SRV-803). The proxy has no commit of its own and
            # no --log-content flag, so the header says "unknown" for both.
            return logbuf.serve_export(self, logbuf.BUFFER, version=STATE.get("version"),
                                       started=getattr(STATE.get("svc"), "started", None),
                                       log_content=None)
        q = {k: v[-1] for k, v in parse_qs(urlsplit(self.path).query).items()}
        try:
            if path == "/v1/dashboard/live":
                return self._live(q)
            if path == "/v1/dashboard/parallel":
                return self._parallel(q)
            if path == "/v1/dashboard/logs":
                return self._logs(q)
            api = dashboard()
            own, me = self._reader()
            fn = {"/v1/dashboard/summary": api.summary,
                  "/v1/dashboard/usage": lambda q: api.usage(q, own, me),
                  "/v1/dashboard/requests": lambda q: api.requests(q, own, me),
                  "/v1/dashboard/system": lambda _q: api.system()}.get(path)
            if fn is None:
                return self._json(404, {"error": {"type": "not_found",
                                                  "message": f"no route {path}"}})
            return self._json(200, fn(q), extra_headers=(("Cache-Control", "no-store"),))
        except dashboard_api.ApiError as exc:
            return self._json(exc.status, exc.body())

    def _session(self, method: str, body=None, fleet: bool = False) -> None:
        """/v1/dashboard/session and /v1/fleet/session, kept one release over the accounts'
        sessions (SEC-28, proxy/authapi.py legacy_session)."""
        from proxy import authapi
        return authapi.legacy_session(self, STATE["accounts"], method, fleet, body)

    def _sse_head(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream; charset=utf-8")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True

    def _parallel(self, q: dict) -> None:
        """`GET /v1/dashboard/parallel[?since=<t>]`: every request in flight on every backend
        (server/parallel.py)."""
        board = STATE.get("parallel")
        if board is None:
            raise dashboard_api.ApiError(404, "not_found", "the parallel view is off")
        try:
            since = float(q["since"]) if q.get("since") else None
        except ValueError:
            raise dashboard_api.ApiError(400, "bad_request", "since: a number (unix seconds)")
        own, me = self._reader()            # SEC-28: no other account's username without users
        data = parallel_mod.encode(board.snapshot(since, own, me))
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _live(self, q: dict) -> None:
        reg = STATE["live"]
        if q.get("follow", "1") in ("0", "false", "no"):
            return self._json(200, reg.snapshot(), extra_headers=(("Cache-Control", "no-store"),))
        if not reg.subscribe():
            raise dashboard_api.ApiError(429, "too_many",
                                         f"at most {live_mod.MAX_STREAMS} live streams at once")
        try:
            self._sse_head()
            sent, data = reg.first_event()
            self.wfile.write(data)
            self.wfile.flush()
            pinged = time.monotonic()
            while not STATE.get("draining"):
                reg.wait_event(sent, 0.25)
                ev = reg.event()
                if ev is not None and ev[0] > sent:
                    sent = ev[0]
                    self.wfile.write(ev[1])
                    self.wfile.flush()
                    pinged = time.monotonic()
                elif self._reader_gone():
                    break
                elif time.monotonic() - pinged > 15.0:
                    self.wfile.write(b": ping\n\n")
                    self.wfile.flush()
                    pinged = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True
        finally:
            reg.unsubscribe()

    def _logs(self, q: dict) -> None:
        buf = logbuf.BUFFER
        level = q.get("level") or "info"
        if level not in logbuf.LEVELS:
            raise dashboard_api.ApiError(400, "bad_request",
                                         f"level must be one of {', '.join(logbuf.LEVELS)}")
        grep = q.get("grep") or None
        try:
            backlog = int(q.get("backlog") or 500)
            lei = self.headers.get("Last-Event-ID")
            since = q.get("since")
            after = int(lei) if lei else (int(since) if since and since.isdigit() else None)
        except ValueError:
            raise dashboard_api.ApiError(400, "bad_request", "backlog and Last-Event-ID are "
                                                             "integers")
        if q.get("follow", "1") in ("0", "false", "no"):
            return self._json(200, {"contract_version": dashboard_api.LOGS_CONTRACT,
                                    "lines": buf.lines(level=level, after=after, grep=grep,
                                                       limit=backlog), "last_seq": buf.seq},
                              extra_headers=(("Cache-Control", "no-store"),))
        sub = buf.subscribe(level, grep, self._reader_gone)
        if sub is None:
            raise dashboard_api.ApiError(429, "too_many",
                                         f"at most {logbuf.MAX_SUBSCRIBERS} log streams at once")
        try:
            self._sse_head()
            last = after or 0

            def send(entries):
                nonlocal last
                for e in entries:
                    if e["seq"] > last:
                        self.wfile.write(f"event: log\nid: {e['seq']}\ndata: "
                                         f"{json.dumps(e, ensure_ascii=False)}\n\n".encode())
                        last = e["seq"]

            send(buf.lines(level=level, after=after, grep=grep, limit=backlog))
            self.wfile.flush()
            quiet = time.monotonic()
            while not STATE.get("draining"):
                items, dropped = sub.take(1.0)
                if sub.closed or self._reader_gone():
                    break
                if dropped:
                    self.wfile.write(f"event: gap\ndata: {json.dumps({'dropped': dropped})}\n\n"
                                     .encode())
                if items:
                    send(items)
                elif not dropped:
                    if time.monotonic() - quiet < 15.0:
                        continue
                    self.wfile.write(b": ping\n\n")
                self.wfile.flush()
                quiet = time.monotonic()
        except (BrokenPipeError, ConnectionResetError, OSError):
            self.close_connection = True
        finally:
            buf.unsubscribe(sub)


class Server(ThreadingHTTPServer):
    """The standalone server: a thread per request; a peer that resets a keep-alive connection
    between requests is not an error worth a traceback."""

    daemon_threads = True
    # clients that connect in the same moment all get in (the night pass's bench: 64 at once);
    # the default backlog of 5 reset some of them on Linux
    request_queue_size = 128

    def handle_error(self, request, client_address):
        exc = sys.exc_info()[1]
        if isinstance(exc, (ConnectionResetError, BrokenPipeError, TimeoutError)):
            return
        super().handle_error(request, client_address)


def build(args, env=None) -> Service:
    """The service of the standalone server from its arguments (tests call this too)."""
    from proxy import config as config_mod
    from tandemapp import ledger as ledger_mod
    env = os.environ if env is None else env
    Handler.timeout = getattr(args, "idle_timeout", IDLE_TIMEOUT_S) or None
    # OPS-298: addresses and CIDR ranges; an entry that reads as neither is ignored, never a stop
    proxies = auth_mod.TrustedProxies.from_env(env)
    if proxies.bad:
        print(f"[auth] {auth_mod.TRUSTED_PROXY_ENV}: ignored {', '.join(proxies.bad)[:300]!r} "
              f"(neither an address nor a CIDR range)", flush=True)
    # SRV-865, SEC-28: a variable of the old sign-in is a health finding, never a stop; it is
    # read once, to import it (proxy/authlegacy.py), and opens nothing itself
    from proxy import authlegacy
    env_file = getattr(args, "env_file", None)
    # OPS-298: on Kubernetes the old admin token is a mounted file, not a variable
    env, files = authlegacy.token_from_file(env, {v: env_file for v in
                                                  authlegacy.env_file_vars(env_file)})
    env, legacy = authlegacy.scan(env, files)
    cfg = config_mod.ConfigDB(args.config_db, env=env,
                              use_keychain=not getattr(args, "no_keychain", False)).open()
    cfg.apply_env()
    from proxy import ctlmove
    try:
        # a move put the fleet's export here (proxy/ctlmove.py): imported once, before anything
        # reads the registry
        ctlmove.import_at_start(cfg, os.path.dirname(cfg.path),
                                log=lambda m: print(m, flush=True))
    except ctlmove.MoveError as exc:
        print(f"[fleet] the fleet's export was not imported: {exc.message}", flush=True)
    led = None
    if args.usage_ledger and args.usage_ledger != "off":
        ledger_mod.check_config(args.usage_ledger, args.usage_retention_days,
                                test=env.get("QSE_TEST") == "1")
        led = ledger_mod.Ledger(args.usage_ledger, retention_days=args.usage_retention_days).open()
    # SEC-22..SEC-28: the accounts, their sessions, API tokens and one permission table
    # (proxy/accounts.py); there is no other sign-in
    from proxy import accounts as accounts_mod
    a = accounts = accounts_mod.AccountsAuth(
        cfg, trusted_proxies=proxies, log=lambda m: STATE["svc"].log(m), env=env)
    accounts.session_tokens = lambda: STATE.get("session_tokens")
    accounts.legacy = legacy
    accounts.trusted_proxy_file = files.get(auth_mod.TRUSTED_PROXY_ENV)
    # SRV-865: admin.json's password becomes the owner; the admin token alone, the setup code
    did = authlegacy.migrate(accounts, os.path.dirname(cfg.path))
    if did["owner"]:
        print(f"[auth] admin.json's password signs in as the owner account {did['owner']!r}",
              flush=True)
    for f in legacy:
        print(f"[auth] {f['variable']} ({f['file']}): {f['code']}; sign-in and API tokens "
              f"replace it", flush=True)
    registry = metrics_mod.Registry()
    live = live_mod.LiveRegistry(blocks=lambda: None, last_prefill=lambda: None,
                                 engine=_live_engine, hz=live_mod.DEFAULT_HZ, activity=False,
                                 queue_timeout=lambda: 0.0)
    STATE.update(version=version(), ledger=led, auth=a, accounts=accounts, registry=registry,
                 live=live, dashboard_dir=args.dashboard_dir, verbose=args.verbose,
                 args={k: v for k, v in vars(args).items()})
    from proxy import appparts
    state_dir = os.path.dirname(cfg.path)
    cond, base = conductor_of(args, led, env, root=appparts.conductor_root(
        cfg, getattr(args, "conductor_config", "off"), state_dir) or "off")
    svc = Service(cfg, ledger=led, live=live, auth=a, registry=registry,
                  engine_version=STATE["version"], log=lambda m: print(m, flush=True),
                  conductor=cond, monitor_base=base)
    STATE["svc"] = svc
    # SRV-873: an API token's limits, kept in memory and read from the ledger at first sight
    from proxy import tokenlimits
    svc.limits = accounts.token_limits = tokenlimits.TokenLimits(
        accounts.policy, reader=led.reader if led is not None else None)
    # SRV-859: the plugin host reads the conductor's Library of the moment (the page's switch)
    svc.plugin_host.library = lambda: getattr(STATE.get("conductor"), "library", None)
    svc.plugin_host.env = dict(env)
    svc.plugin_host.state_dir = state_dir
    if getattr(args, "port", 0):
        svc.plugin_host.own_url = gateway_url(getattr(args, "host", "127.0.0.1"), args.port)
    # SRV-869: the relay reaches a plugin's backend with this process's own secret
    cfg.service_key = lambda url: accounts.relay_key(url, svc.plugin_host.own_url)
    if env.get("TANDEM_DECISION_REPLAY"):
        # SRV-851: recorded provider answers in place of every provider (tools/decision_replay.py)
        n = svc.providers.load_replay(env["TANDEM_DECISION_REPLAY"])
        print(f"[providers] replaying {n} recorded answers", flush=True)
    STATE["parallel"] = parallel_mod.ParallelBoard(live, cost=svc.live_cost,
                                                   peers=svc.peer_backends,
                                                   auth=svc.relay.bearer_of)
    STATE["fleet"] = fleet_mod.Fleet(svc, nodesource.LocalNode(
        hosts=lambda: (_hosts() or [None])[0], draining=lambda: bool(STATE.get("draining")),
        version=lambda: STATE.get("version", ""), logbuf=logbuf.BUFFER,
        key=lambda bid: svc.cfg.key_for(bid)),
        port=getattr(args, "port", None), bind=getattr(args, "host", "127.0.0.1"),
        log=lambda m: print(m, flush=True), agent=node_agent(env),
        store=fleet_store(args, cfg))
    STATE["fleet"].local_node = env.get("TANDEM_LOCAL_NODE", "on") != "off"
    chat_of(args, cfg, env)
    STATE["parts"] = appparts.Parts(cfg, install_chat=getattr(args, "chat", None) or "external",
                                    install_conductor=getattr(args, "conductor_config", "off"),
                                    state_dir=state_dir, app=AppSide(args, cfg, env, led),
                                    log=lambda m: print(m, flush=True))
    STATE["settings"] = settings_of(cfg, svc, STATE["parts"], state_dir)
    # the sampler: it sleeps while nothing is in flight (the live stream and the parallel view's
    # once-a-second sample; before 0.3.32 the standalone proxy never started it)
    live.start()
    # the engines a paired node reports, discovered as backends (proxy/autodetect.py): an
    # engine on a custom port joins without an env pin, since its node's agent already knows
    # its real port; TANDEM_ENGINE_AUTODETECT=off leaves the backend table exactly as it was
    if env.get("TANDEM_ENGINE_AUTODETECT", "on") != "off":
        from proxy import autodetect
        STATE["autodetect"] = svc.autodetect = autodetect.EngineAutoDetect(
            svc, STATE["fleet"], log=lambda m: print(m, flush=True))
        svc.autodetect.start()
    STATE.pop("dashboard_api", None)
    return svc


def settings_of(cfg, svc, parts, state_dir: str, *, ctl_for=None, backends_of=None, warm=None,
                hot_patch=None, rows=None, poll_s: float | None = None, ext_wheel=None):
    """SRV-845: the settings interface over this controller's stores (proxy/settings.py). The
    keyword arguments replace a part for a test (an engine host's control, its backends, the
    warm-up, the hot route, the chat extension's wheel); each default is the fleet's own."""
    from proxy import hooksettings, pluginsettings
    from proxy import settings as settings_mod
    from proxy import settingscopes, settingsengine

    def fleet():
        return STATE["fleet"]

    def audit(who, action, obj_id, keys, cs, result, detail=None):
        d = {"result": result, **{k: v for k, v in (detail or {}).items() if v is not None}}
        with cfg.change_set(cs):
            cfg.log_audit(who, action, "settings", obj_id, detail=d, fields=keys or None)

    def tandem_backends_on(host_id):
        return [b["id"] for b in cfg.backends()
                if b["kind"] == "tandemllm" and cfg.engine_host_for(b["url"]) == host_id]

    def warm_one(bid, model):
        return fleet().deps.warm(bid, model)

    def http_only():
        return [{"id": f"http:{b['id']}", "label": b.get("label") or b["id"], "backend": b}
                for b in cfg.backends()
                if b["kind"] == "tandemllm" and cfg.engine_host_for(b["url"]) is None]

    def http_read(item):
        try:
            key = cfg.key_for(item["backend"]["id"])
        except Exception:                                            # noqa: BLE001
            key = None
        return settingsengine.http_read(item["backend"], key)

    def patch_hot(row, body):
        from proxy import enginecodes
        try:
            return fleet().engines()._raw(row, "PATCH", "/v1/engine/settings", body)
        except enginecodes.OnboardError as exc:
            raise settings_mod.SettingsError(502, "engine_unreachable",
                                             f"PATCH /v1/engine/settings: {exc.code}") from None

    def conductor_store():
        from proxy import appparts
        from tandemapp.conductor import store as store_mod
        cond = STATE.get("conductor")
        if cond is not None:
            return cond.store, True
        root = appparts.conductor_root(appparts._Fake({appparts.KEYS["conductor"]: "on"}),
                                       parts.install_conductor, state_dir)
        return store_mod.Store(root), False

    def own_wheel():
        # the wheel Add Spark sends a new node (TANDEM_NODE_WHEEL, else built from this copy):
        # it carries tandemapp.engine_ext, the extension chat mode runs
        from proxy import nodeinstall
        return nodeinstall.Package(os.path.join(os.path.dirname(cfg.path), "packages")).wheel()

    ctl_for = ctl_for or (lambda row: fleet().engines().ctl(row))
    restarts = settingsengine.Restarts(
        ctl_for=ctl_for, backends_of=backends_of or tandem_backends_on, warm=warm or warm_one,
        audit=audit, log=lambda m: svc.log(m),
        **({"poll_s": poll_s} if poll_s is not None else {}))
    svc.restart_drain_of = restarts.drained
    scopes = {
        "app": settingscopes.AppScope(cfg, parts),
        "backends": settingscopes.BackendsScope(cfg),
        "conductor": settingscopes.ConductorScope(conductor_store),
        "engine": settingsengine.EngineScope(
            rows=rows or (lambda: fleet().engines()._ssh_rows()), ctl_for=ctl_for,
            restarts=restarts, http_only=http_only, http_read=http_read,
            hot_patch=hot_patch or patch_hot, ext_wheel=ext_wheel or own_wheel),
        # the hook registry's and the decision providers' switches (proxy/hooksettings.py)
        "hooks": settingscopes.HooksScope(cfg, hooksettings.RegistryHooks(
            svc.hooks, svc.providers.store)),
        # each installed plugin's settings, secrets and capability grants (SRV-857)
        "plugins": pluginsettings.PluginsScope(cfg, svc.plugins),
    }
    acc = STATE.get("accounts")
    if acc is not None:
        # SRV-871: the sign-in's and the API's settings
        env_file = (STATE.get("args") or {}).get("env_file")
        scopes["auth"] = settingscopes.AuthScope(cfg, acc, env_file)
        scopes["api"] = settingscopes.ApiScope(cfg, acc, env_file)
    return settings_mod.Settings(cfg, scopes, restarts=restarts,
                                 log=lambda m: print(m, flush=True))


def gateway_url(host: str, port: int) -> str:
    """This controller as a client on its own host reaches it: a wildcard bind on loopback."""
    h = "127.0.0.1" if host in ("0.0.0.0", "", "::") else host
    return f"http://{'[' + h + ']' if ':' in h else h}:{port}"


def chat_of(args, cfg, env):
    """SRV-841: who runs chatd (`--chat`, TANDEM_CHAT). `external` (the default): someone else,
    the forward goes to QSE_CHAT_URL as before. `own`: this controller, on its own host, with its
    own env file (proxy/appchat.py); the forward goes to that chatd. `off` (OPS-288): no chatd,
    every chat route answers 503 chat_off. The page's switch (proxy/appparts.py) wins over the
    install's mode once it was used: on runs an `external` install's forward, else an own chatd."""
    from proxy import appchat, appparts
    mode = getattr(args, "chat", None) or "external"
    if mode not in appchat.MODES:
        raise SystemExit(f"--chat is one of {', '.join(appchat.MODES)}")
    STATE["chat"] = None
    chat_forward.configure()
    on = appparts.chat_on(cfg, mode)
    chat_forward.switch_off(not on)
    if not on or appparts.chat_runner(mode) != "own":
        return None
    return own_chat(args, cfg, env)


def own_chat(args, cfg, env):
    """This controller's chatd (proxy/appchat.py), the forward to it, and the fleet's view of it."""
    from proxy import appchat
    try:
        chat = appchat.AppChat.load(os.path.dirname(cfg.path), env=env,
                                    log=lambda m: print(m, flush=True))
    except ValueError as exc:
        raise SystemExit(f"[chat] {exc}")
    port = getattr(args, "port", 0) or 0
    if port:
        chat.gateway = gateway_url(getattr(args, "host", "127.0.0.1"), port)
    chat_forward.configure(url=chat.url, data=chat.data, key=chat.forward_key)
    # SRV-843: chatd's engine is this gateway; whether the chat engine runs here sizes its
    # sandboxes. SEC-28: chatd's key is its service identity, never a `users` row
    from proxy import chatengine
    svc = STATE.get("svc")
    if svc is not None:
        chat.engine_here = lambda: chatengine.engine_here(svc)
    acc = STATE.get("accounts")
    if acc is not None:
        # SRV-869: chatd's identity svc:chatd, a fresh secret at each start of chatd, into its
        # environment only (no users row, no engine.key)
        from proxy import tokens as tokens_mod
        chat.service_key = lambda: acc.tokens.mint_service(
            tokens_mod.CHATD_SERVICE, tokens_mod.CHATD_SCOPES, memory_user=tokens_mod.CHATD_LABEL)
    STATE["chat"] = chat
    if STATE.get("fleet") is not None:
        STATE["fleet"].chat = chat              # SRV-835: the health page checks it
    return chat


class AppSide:
    """The controller's side of the page's switches (proxy/appparts.py `Parts`)."""

    def __init__(self, args, cfg, env, led):
        self.args, self.cfg, self.env, self.led = args, cfg, env, led
        self.state_dir = os.path.dirname(cfg.path)

    def chat_status(self) -> dict:
        from proxy import appchat
        if (getattr(self.args, "chat", None) or "external") == "external":
            return {"running": None, "url": self.env.get("QSE_CHAT_URL") or appchat.DEFAULT_URL,
                    "data": None, "sandbox": None, "sandbox_path": None}
        chat = STATE.get("chat")
        try:
            c = chat or appchat.AppChat.load(self.state_dir, env=self.env, log=lambda m: None)
        except ValueError:
            return {"running": False, "url": None, "data": None, "sandbox": "none",
                    "sandbox_path": None}
        if (c.cfg.get("QSE_SANDBOX_BACKEND") or "") == "kubernetes":
            sb, path = "kubernetes", None
        else:
            path = c.cfg.get("QSE_SANDBOX_SOCKET") or "/var/run/docker.sock"
            sb = "socket" if os.path.exists(path) else "none"
        return {"running": bool(chat is not None and c.status()["running"]), "url": c.url,
                "data": c.data, "sandbox": sb, "sandbox_path": path}

    def set_chat(self, on: bool, runner: str, who: str) -> str:
        from proxy import appchat
        if runner == "external":
            chat_forward.configure()
            chat_forward.switch_off(not on)
            url = self.env.get("QSE_CHAT_URL") or appchat.DEFAULT_URL
            return f"forwarding to {url}" if on else f"no longer forwarding to {url}"
        if on:
            chat = STATE.get("chat")
            if chat is None:
                from proxy.appparts import PartsError
                try:
                    chat = own_chat(self.args, self.cfg, self.env)
                except SystemExit as exc:
                    raise PartsError(409, "chat_config", f"chatd's env file: {exc}") from None
                port = getattr(self.args, "port", 0) or 0
                if port and not chat.gateway:
                    chat.gateway = gateway_url(getattr(self.args, "host", "127.0.0.1"), port)
            chat_forward.switch_off(False)
            rc, msg = chat.run_action("start")
            chat.start_watcher()
            return f"chatd {msg}" if rc == 0 else \
                f"chatd did not start ({msg}); its watcher tries again"
        chat = STATE.get("chat")
        said = "chatd was not this controller's"
        if chat is not None:
            _, msg = chat.run_action("stop")
            chat.close()
            said = f"chatd {msg}"
        STATE["chat"] = None
        if STATE.get("fleet") is not None:
            STATE["fleet"].chat = None
        chat_forward.configure()
        chat_forward.switch_off(True)
        return said

    def set_conductor(self, root: str | None, who: str) -> str:
        svc = STATE["svc"]
        if svc.harness is not None:
            svc.harness.stop()
            svc.harness = None
        STATE.update(conductor=None, session_tokens=None, laya=None)
        if root is None:
            return "off: every backend is relayed as it is"
        cond, base = conductor_of(self.args, self.led, self.env, root=root)
        from proxy.harness import Harness
        svc.monitor_base = base
        svc.harness = Harness(svc, cond, base, log=svc.log)
        return f"on, its documents in {root}"

    def backends(self) -> list[dict]:
        return [dict(b, env=bool(self.cfg.backend_locks(b["id"]))) for b in self.cfg.backends()]

    def engines(self) -> list[dict]:
        fleet = STATE.get("fleet")
        if fleet is None:
            return []
        return fleet.engines()._ssh_rows()


def fleet_store(args, cfg):
    """The fleet's store (proxy/fleetstore.py): `--fleet-store` beside (fleet.sqlite3 next to the
    config database), a path, or off (memory only)."""
    from proxy import fleetstore
    where = getattr(args, "fleet_store", "off") or "off"
    if where == "beside":
        where = os.path.join(os.path.dirname(cfg.path), "fleet.sqlite3")
    days = getattr(args, "fleet_retention_days", fleetstore.RETENTION_DAYS)
    if days < fleetstore.MIN_RETENTION_DAYS:
        raise SystemExit(f"--fleet-retention-days is at least {fleetstore.MIN_RETENTION_DAYS}")
    return fleetstore.Store(":memory:" if where == "off" else where, retention_days=days,
                            log=lambda m: print(m, flush=True)).open()


def node_agent(env):
    """This machine's TandemNode agent (SRV-725): TANDEM_NODE_AGENT (default
    http://127.0.0.1:7700; `off` for none) with the loopback secret TANDEM_NODE_SECRET (default
    ~/.tandemllm/node/secret). The fleet reads this machine through it while it answers."""
    url = env.get("TANDEM_NODE_AGENT", "http://127.0.0.1:7700")
    if url == "off":
        return None
    from proxy import nodetrust
    path = os.path.expanduser(env.get("TANDEM_NODE_SECRET", "~/.tandemllm/node/secret"))
    return nodesource.AgentNode(url, secret=lambda: nodetrust.load_secret(path),
                                draining=lambda: bool(STATE.get("draining")),
                                logbuf=logbuf.BUFFER)


def conductor_of(args, led, env, root: str | None = None) -> tuple:
    """The standalone proxy's conductor (P4): its documents under `--conductor-config` (or
    `root`: the page's switch, proxy/appparts.py), its journal in the usage ledger's file (as the
    engine's), the session tokens beside the documents, the monitor's policy file
    (`ops/supervisor.yaml`, when PyYAML is there), and Laya when the `decider` role names a Jev
    endpoint. (None, None) with `off`."""
    if root is None:
        root = getattr(args, "conductor_config", "off") or "off"
    STATE.update(conductor=None, session_tokens=None, laya=None)
    if root == "off":
        return None, None
    from tandemapp import session_tokens as st_mod
    from tandemapp.conductor import shipped
    from tandemapp.conductor.decisions import Journal
    from tandemapp.conductor.hook import Conductor
    root = os.path.realpath(os.path.expanduser(root))
    os.makedirs(root, mode=0o700, exist_ok=True)
    if getattr(args, "conductor_defaults", "shipped") == "shipped":
        for line in shipped.install_at(root):
            print(f"[conductor] shipped default stored: {line}", flush=True)
    journal = Journal(led.path if led is not None else None)
    cond = Conductor(root, state={"port": getattr(args, "port", None), "model": "TandemLLM"},
                     journal=journal)
    tokens = st_mod.SessionTokens(os.path.join(root, "session-tokens.json"))
    cond.router.session_tokens = tokens
    from tandemapp.conductor import migrations
    migrations.run(cond.store, cond.journal)
    base = None
    try:
        from tandemapp.conductor.monitor import policy as policy_mod
        base = policy_mod.load_default()
    except Exception as exc:                                       # noqa: BLE001
        print(f"[conductor] the monitor's policy file did not load ({type(exc).__name__}): "
              f"E1, the tool checks and the loop guards stay off", flush=True)
    STATE.update(conductor=cond, session_tokens=tokens, laya=laya_of(cond))
    return cond, base


def laya_of(cond):
    """Laya for `/v1/laya/ask` on the standalone proxy: the `decider` role's Jev endpoint over
    HTTP (another engine's decision models, arbiter), as the engine's own registry answers."""
    from concurrent.futures import Future
    from tandemapp.conductor.classify import JevHttp
    _, roles = cond.store.peek("roles")
    r = roles.get("decider") or {}
    ep = str(r.get("endpoint") or "")
    if r.get("mode", "off") == "off" or not ep.startswith(("http://", "https://")):
        return None
    d = JevHttp(ep, r.get("model") or "auto", int(r.get("timeout_ms") or 400))

    class Registry:
        device = None

        def ask(self, state, questions, model="auto", window="idle", deadline_ms=400,
                source=None):
            f: Future = Future()
            try:
                d.timeout_ms = max(d.timeout_ms, int(deadline_ms))
                f.set_result(d.ask(state, questions))
            except Exception as exc:                               # noqa: BLE001
                f.set_exception(exc)
            return f

        def limits(self, model="auto"):
            return d.limits()

        def ready(self):
            return bool(d.health().get("up"))

        def loaded(self):
            return [d.model]

    return Registry()


def serve(args) -> None:
    logbuf.install()
    svc = build(args)
    srv = Server((args.host, args.port), Handler)
    # SEC-26: the bound address decides local mode; no owner: a setup code in the log
    STATE["accounts"].listen(args.host, srv.server_address[1],
                             say=lambda m: print(m, flush=True))
    svc.plugin_host.own_url = gateway_url(args.host, srv.server_address[1])
    svc.start()
    STATE["fleet"].start()
    led = STATE.get("ledger")
    print(f"[proxy] {version()} on {args.host}:{args.port}, config {svc.cfg.path} "
          f"(schema {svc.cfg.schema_was or 'new'} -> {proxy_config.SCHEMA_VERSION}), {len(svc.cfg.backends())} backends, "
          f"ledger {'off' if led is None else led.path}, {STATE['auth'].describe()}",
          flush=True)
    if svc.cfg.empty():
        print(f"[proxy] no backend yet: open http://{args.host}:{args.port}/dashboard/ for the "
              f"Setup screen, or set TANDEM_BACKENDS", flush=True)
    chat = STATE.get("chat")
    if chat is not None:
        chat.gateway = chat.gateway or gateway_url(args.host, srv.server_address[1])
        print(f"[chat] chatd is this controller's: {chat.url}, data {chat.data}, engine "
              f"{chat.engine_url()}, env file {chat.env_path}", flush=True)
        chat.start_watcher()
    import signal

    def drain(signum, frame):
        STATE["draining"] = True
        threading.Thread(target=srv.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, drain)
    try:
        srv.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        if STATE.get("chat") is not None:
            STATE["chat"].close()                # chatd keeps running: a chat run outlives this
        STATE["fleet"].stop()
        svc.stop()
        if led is not None:
            led.close()
        svc.cfg.close()
