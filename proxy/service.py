"""The proxy as one object, shared by its two entry points.

`python -m proxy` (proxy/app.py) serves it alone, without torch and without an engine.
`server/app.py --backends PATH` serves it inside the engine's process: a request whose model routes
to a backend goes to `Service.serve` on the handler's own thread and never touches the engine's
lock, queue or `max_queue`; a request for `TandemLLM` runs as before. One port, one ledger, one
dashboard.

    svc = Service(cfg, ledger=..., live=..., auth=..., reserved=("TandemLLM", raw_name))
    svc.start()                                  the health poller
    svc.authorize(handler) -> (ok, user)         the users' keys on the OpenAI routes
    svc.route(path, body) -> Route | None        None: not a backend's model (the engine's, or 404)
    svc.serve(handler, path, raw, body, route, user)
    svc.models(created) -> [...]                 the backends' part of /v1/models
    svc.model_extras() -> {...}                  beside `data`: a TandemLLM engine's own keys

THE HARNESS (P4, proxy/harness.py). With a conductor (the engine's own in the embedded mode, one
of the proxy's in the standalone mode) every chat completion relayed to a backend whose harness
mode is not `off` runs under it: `serve` asks `Harness.begin` for the request's turn, the relay
asks the turn for the body and passes every event through it, and the turn writes its journal
rows before the ledger row. A backend in `off` is P1's relay, byte for byte.

THE HOOKS (SRV-849, proxy/hooks.py). `serve` asks the hook runner for the request's turn: None
while no hook is enabled, and the relay runs as before, byte for byte. With one, request.received
and route.select run here, before the harness; request.before_engine, the answer's stages and
request.error in the relay; request.finished after the ledger row. A hook's deny is the client's
answer (403 hook_denied, or 503 hook_unavailable for a closed hook that failed).

WHO MAY ASK (SRV-867, SEC-28). The request gate's principal decides (proxy/accounts.py), an API
token's included (proxy/tokens.py). The keys of before (the `users` rows, `TANDEM_API_KEYS`,
`TANDEM_API_USERS_FROM`) are imported once as tokens with the same hash (proxy/authlegacy.py)
and are not read here. The user is the principal's
memory owner: a token's owner's username, an imported key's old label. A token with a model list
answers 403 for any other model (`resolve`), and the ledger row names the token (`token_id`) with
the same client id the bearer's hash gave before. SRV-873: `serve` holds a token to its limits
(proxy/tokenlimits.py) before anything else runs for the request: a refusal is a 429 or a 403
and a ledger row with the token and the reason.
"""

from __future__ import annotations

import datetime as _dt
import json
import threading
import time
import uuid

from proxy import adapters as adapters_mod
from proxy import collect_api as collect_mod
from proxy import config as config_mod
from proxy import engines as engines_mod
from proxy import health as health_mod
from proxy import hooks as hooks_mod
from proxy import pluginhost as pluginhost_mod
from proxy import pluginstore as pluginstore_mod
from proxy import providers as providers_mod
from proxy import http1
from proxy import relay as relay_mod
from proxy import route as route_mod
from proxy.measure import RelayRecord
from tandemapp import ledger as ledger_mod

PROXY_PATHS = ("/v1/chat/completions", "/v1/completions", "/v1/embeddings", "/v1/responses")

MS_BUCKETS = (0.25, 0.5, 1, 2, 3, 5, 10, 20, 50, 100, 250, 1000)
TTFT_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 20, 60, 120)


class Metrics:
    """The proxy's families, added to a `server.metrics.Registry` (the engine's own in the
    embedded mode, so one /metrics page carries both). The engine's families keep their labels:
    a relayed request is never counted in them."""

    def __init__(self, registry, svc: "Service"):
        from tandemapp import metrics as m
        self.svc = svc
        self.requests = registry.add(m.Counter(
            "qse_proxy_requests_total", "requests relayed to a backend, by its id, its kind and "
            "the finish reason", ("backend", "kind", "finish_reason")))
        self.tokens = registry.add(m.Counter(
            "qse_proxy_tokens_total", "tokens a backend reported in `usage`, by type (prompt, "
            "cached, completion, reasoning)", ("backend", "type")))
        self.cost = registry.add(m.Counter(
            "qse_proxy_cost_micros_total", "cost at the model's price, micro-dollars",
            ("backend",)))
        self.retries = registry.add(m.Counter(
            "qse_upstream_retries_total", "tries again before any byte reached the client, by "
            "backend and reason (429, 502, 503, connect)", ("backend", "reason")))
        self.proxy_ms = registry.add(m.Histogram(
            "qse_proxy_overhead_ms", "the proxy's own time per request: arrival to the upstream "
            "send, plus the first frame's delay through the proxy", MS_BUCKETS, ("backend",)))
        self.ttft = registry.add(m.Histogram(
            "qse_proxy_ttft_seconds", "arrival at the proxy to the first token frame (the "
            "network and the backend's queue inside)", TTFT_BUCKETS, ("backend",)))
        registry.add(m.Gauge("qse_backend_up", "1 when the backend's last health check passed",
                             ("backend",), collect=self._up))
        registry.add(m.Gauge("qse_backend_inflight", "requests relayed to the backend now",
                             ("backend",), collect=self._inflight))
        # P2, P3: the vLLM and SGLang pages, mapped (proxy/engines.py)
        registry.add(engines_mod.Families(svc.engines))
        registry.add(m.Gauge("qse_backend_scrape_age_seconds", "seconds since the backend's "
                             "own /metrics page was last read (vLLM, SGLang)", ("backend",),
                             collect=self._scrape_age))
        # P6: the plugins inside vLLM and SGLang (proxy/collect_api.py)
        registry.add(m.Gauge("qse_backend_plugin_age_seconds", "seconds since the plugin inside "
                             "the backend's engine last posted (vLLM, SGLang with "
                             "tandemllm-*-plugin)", ("backend",), collect=self._plugin_age))
        registry.add(m.Gauge("qse_backend_plugin_joins", "requests whose ledger row got the "
                             "engine's own numbers, by how the plugin's item was matched "
                             "(exact, approximate) or why it was not (unmatched, untagged)",
                             ("backend", "join"), collect=self._joins))

    def _up(self):
        return {(bid,): 1.0 if s.get("state") == "up" else 0.0
                for bid, s in self.svc.health.state.items() if s.get("state") != "disabled"}

    def _scrape_age(self):
        out = {}
        for b in self.svc.cfg.backends():
            age = self.svc.engines.scrape_age(b["id"])
            if age is not None:
                out[(b["id"],)] = age
        return out

    def _plugin_age(self):
        out = {}
        for bid in list(self.svc.collect.backends):
            age = self.svc.collect.age(bid)
            if age is not None:
                out[(bid,)] = age
        return out

    def _joins(self):
        out = {}
        for bid, b in list(self.svc.collect.backends.items()):
            c = b.counts
            for join, n in (("exact", c["exact"]), ("approximate", c["approximate"]),
                            ("unmatched", c["row_unmatched"] + c["item_unmatched"]),
                            ("untagged", c["item_untagged"])):
                out[(bid, join)] = float(n)
        return out

    def _inflight(self):
        return {(b["id"],): float(self.svc.relay.inflight.count(b["id"]))
                for b in self.svc.cfg.backends()}

    def record(self, rec: RelayRecord) -> None:
        b = rec.backend or "unknown"
        self.requests.inc(backend=b, kind=rec.backend_kind or "unknown",
                          finish_reason=rec.finish_reason or "unknown")
        for t, v in (("prompt", rec.prompt_tokens), ("cached", rec.cached_tokens),
                     ("completion", rec.completion_tokens), ("reasoning", rec.reasoning_tokens)):
            if v:
                self.tokens.inc(float(v), backend=b, type=t)
        if rec.cost_micros:
            self.cost.inc(float(rec.cost_micros), backend=b)
        if rec.proxy_ms is not None:
            self.proxy_ms.observe(rec.proxy_ms, backend=b)
        if rec.ttft_ms is not None:
            self.ttft.observe(rec.ttft_ms / 1e3, backend=b)


class Service:
    def __init__(self, cfg: "config_mod.ConfigDB", *, ledger=None, live=None, auth=None,
                 reserved=(), registry=None, log=None, engine_version: str = "",
                 code_sha: str = "", health_kw: dict | None = None,
                 engines_kw: dict | None = None, conductor=None, monitor_base=None):
        self.cfg = cfg
        self.ledger = ledger
        self.live = live
        self.auth = auth
        self.log = log or (lambda msg: print(msg, flush=True))
        self.engine_version, self.code_sha = engine_version, code_sha
        self.table = route_mod.Table(cfg, reserved)
        self.relay = relay_mod.Relay(self)
        self.health = health_mod.Health(self, **(health_kw or {}))
        self.table.upstream_card = self._engine_card
        self.engines = engines_mod.Watch(self, **(engines_kw or {}))
        self.collect = collect_mod.Collector(self)
        self._adapters: dict = {}
        self._lock = threading.Lock()
        self.metrics = Metrics(registry, self) if registry is not None else None
        self.started = time.time()
        self.harness = None
        # the backend's drained node, or None (TandemApp's fleet sets it, proxy/fleetops.py)
        self.drain_of = lambda bid: None
        # the backend's engine host while a settings change restarts it (SRV-845,
        # proxy/settingsengine.py), or None: the gateway answers as for a drained node
        self.restart_drain_of = lambda bid: None
        # SRV-731 (proxy/deployments.py, set by TandemApp's fleet): the route of a deployment's
        # model (None: the table routes it), the model list with each deployment once, and the
        # node and deployment of a backend for the answer's headers and the ledger row
        self.place = None
        self.listing = None
        self.locate = lambda bid, model: None
        # SRV-749: told of every accounted request (the fleet's backend members' figures)
        self.observe = None
        # SRV-741: a cache route's (status, answer, headers) on the member that holds the
        # conversation; None: no fleet, the route is not served here
        self.cache_route = None
        # the monitor's policy: its stall watchdog's ntfy settings carry the push of a new model
        self.monitor_base = monitor_base
        self.notifier = None                   # notify.send; a test puts its own here
        # SRV-873: an API token's limits (proxy/tokenlimits.py)
        self.limits = None
        # the engines' live figures for the live view (proxy/liverelay.py); app.build starts it
        self.liverelay = None
        # SRV-849: the remote hooks around the relay (proxy/hooks.py); idle while none is enabled
        self.hooks = hooks_mod.Runner(cfg, log=lambda m: self.log(m))
        if registry is not None:
            self.hooks.families(registry)
        # SRV-851: the decision providers the controller's conductor asks through (proxy/
        # providers.py); idle while none is enabled
        self.providers = providers_mod.Providers(cfg, runner=self.hooks,
                                                 log=lambda m: self.log(m))
        # SRV-857: the plugin registry (proxy/pluginstore.py)
        self.plugins = pluginstore_mod.PluginStore(cfg, hooks=self.hooks.store,
                                                   log=lambda m: self.log(m))
        # SRV-859: the plugin host (proxy/pluginhost.py): the enabled plugins' processes and
        # their contributions; idle with none enabled (started by `start`)
        self.plugin_host = pluginhost_mod.PluginHost(
            self.plugins, runner=self.hooks, providers=self.providers, cfg=cfg,
            log=lambda m: self.log(m), health_check=lambda b: self.health.check(b))
        if conductor is not None:
            from proxy.harness import Harness
            self.harness = Harness(self, conductor, monitor_base, log=self.log)

    def start(self) -> "Service":
        self.health.start()
        self.engines.start()
        self.collect.start()
        self.plugin_host.start()
        return self

    def stop(self) -> None:
        if getattr(self, "plugin_host", None) is not None:
            self.plugin_host.close()
        if self.harness is not None:
            self.harness.stop()
        if self.hooks is not None:
            self.hooks.stop()
        if getattr(self, "providers", None) is not None:
            self.providers.stop()
        relay = getattr(self, "liverelay", None)
        if relay is not None:
            relay.stop()
        self.health.stop()
        self.engines.stop()
        self.collect.stop()
        self.cfg.flush_seen(force=True)

    # ----------------------------------------------------------------- pieces the relay asks for
    def adapter(self, backend: dict):
        key = (backend["id"], backend["updated_at"], backend["kind"])
        with self._lock:
            a = self._adapters.get(key)
            if a is None:
                a = adapters_mod.make(backend, log=self.log)
                self._adapters = {k: v for k, v in self._adapters.items() if k[0] != key[0]}
                self._adapters[key] = a
            return a

    def price_of(self, bid: str, model: str | None) -> dict | None:
        if not model:
            return None
        rows = self.table.snapshot()["by_backend"].get(bid) or {}
        m = rows.get(model)
        if m is None:
            for r in rows.values():
                if r["public_id"] == model:
                    m = r
                    break
        return m["price"] if m is not None else None

    def live_cost(self, rec, tokens: int) -> int | None:
        """The parallel view's cost so far of a relayed request in flight: `tokens` completion
        tokens at its model's price (the hour's, for DeepSeek); the prompt's part comes with the
        usage at the end."""
        b = self.table.backend_for(rec.backend) if rec.backend else None
        if b is None:
            return None
        price = self.price_of(rec.backend, rec.backend_model or rec.public_model)
        if not price:
            return None
        return self.adapter(b).price({"prompt_tokens": 0, "completion_tokens": int(tokens),
                                      "cached_tokens": 0}, price, rec.ts)

    def new_models(self, bid: str, ids: list[str]) -> None:
        """A discovery found model ids the backend never listed (config.sync_models stored them
        disabled): one log line each, and one push through the box's ntfy topic (the stall
        watchdog's url, token file and CA in the monitor's policy) when it has one. A new id is
        new once: its row stays, so it is never reported again."""
        b = self.table.backend_for(bid) or self.cfg.backend(bid) or {}
        who = b.get("label") or bid
        for mid in ids:
            self.log(f"[proxy] {bid}: new model {mid} in /models, disabled until it is turned "
                     f"on under Backends")
        cfg = None
        try:
            task = self.monitor_base.tasks.get("S4") if self.monitor_base is not None else None
            cfg = dict(task["notify"]) if task is not None else None
        except Exception:                                          # noqa: BLE001
            cfg = None
        if not cfg or not (cfg.get("url") and cfg.get("token_file")):
            return
        if self.notifier is None:
            from tandemapp.conductor.monitor import notify
            self.notifier = notify.send
        cfg = dict(cfg, tags="new")
        said = self.notifier(cfg, "TandemLLM: new model",
                             f"{who}: new model {', '.join(ids)} in /models (disabled; turn it on "
                             f"under Backends)")
        self.log(f"[proxy] {bid}: push of the new model: {said}")

    def peer_backends(self) -> list[tuple[str, str]]:
        """The TandemLLM backends, `(id, url)`: the parallel view reads their own lanes."""
        return [(b["id"], b["url"]) for b in self.table.snapshot()["backends"].values()
                if b.get("kind") == "tandemllm"]

    def count_retry(self, bid: str, reason: str) -> None:
        if self.metrics is not None:
            self.metrics.retries.inc(backend=bid, reason=reason)

    # ----------------------------------------------------------------- the OpenAI routes
    def authorize(self, handler, headers: tuple = ()) -> tuple[bool, str | None]:
        """(may it go on, the user's label). On False a 401 is already written, with `headers`
        beside its own. SEC-22..SEC-28: the gate's principal decides (proxy/accounts.py): a
        session, local mode, or an API token checked there and stripped from the request. A
        service with no sign-in (`auth` None: a test of the relay alone) lets everything in."""
        if self.auth is None:
            return True, None
        from proxy import authz
        p = self.principal(handler)
        if p is not None and (authz.allow(p, "inference", self.auth.policy)
                              or authz.allow(p, "chat-engine", self.auth.policy)):
            # local mode is this machine as before the accounts: no user, so its memory and its
            # rows stay the default owner's (`owner`), as they were with no key
            return True, (None if p.kind == "local" else p.name)
        body = (b'{"error": {"message": "an API token is needed (Authorization: Bearer '
                b'<token>); `tandem-app tokens create` makes one", "type": '
                b'"invalid_request_error", "code": "invalid_api_key"}}')
        try:
            handler.send_response(401)
            handler.send_header("Content-Type", "application/json")
            handler.send_header("WWW-Authenticate", 'Bearer realm="TandemLLM"')
            for k, v in headers:
                handler.send_header(k, v)
            handler.send_header("Content-Length", str(len(body)))
            handler.end_headers()
            handler.wfile.write(body)
        except OSError:
            pass
        return False, None

    def principal(self, handler=None):
        """SRV-867: the request's principal (the gate's); None without a sign-in. A handler that
        rebuilt the request (the Messages API's) finds it through the gate's thread."""
        if self.auth is None:
            return None
        p = getattr(handler, "_principal", None) if handler is not None else None
        if p is None and hasattr(self.auth, "request_principal"):
            p = self.auth.request_principal()
        return p

    def check_model(self, model) -> None:
        """SRV-867: a token with a model list takes only those models (403 naming the model)."""
        p = self.principal()
        if p is None or getattr(p, "via", None) != "token":
            return
        from proxy import deployments
        from proxy import tokens as tokens_mod
        if not tokens_mod.model_allowed(p.models, model):
            raise deployments.Refused(403, "permission_denied",
                                      f"this token may not use the model {model!r}")

    def _token(self):
        p = self.principal()
        if p is None or getattr(p, "via", None) != "token":
            return None
        if not getattr(p, "models", ()) and not getattr(p, "backends", ()):
            return None
        return p

    def restrict(self, route):
        """SRV-889: the route a token may run: its backend and fallbacks cut to the token's
        backends and models (proxy/tokens.py may_run). A deployment's route keeps its member or is
        refused; a table route moves to its first allowed fallback. 403 when nothing is left."""
        p = self._token()
        if p is None or route is None:
            return route
        from proxy import deployments
        from proxy import tokens as tokens_mod
        ok = lambda bid, mid: tokens_mod.may_run(p.models, p.backends, route.public, bid, mid)  # noqa: E731
        allowed = [(b, m) for b, m in [(route.backend, route.model), *route.fallbacks] if ok(b, m)]
        if not allowed or (route.deployment is not None and not ok(route.backend, route.model)):
            what = (f"the backend {route.backend!r}" if p.backends and route.backend not in p.backends
                    else f"the model {route.public!r}")
            raise deployments.Refused(403, "permission_denied", f"this token may not use {what}")
        route.backend, route.model = allowed[0]
        route.fallbacks = allowed[1:]
        return route

    def visible_models(self, created: int) -> list[dict]:
        """SRV-889: /v1/models as the request's token sees it: only what it may run."""
        cards = self.models(created)
        p = self._token()
        if p is None:
            return cards
        from proxy import tokens as tokens_mod
        # an engine's own card names the engine in owned_by: the table's rows name the backend
        where = {card.get("id"): (bid, up) for bid, up, card in self.table.listing_rows(created)}
        out = []
        for c in cards:
            name = c.get("id", "")
            bid, up = where.get(name, (None, None))
            if bid is None:
                try:
                    r = self.table.resolve(name)
                    bid, up = r.backend, r.model
                except Exception:                                  # noqa: BLE001
                    t = c.get("tandem") if isinstance(c.get("tandem"), dict) else {}
                    bid, up = t.get("backend") or c.get("owned_by") or "", t.get("upstream_id") or name
            if tokens_mod.may_run(p.models, p.backends, name, bid, up):
                out.append(c)
        return out

    def visible_card(self, name: str, created: int) -> dict | None:
        card = self.card(name, created)
        if card is None or self._token() is None:
            return card
        return card if any(c.get("id") == name for c in self.visible_models(created)) else None

    def resolve(self, body: dict, headers=None, check_token: bool = True):
        """The route of a request at the standalone proxy: a fleet deployment's (its members),
        else the table's. route.NotFound, or proxy.deployments.Refused for a deployment that
        serves nothing now, and (SRV-867) for a model the request's token may not use; a hook's
        own choice of model later (`check_token` False) is not the client's."""
        if check_token:
            self.check_model(body.get("model"))
        model = body.get("model") or ""
        if self.place is not None:
            r = self.place(model, body, headers)
            if r is not None:
                return self.restrict(r) if check_token else r
        r = self.table.resolve(model)
        return self.restrict(r) if check_token else r

    def route(self, path: str, body: dict):
        """The route of a request, or None when no backend's model is asked for. NotFound for a
        model the table knows is ambiguous."""
        if path not in PROXY_PATHS or not isinstance(body, dict):
            return None
        model = body.get("model")
        if not isinstance(model, str) or not model:
            return None
        try:
            return self.table.resolve(model)
        except route_mod.NotFound as exc:
            if "served by" in exc.message:
                raise
            return None

    def serve(self, handler, path: str, raw: bytes, body: dict, route, user: str | None,
              t_arrival: float | None = None, endpoint: str | None = None,
              client: tuple | None = None, native: bool = False) -> RelayRecord:
        """`endpoint` and `client` (SRV-879): the ledger's endpoint and (client id, kind) of a
        request that came in another protocol (the Messages API: `messages`, its key's hash),
        when the path and the handler's headers would say otherwise. `native` (SRV-881): the
        request passes through in the Messages API's shape (proxy/passthrough.py)."""
        rid = ("chatcmpl-px-" if path.endswith("chat/completions") else "px-") + \
            uuid.uuid4().hex[:24]
        rec = RelayRecord(rid, endpoint or relay_mod.endpoint_of(path),
                          bool(body.get("stream")) and path != "/v1/embeddings",
                          t_arrival=t_arrival)
        # the inbound protocol: the ledger's `api` column (schema 13, `messages` is `anthropic`)
        rec.api = "messages" if endpoint == "messages" else "openai"
        rec.native = native
        rec.model = route.public
        rec.model_served = route.public
        rec.user = user
        self._caller(rec, handler, body, client)
        rec.sock = getattr(handler, "connection", None)
        try:                        # the session a client names (opencode's x-session-id)
            sess = handler.headers.get("x-session-id") or handler.headers.get("X-Session-Id")
        except Exception:                                          # noqa: BLE001
            sess = None
        rec.conv = sess[:128] if isinstance(sess, str) and sess else None
        mt = body.get("max_tokens", body.get("max_completion_tokens",
                                             body.get("max_output_tokens")))
        rec.max_tokens = mt if isinstance(mt, int) and not isinstance(mt, bool) else None
        rec.backend = route.backend
        b = self.table.backend_for(route.backend)
        rec.backend_kind = b["kind"] if b else None
        rec.hx = None
        rec.hk = None
        deny = None
        ticket = self._admit(handler, route, rec)
        if ticket is False:
            return rec
        if self.hooks is not None:
            try:
                rec.hk = self.hooks.turn(self._hook_ctx(handler, path, route, rec, b))
            except Exception as exc:                               # noqa: BLE001
                self.log(f"[hooks] turn failed, relayed as is: {type(exc).__name__}: {exc}")
                rec.hk = None
            if rec.hk is not None and native:
                from proxy import passthrough
                rec.hk.native = passthrough.HookView
            if rec.hk is not None:
                try:
                    body, raw, route, deny = self._hooks_front(handler, path, raw, body, route,
                                                               rec)
                except Exception as exc:                           # noqa: BLE001
                    self.log(f"[hooks] the request's stages failed: {type(exc).__name__}: {exc}")
        if deny is None and self.harness is not None:
            try:
                rec.hx = self.harness.begin(handler, path, body, route, rec)
            except Exception as exc:                               # noqa: BLE001
                self.log(f"[harness] begin failed, relayed as is: {type(exc).__name__}: {exc}")
                rec.hx = None
        if self.live is not None:
            try:
                self.live.register(rec)
            except Exception:                                      # noqa: BLE001
                pass
        handler._px_written = False
        try:
            if deny is not None:
                self.refuse(handler, rec, deny)
            else:
                self.relay.serve(handler, path, raw, body, route, rec)
            if deny is None and getattr(route, "on_served", None) is not None:
                route.on_served(rec)                 # a Replicas member keeps the conversation
        except Exception as exc:                                   # noqa: BLE001
            rec.finish_reason = rec.finish_reason or "error"
            rec.error_type = rec.error_type or type(exc).__name__
            self.log(f"[proxy] relay failed: {type(exc).__name__}: {exc}")
            if rec.status in (None, 200) and rec.t_sent is None:
                rec.status = 500
            self._answer_failure(handler, rec, exc)
        finally:
            rec.end()
            if ticket is not None:
                if rec.finish_reason == "limit" and rec.cost_micros is None and ticket.live:
                    # a stream its spend limit cut has no usage: its row keeps the live cost, so
                    # the spend is still there after a restart (an estimate, named here)
                    rec.cost_micros = ticket.live
                try:
                    self.limits.done(ticket, (rec.prompt_tokens or 0) + (rec.completion_tokens
                                                                          or 0),
                                     rec.cost_micros)
                except Exception as exc:                           # noqa: BLE001
                    self.log(f"[limits] {type(exc).__name__}: {exc}")
            if self.live is not None:
                try:
                    self.live.finish(rec)
                except Exception:                                  # noqa: BLE001
                    pass
            if rec.hx is not None:
                try:
                    rec.hx.finish()
                except Exception as exc:                           # noqa: BLE001
                    import traceback
                    self.log(f"[harness] finish failed: {type(exc).__name__}: {exc} "
                             + traceback.format_exc()[-800:])
            self.account(rec)
        return rec

    # ----------------------------------------------------------------- limits (SRV-873)
    def _admit(self, handler, route, rec):
        """An API token's limits (proxy/tokenlimits.py): its Ticket, None when no limit is
        kept for the caller, or False when a limit refused the request (answered here in the
        OpenAI shape, which the Messages API's handler turns into its own, and accounted)."""
        if self.limits is None:
            return None
        p = self.principal(handler)
        if p is None or getattr(p, "kind", None) != "token":
            return None
        try:
            priced = self.price_of(route.backend, route.model) is not None or \
                self.price_of(route.backend, route.public) is not None
            got = self.limits.admit(p, priced=priced)
        except Exception as exc:                                   # noqa: BLE001
            self.log(f"[limits] the check failed, the request goes on: "
                     f"{type(exc).__name__}: {exc}")
            return None
        if not hasattr(got, "status"):
            if self.limits.watched(got):
                rec.watch = lambda: self._watch(got, rec)
            return got
        rec.finish_reason = "refused"
        rec.error_type = got.code
        self.relay._write_plain(handler, got.status, list(got.headers()),
                                json.dumps(got.body()).encode(), rec)
        rec.end()
        self.account(rec)
        return False

    def _watch(self, ticket, rec):
        """A watched stream's check (relay._pump, every few frames): a Refusal ends it."""
        try:
            return self.limits.watch(ticket, self.live_cost(rec, rec.frames))
        except Exception:                                          # noqa: BLE001
            return None

    # ----------------------------------------------------------------- the hooks (SRV-849)
    def _hook_ctx(self, handler, path: str, route, rec, backend) -> dict:
        try:
            prof = handler.headers.get("X-Tandem-Profile")
        except Exception:                                          # noqa: BLE001
            prof = None
        if not prof and backend:
            prof = (backend.get("harness") or {}).get("profile")
        return {"request_id": rec.request_id, "endpoint": relay_mod.endpoint_of(path),
                "model": route.public, "upstream_model": route.model, "backend": route.backend,
                "profile": prof or None, "user": rec.user or "owner", "session": rec.conv,
                "client_kind": rec.client_kind, "stream": bool(rec.stream),
                # hooks-v1 1.1 (SRV-881): the client's protocol and the backend's
                "protocol": {"inbound": getattr(rec, "api", None) or "openai",
                             "upstream": _protocol_of(backend)}}

    def _hooks_front(self, handler, path, raw, body, route, rec):
        """request.received and route.select: (body, raw, route, a deny or None)."""
        hk = rec.hk
        asked = body.get("model")
        body, raw, deny = hk.received(body, raw)
        if deny is not None:
            return body, raw, route, deny
        if body.get("model") != asked:
            try:
                route = self.resolve(body, getattr(handler, "headers", None), check_token=False)
            except route_mod.NotFound as exc:
                return body, raw, route, {"hook": None, "status": 404,
                                          "code": "model_not_found", "message": exc.message}
            except Exception as exc:                               # noqa: BLE001 (Refused)
                return body, raw, route, {"hook": None,
                                          "status": int(getattr(exc, "status", 503)),
                                          "code": getattr(exc, "code", "refused"),
                                          "message": str(exc)}
        decision, deny = hk.select(route, [b["id"] for b in self.cfg.backends() if b["enabled"]])
        if deny is not None:
            return body, raw, route, deny
        if decision:
            route = self._decided(handler, route, decision)
        if route.backend != rec.backend or route.public != rec.model:
            rec.model = rec.model_served = route.public
            rec.backend = route.backend
            b = self.table.backend_for(route.backend)
            rec.backend_kind = b["kind"] if b else None
        hk.ctx.update(model=route.public, upstream_model=route.model, backend=route.backend,
                      protocol=dict(hk.ctx.get("protocol") or {}, upstream=_protocol_of(
                          self.table.backend_for(route.backend))))
        if getattr(rec, "native", False) and not self.all_native(route):
            # SRV-881: a request that passes through goes to a Messages API backend only
            return body, raw, route, {"hook": None,
                                      "status": 409, "code": "protocol_native",
                                      "message": f"a hook moved the request to "
                                                 f"'{route.backend}', which does not speak the "
                                                 f"Messages API; a request that passes through "
                                                 f"stays on Messages API backends"}
        return body, raw, route, None

    def all_native(self, route) -> bool:
        """SRV-881: every candidate of `route` is a backend a Messages API request passes
        through to (proxy/passthrough.py)."""
        from proxy import passthrough
        from proxy.messages import MESSAGES
        for bid, _ in route.candidates():
            b = self.table.backend_for(bid)
            if b is None or not passthrough.native(self.adapter(b), MESSAGES):
                return False
        return True

    def _decided(self, handler, route, d: dict):
        """route.select's decision as a route: a backend (and its model), a model, a profile (the
        X-Tandem-Profile the harness and our engine read). A backend or model the table does not
        have leaves the table's route, with a log line."""
        if d.get("profile"):
            try:
                del handler.headers["X-Tandem-Profile"]
                handler.headers["X-Tandem-Profile"] = d["profile"]
            except Exception:                                      # noqa: BLE001
                pass
        bid, model = d.get("backend"), d.get("model")
        if bid:
            if self.table.backend_for(bid) is None:
                self.log(f"[hooks] route.select chose the backend {bid!r}, which does not "
                         f"serve; the table's route stands")
                return route
            got = None
            try:
                got = self.table.resolve(f"{bid}/{model or route.public}", default=False)
            except route_mod.NotFound:
                got = None
            if got is not None and got.backend == bid:
                got.public = route.public
                return got
            return route_mod.Route(bid, model or (route.model if bid == route.backend
                                                  else route.public), route.public,
                                   [c for c in route.candidates() if c[0] != bid])
        if model:
            try:
                got = self.table.resolve(model)
            except route_mod.NotFound:
                self.log(f"[hooks] route.select chose the model {model!r}, which no backend "
                         f"serves; the table's route stands")
                return route
            got.public = route.public
            return got
        return route

    def refuse(self, handler, rec, deny: dict) -> None:
        """A hook's refusal as the client's answer (an OpenAI error)."""
        st = int(deny.get("status") or 403)
        rec.finish_reason = "refused"
        rec.error_type = deny.get("code") or "hook_denied"
        hdrs = [("Content-Type", "application/json")]
        if deny.get("hook"):
            hdrs.append(("X-Tandem-Hook", deny["hook"]))
        data = json.dumps({"error": {"message": deny.get("message") or "refused by a hook",
                                     "type": "server_busy" if st == 503 else
                                     "invalid_request_error",
                                     "code": rec.error_type}}).encode()
        self.relay._write_plain(handler, st, hdrs, data, rec)

    @staticmethod
    def _answer_failure(handler, rec, exc) -> None:
        """A relay that failed in the proxy's own code still answers: a 500 when nothing went
        out, an error frame and [DONE] when a stream had begun (the client never waits on a
        socket nobody writes to)."""
        msg = {"error": {"message": f"the proxy failed: {type(exc).__name__}",
                         "type": "internal_error"}}
        try:
            if not getattr(handler, "_px_written", False):
                data = json.dumps(msg).encode()
                handler.send_response(500)
                handler.send_header("Content-Type", "application/json")
                handler.send_header("Content-Length", str(len(data)))
                handler.end_headers()
                handler.wfile.write(data)
                handler._px_written = True
            elif rec.stream and getattr(rec, "native", False):
                # SRV-881: a Messages stream that passes through ends with an `error` event
                handler.wfile.write(("event: error\ndata: " + json.dumps(
                    {"type": "error", "error": {"type": "api_error",
                                                "message": msg["error"]["message"]}})
                    + "\n\n").encode())
                handler.wfile.flush()
                handler.close_connection = True
            elif rec.stream:
                handler.wfile.write(("data: " + json.dumps(msg) + "\n\ndata: [DONE]\n\n")
                                    .encode())
                handler.wfile.flush()
                handler.close_connection = True
        except OSError:
            handler.close_connection = True

    def account(self, rec) -> None:
        """One ledger row and the proxy's metrics. Never fails a request."""
        if getattr(rec, "thinking_dropped", 0) and rec.error_type is None:
            # SRV-881: earlier turns went without their thinking (the stash had lost them)
            rec.error_type = "thinking_evicted"
        try:
            row = rec.row(self.engine_version, self.code_sha)
        except Exception as exc:                                   # noqa: BLE001
            self.log(f"[proxy] row failed: {type(exc).__name__}: {exc}")
            return
        if self.metrics is not None:
            try:
                self.metrics.record(rec)
            except Exception:                                      # noqa: BLE001
                pass
        try:
            self.collect.on_row(rec, row)                          # P6: the engine's numbers
        except Exception as exc:                                   # noqa: BLE001
            self.log(f"[collect] join failed: {type(exc).__name__}: {exc}")
        if self.ledger is not None:
            self.ledger.submit(row)
        hk = getattr(rec, "hk", None)
        if hk is not None:
            try:
                hk.finished(rec)                    # SRV-849: request.finished, off the path
            except Exception:                                      # noqa: BLE001
                pass
        if self.observe is not None:
            try:
                self.observe(rec)
            except Exception:                                      # noqa: BLE001
                pass
        self.log(f"[proxy] {rec.request_id} {rec.backend}/{rec.backend_model} "
                 f"status={rec.status} finish={rec.finish_reason} "
                 f"tokens={rec.prompt_tokens}/{rec.completion_tokens} "
                 f"ttft_ms={_f(rec.ttft_ms)} decode_tps={_f(rec.decode_tps)} "
                 f"proxy_ms={_f(rec.proxy_ms)} connect_ms={_f(getattr(rec, 'connect_ms', None))} "
                 f"retries={rec.retries}"
                 + (f" fallback_from={rec.fallback_from}" if rec.fallback_from else ""))

    def account_refusal(self, handler, path: str, body, status: int, user=None,
                        finish="error", error_type=None, model=None) -> None:
        """A ledger row for a request answered before any backend (a 401, a 404 model)."""
        rec = RelayRecord("px-" + uuid.uuid4().hex[:24], relay_mod.endpoint_of(path),
                          bool(isinstance(body, dict) and body.get("stream")))
        rec.model = model or (body.get("model") if isinstance(body, dict) else "") or ""
        rec.status, rec.finish_reason, rec.error_type, rec.user = status, finish, error_type, user
        from proxy import tokens as tokens_mod
        rec.api = "messages" if tokens_mod.anthropic(path) else "openai"
        self._caller(rec, handler, body if isinstance(body, dict) else None)
        rec.end()
        self.account(rec)

    def _caller(self, rec, handler, body, client: tuple | None = None) -> None:
        """The row's client id and kind; with an API token (SRV-867) its id, and the client id
        its hash gave before the gate stripped it."""
        rec.client_id, rec.client_kind = client or ledger_mod.client_of(handler.headers, body)
        p = self.principal(handler)
        if p is not None and getattr(p, "via", None) == "token":
            rec.token_id, rec.client_id = p.id, p.client or rec.client_id

    def models(self, created: int) -> list[dict]:
        if self.listing is not None:
            return self.listing(created)
        return self.table.listing(created)

    def card(self, name: str, created: int) -> dict | None:
        if self.listing is not None:
            got = next((m for m in self.listing(created) if m["id"] == name), None)
            if got is not None:
                return got
        return self.table.card(name, created)

    # ----------------------------------------------------------------- our engine's own list
    def _engine_card(self, bid: str, model: str) -> dict | None:
        """SRV-733: a TandemLLM backend's own card of `model`, from its last /models answer."""
        doc = self.health.docs.get(bid)
        for m in (doc or {}).get("data") or []:
            if isinstance(m, dict) and m.get("id") == model:
                return m
        return None

    def model_extras(self) -> dict:
        """SRV-733: what the TandemLLM backends' lists hold beside `data` (the engine's
        `decision_models`), each item once by id, for /v1/models to carry as the engine does."""
        out: dict = {}
        for b in self.cfg.backends():
            if not b["enabled"]:
                continue
            for k, v in (self.health.docs.get(b["id"]) or {}).items():
                if k in ("object", "data") or not isinstance(v, list):
                    continue
                have = {x.get("id") for x in out.get(k, []) if isinstance(x, dict)}
                out.setdefault(k, []).extend(x for x in v if not (isinstance(x, dict)
                                                                  and x.get("id") in have))
        return out

    def default_card(self, name: str) -> tuple[int, bytes] | None:
        """SRV-733: `GET /v1/models/{name}` of the default backend (TANDEM_DEFAULT_BACKEND) when
        it is a TandemLLM engine, for an id this proxy does not list: the engine's own answer
        (its raw model's alias, a decision model, its 404). None without such a backend."""
        bid = self.cfg.default_backend
        b = self.table.backend_for(bid) if bid else None
        if b is None or b["kind"] != "tandemllm":
            return None
        from urllib.parse import quote
        adapter = self.adapter(b)
        pool = http1.Pool(b["url"], float(b["timeouts"]["connect"]))
        conn = None
        try:
            conn = pool.get()
            conn.send("GET", pool.path("/v1/models/" + quote(name, safe="")),
                      [h for h in adapter.headers(self.cfg.key_for(bid))
                       if h[0] != "Content-Type"], b"")
            status, _, _ = conn.read_head(health_mod.ANSWER_TIMEOUT_S)
            return status, conn.read_all(health_mod.ANSWER_TIMEOUT_S, limit=1024 * 1024)
        except Exception:                                          # noqa: BLE001
            return None
        finally:
            if conn is not None:
                conn.close()
            pool.close()

    # ----------------------------------------------------------------- views
    def cost_today(self, tz=None) -> dict:
        """backend id -> micro-dollars since local midnight, from the ledger (None without one)."""
        if self.ledger is None:
            return {}
        now = _dt.datetime.now().astimezone() if tz is None else _dt.datetime.now(tz)
        midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
        try:
            rows = self.ledger.reader().execute(
                "SELECT backend, SUM(cost_micros), COUNT(cost_micros) FROM requests WHERE "
                "ts_ms >= ? AND backend IS NOT NULL GROUP BY backend",
                (int(midnight.timestamp() * 1000),)).fetchall()
        except Exception:                                          # noqa: BLE001
            return {}
        return {b: (int(s) if n else None) for b, s, n in rows}

    def backends_view(self) -> list[dict]:
        """One row per backend for the summary, the system snapshot and the Backends screen."""
        cost = self.cost_today()
        out = []
        for b in self.cfg.backends():
            h = self.health.view(b["id"])
            n_models = sum(1 for m in self.cfg.models(b["id"]) if m["present"] and m["enabled"])
            out.append({"id": b["id"], "kind": b["kind"], "label": b["label"],
                        "enabled": b["enabled"], "cloud": b["cloud"],
                        "state": h["state"] if b["enabled"] else "disabled",
                        "since": _iso(h["since"]), "last_error": h["last_error"],
                        "latency_ms": h["latency_ms"], "in_flight": self.relay.inflight.count(
                            b["id"]), "max_inflight": b["max_inflight"], "models": n_models,
                        "harness_mode": b["harness"]["mode"],
                        "cost_today_micros": cost.get(b["id"]),
                        "scrape_age_s": self.engines.scrape_age(b["id"]),
                        "plugin_age_s": self.collect.age(b["id"]),
                        "engine": self._engine_block(b)})
        return out


    def _engine_block(self, b: dict) -> dict | None:
        """The backend's `engine` block: L1 from its /metrics page, and the plugin's `plugin`
        block beside it (P6); `layer` says L2 while the plugin posts."""
        e = self.engines.view(b["id"], b["kind"])
        if e is None:
            return None
        p = self.collect.view(b["id"])
        e = dict(e)
        e["plugin"] = p
        e["layer"] = "L2" if p is not None and p["state"] == "l2" else "L1"
        return e


def _protocol_of(backend) -> str:
    """hooks-v1 1.1: the protocol a backend speaks (`messages` for kind anthropic)."""
    return "messages" if backend and backend.get("kind") == "anthropic" else "openai"


def _f(x):
    return "-" if x is None else f"{x:.1f}"


def _iso(ts):
    if ts is None:
        return None
    return _dt.datetime.fromtimestamp(ts, _dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
