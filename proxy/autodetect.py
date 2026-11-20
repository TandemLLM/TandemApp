"""The engines a paired node reports, discovered as controller backends.

THE PROBLEM. The installer writes one engine backend: `local` at :8000. An engine that runs on
another port (TANDEM_ENGINE_PORT, a second engine beside the first) is invisible to the
controller: its dash reads `engine-down:local` until someone pins
`TANDEM_BACKEND_<ID>_URL` in `app.env` — and while such a pin stands, the page's own edits to
the backend's URL are refused as env-set. Nothing looks for the engine.

WHY NOT A PORT SWEEP. The house consent rule (proxy/discovery.py, proxy/addspark.py): nothing
goes to a machine until it has answered a question we were asked to ask — Add Spark asks, the
node answers, the pairing follows. Scanning the LAN's ports for a `/health` would break that for
every box in the house. And it would not be needed: a paired node's agent already knows its
engines' real ports — `engines.view()` (proxy/nodeengines.py) lists each with its port, its
state and its `/health` in short, and the Fleet row already reads engine figures through this
same path (proxy/nodesource.py `AgentNode.agent_engines`). This module consumes that answer.

THE SYNC. Every `SYNC_S`, for every paired node (a `nodes` row whose trust loads an agent), one
`GET /node/engines` through the node's client (proxy/nodeclient.py — the same call the Fleet
row's path makes, read fresh rather than from the 30 s cache so a stopped engine shows at once).
Each entry of kind `tandemllm` whose `/health` answered becomes a backend:

    id    `node-<node id>-<engine id>`, lowercased, 32 chars at most (the backend id's shape)
    url   http://<the node's address>:<the engine's port>/v1
    label "<engine id> on <the node's alias>"
    discovered  {"node": ..., "engine": ...} in the backends row: who owns it

THE RULES.
- A backend the user made (the GUI, the installer, `TANDEM_BACKENDS`/`TANDEM_BACKEND_<ID>_*`) is
  never touched: only rows whose `discovered` column is set are this module's, and an id it
  wants that a manual row holds is left alone. An env pin wins over everything: a field an env
  variable locks is never written (the config refuses it anyway), and a backend an env variable
  declares is never deleted.
- A discovered backend never duplicates a url: if any backend already stands at the engine's
  url, no second one is made for it.
- An engine that stops answering its `/health` (or a node whose agent stops answering) is a
  miss; after `DROP_AFTER` cycles of misses its discovered backend is dropped. A manual backend
  at the same dead url stands forever, as the user left it.
- The default backend (`TANDEM_DEFAULT_BACKEND`, the id a model no backend lists routes to):
  when no enabled manual `tandemllm` backend answers its health check, none is still unpolled,
  and exactly one discovered `tandemllm` backend is up, the default is filled with it — the
  installer's `local` at :8000 is down and the engine on :8020 answers, so the dash and the
  unknown-model route prefer the engine that is actually there. Two discovered engines up is
  ambiguous and fills nothing. A default that names a non-engine backend (a cloud API) is the
  user's own choice and is never overridden. Whenever a manual engine answers again, or the
  configured default does, the default goes back to what `app.env` says.

`TANDEM_ENGINE_AUTODETECT=off` (proxy/app.py, beside `TANDEM_LIVE_RELAY`) leaves the backend
table exactly as it was: nothing is started, nothing is read from any node.
"""

from __future__ import annotations

import hashlib
import re
import threading
import time

from proxy import config as config_mod
from proxy import nodesource

#: how often the sync asks every paired node its agent's engine list
SYNC_S = 15.0
#: cycles a discovered engine must stay silent before its backend is dropped (3 x SYNC_S)
DROP_AFTER = 3
#: the node's answer gets this socket timeout (the nodeclient's own default)
TIMEOUT_S = 5.0
#: who the config's audit names for a discovered backend
WHO = "autodetect"
#: the backend id's shape (the config schema's): at most 32 chars, [a-z0-9_-]
ID_MAX = 32


def backend_id(node_id: str, engine_id: str) -> str:
    """The deterministic id of one node's one engine: `node-<node>-<engine>`, shortened to the
    schema's 32 chars with a hash of the whole when they do not fit — the same inputs, always
    the same id."""
    raw = re.sub(r"[^a-z0-9_-]", "-", f"node-{node_id}-{engine_id}".lower())
    if len(raw) <= ID_MAX:
        return raw
    digest = hashlib.sha256(raw.encode()).hexdigest()[:8]
    keep = re.sub(r"[^a-z0-9_-]", "-", str(engine_id).lower())[:ID_MAX - len(digest) - 1]
    return f"{digest}-{keep}"


def _host(address: str) -> str:
    return f"[{address}]" if ":" in address else address


class EngineAutoDetect:
    """One supervisor thread: every `sync_s` it asks the paired nodes their engines and brings
    the backend table up to what answers (`sync()` does one cycle, in the caller's thread, and
    says what it did). `svc` is read as the fleet changes: its config holds the backends and the
    nodes, its health poller says which of them answer, its fleet hands out the node agents."""

    def __init__(self, svc, fleet, *, sync_s: float = SYNC_S, drop_after: int = DROP_AFTER,
                 log=None):
        self.svc = svc
        self.fleet = fleet
        self.cfg = svc.cfg
        self.sync_s = float(sync_s)
        self.drop_after = max(1, int(drop_after))
        self.log = log or (lambda m: None)
        self._misses: dict[str, int] = {}     # discovered backend id -> cycles without an answer
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------ lifecycle
    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._supervise, name="qse-autodetect",
                                        daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _supervise(self) -> None:
        while not self._stop.wait(self.sync_s):
            try:
                self.sync()
            except Exception as exc:                                   # noqa: BLE001
                self.log(f"[autodetect] cycle failed: {type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------ one cycle
    def sync(self) -> dict:
        """Ask the paired nodes their engines; add, keep and drop discovered backends; fill or
        restore the default. Returns {added, dropped, seen} — the backend ids."""
        cfg = self.cfg
        backends = cfg.backends()
        by_id = {b["id"]: b for b in backends}
        added: list[str] = []
        seen: set[str] = set()
        for row in cfg.nodes():
            if row["endpoint"] == "local" or not (row.get("trust") or {}):
                continue                       # this machine, or a node nobody has paired
            src = self.fleet.source(row)
            if not isinstance(src, nodesource.AgentNode):
                continue                       # not paired, off, or an engine host over SSH
            addr = row.get("address") or src.client.host
            for e in self._agent_engines(src):
                eid, port = e.get("id"), e.get("port")
                if e.get("kind") != "tandemllm" or not isinstance(eid, str) \
                        or not isinstance(port, int) or e.get("health") is None:
                    continue                   # not our kind, or its /health answers nothing
                bid = backend_id(row["id"], eid)
                url = f"http://{_host(addr)}:{port}/v1"
                seen.add(bid)
                cur = by_id.get(bid)
                if cur is None:
                    if any(config_mod.norm_url(b["url"]) == config_mod.norm_url(url)
                           for b in backends):
                        continue               # a backend already stands at this url
                    try:
                        cfg.create_backend({"id": bid, "kind": "tandemllm", "url": url,
                                            "label": f"{eid} on {row['alias']}"[:80],
                                            "discovered": {"node": row["id"], "engine": eid}},
                                           who=WHO)
                        added.append(bid)
                        self.log(f"[autodetect] {bid}: {url} joins as a backend")
                    except config_mod.ConfigError as exc:
                        self.log(f"[autodetect] {bid}: refused ({exc})")
                elif cur.get("discovered"):
                    if config_mod.norm_url(cur["url"]) != config_mod.norm_url(url):
                        try:                   # the engine moved to another port on its node
                            cfg.update_backend(bid, {"url": url}, who=WHO)
                        except config_mod.ConfigError as exc:
                            self.log(f"[autodetect] {bid}: refused ({exc})")
                # else: a manual or env backend holds this id: never touch it
        dropped: list[str] = []
        for b in backends:
            if not b.get("discovered"):
                continue                       # the user's own: this module never drops it
            bid = b["id"]
            if bid in seen:
                self._misses.pop(bid, None)
                continue
            n = self._misses[bid] = self._misses.get(bid, 0) + 1
            if n >= self.drop_after:
                try:
                    cfg.delete_backend(bid, who=WHO)
                    dropped.append(bid)
                    self.log(f"[autodetect] {bid}: silent for {n} cycles, dropped")
                except config_mod.ConfigError as exc:
                    self.log(f"[autodetect] {bid}: not dropped ({exc})")
                self._misses.pop(bid, None)
        self._fill_default()
        return {"added": added, "dropped": dropped, "seen": sorted(seen)}

    # ------------------------------------------------------------------ the pieces
    def _agent_engines(self, src) -> list[dict]:
        """The node's engine list, read now (not from the source's 30 s cache): [] when its
        agent does not answer, so a node that went off counts as silent engines."""
        try:
            st, doc = src.client.json("GET", "/node/engines", timeout=TIMEOUT_S)
        except Exception:                                            # noqa: BLE001
            return []
        if st != 200 or not isinstance(doc, dict):
            return []
        return [e for e in doc.get("engines") or [] if isinstance(e, dict)]

    def _fill_default(self) -> None:
        """The default backend, filled only when the discovered engine is the only one that
        answers (THE RULES); restored to what `app.env` says the moment anything manual is."""
        cfg = self.cfg
        configured = (cfg.env.get("TANDEM_DEFAULT_BACKEND", "") or "").strip() or None
        conf = cfg.backend(configured) if configured else None
        if conf is not None and conf["kind"] != "tandemllm":
            want = configured                  # a cloud default is the user's own choice
        else:
            manual_up = waiting = False
            up: list[str] = []
            for b in cfg.backends():
                if b["kind"] != "tandemllm" or not b["enabled"]:
                    continue
                st = (self.svc.health.state.get(b["id"]) or {}).get("state")
                if b.get("discovered"):
                    if st == "up":
                        up.append(b["id"])
                elif st is None:
                    waiting = True             # the poller has not read it yet: decide later
                elif st == "up":
                    manual_up = True
            want = up[0] if (not manual_up and not waiting and len(up) == 1) else configured
        if cfg.default_backend != want:
            cfg.default_backend = want