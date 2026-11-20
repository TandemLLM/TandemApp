"""Test helpers: the standalone proxy in this process, in front of fake backends.

    with Proxy(env={...}) as px:          # its own temp dir: config, ledger, key file; no node
                                          # agent unless env names the test's own (SRV-745)
                                          # SEC-28: bound to loopback, a bare process: local mode
                                          # (this machine's requests act as the owner `local`)
                                          # until an owner has a password; local=False: not
                                          # listening yet (as the accounts' own tests run it),
                                          # every route needs a sign-in
        px.post("/v1/chat/completions", {...}) -> (status, headers, body bytes)
        px.rows()                         the ledger's rows, flushed

The fixture keys are fakes; tests grep every file the proxy wrote for them.
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fake_backend  # noqa: E402
import owned  # noqa: E402
import realnode  # noqa: E402
from proxy import app as proxy_app  # noqa: E402

owned.mark()                 # the fake engines and backends a test starts die with it (SRV-732)
realnode.guard()             # no test reaches this machine's own TandemNode (SRV-745)

FIXTURE_KEYS = ("sk-fixture-ABCDEFGH1234", "sk-fixture-ZYXWVUTS9876", "dsk-fixture-QRSTUV5555")


class Proxy:
    def __init__(self, env: dict | None = None, login: str = "off", ledger: bool = True,
                 health_kw: dict | None = None, conductor: bool = False, port: int = 0,
                 local: bool = True):
        self.dir = tempfile.mkdtemp(prefix="px-test-")
        self.env = dict(env or {})
        self.env.setdefault("QSE_TEST", "1")
        # no agent unless the test gives its own: the default is this machine's real one, which a
        # test controller claimed for itself (SRV-745)
        self.env.setdefault("TANDEM_NODE_AGENT", "off")
        # no engine autodiscovery either (proxy/autodetect.py): a test that pairs a fake node
        # with a `tandemllm` engine would otherwise find its backend appear under its feet;
        # tests/test_proxy_autodetect.py drives the module by hand and turns it on
        self.env.setdefault("TANDEM_ENGINE_AUTODETECT", "off")
        self.login = login
        self.ledger = ledger
        self.health_kw = health_kw
        self.conductor = conductor
        self.want_port = port
        self.local = local
        self.logs: list[str] = []

    def __enter__(self) -> "Proxy":
        a = argparse.Namespace(
            config_db=os.path.join(self.dir, "config.sqlite3"),
            usage_ledger=os.path.join(self.dir, "ledger.sqlite3") if self.ledger else "off",
            usage_retention_days=400, dashboard_login=self.login, dashboard_dir=None,
            trust_loopback="on", no_keychain=True, verbose=False, host="127.0.0.1", port=0,
            conductor_config=os.path.join(self.dir, "conductor") if self.conductor else "off",
            conductor_defaults="shipped")
        self.svc = proxy_app.build(a, env=self.env)
        self.svc.log = self.logs.append
        # the rows the service accounted: a relay accounts after the client has read the answer, on
        # the handler's thread, so a test that reads the ledger right after a response raced it
        # (MP1, 2026-10-06: test_proxy_relay failed only under the box suite's 4-way load)
        self.sent, self.accounted = 0, 0
        self._acc = threading.Condition()
        account = self.svc.account

        def counted(rec, _account=account):
            try:
                _account(rec)
            finally:
                with self._acc:
                    self.accounted += 1
                    self._acc.notify_all()
        self.svc.account = counted
        if self.svc.harness is not None:
            self.svc.harness.log = self.logs.append
        if self.health_kw:
            for k, v in self.health_kw.items():
                setattr(self.svc.health, k, v)
        self.srv = proxy_app.Server(("127.0.0.1", self.want_port), proxy_app.Handler)
        self.port = self.srv.server_address[1]
        # SEC-28: what serve() says to the sign-in: where it listens (local mode needs loopback
        # and a bare process; the box's test scope must not count as a container)
        acc = proxy_app.STATE["accounts"]
        acc.local.exists = lambda _p: False
        acc.local.bind = "127.0.0.1" if self.local else None
        threading.Thread(target=self.srv.serve_forever, kwargs={"poll_interval": 0.05},
                         daemon=True).start()
        return self

    def __exit__(self, *exc):
        self.srv.shutdown()
        self.srv.server_close()
        self.svc.stop()
        cond = proxy_app.STATE.get("conductor")
        if cond is not None:
            cond.transcripts.flush(2.0)
            cond.journal.close()
        led = proxy_app.STATE.get("ledger")
        if led is not None:
            led.close()
        self.svc.cfg.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def sync(self) -> None:
        """Every backend checked once (health and models), as the poller would."""
        for b in self.svc.cfg.backends():
            self.svc.health.check(b)

    #: the routes whose every answer is one ledger row
    ROW_ROUTES = ("/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/embeddings",
                  "/v1/fleet/gateway/chat/completions", "/v1/messages",
                  "/v1/messages/count_tokens")

    def request(self, method: str, path: str, body=None, headers=None, timeout=30.0):
        if method == "POST" and path.split("?")[0] in self.ROW_ROUTES:
            self.sent += 1
        c = http.client.HTTPConnection("127.0.0.1", self.port, timeout=timeout)
        data = None if body is None else (body if isinstance(body, bytes)
                                          else json.dumps(body).encode())
        h = {"Content-Type": "application/json"}
        h.update(headers or {})
        c.request(method, path, body=data, headers=h)
        r = c.getresponse()
        out = r.read()
        c.close()
        return r.status, dict(r.getheaders()), out

    def post(self, path, body, headers=None, timeout=30.0):
        return self.request("POST", path, body, headers, timeout)

    def get(self, path, headers=None):
        return self.request("GET", path, None, headers)

    def rows(self, wait_s: float = 10.0) -> list[dict]:
        """The ledger's rows, once every request this helper sent was accounted (at most
        `wait_s`: a request that is never accounted leaves the read as it was before)."""
        with self._acc:
            self._acc.wait_for(lambda: self.accounted >= self.sent, timeout=wait_s)
        led = proxy_app.STATE.get("ledger")
        led.flush(5.0)
        con = sqlite3.connect(led.path)
        con.row_factory = sqlite3.Row
        out = [dict(r) for r in con.execute("SELECT * FROM requests ORDER BY id")]
        con.close()
        return out

    def files(self) -> list[str]:
        out = []
        for d, _, fs in os.walk(self.dir):
            out += [os.path.join(d, f) for f in fs]
        return out


OWNER_PW = "the password of a test account"


def owner(px: "Proxy", name: str = "admin", role: str = "owner") -> dict:
    """SEC-28: what the old admin token was to a test: an account of `role` (made once), signed
    in; its session's headers (the cookie and the CSRF token a write needs). Once an owner has a
    password this machine is no longer trusted without one (local mode ends)."""
    acc = proxy_app.STATE["accounts"]
    if acc.store.by_username(name) is None:
        acc.store.create(name, role, OWNER_PW, actor="test")
    st, h, out = px.post("/v1/auth/session", {"username": name, "password": OWNER_PW})
    assert st == 200, (st, out[:200])
    return {"Cookie": h["Set-Cookie"].split(";", 1)[0], "X-Tandem-CSRF": json.loads(out)["csrf"]}


def api_key(px: "Proxy", label: str = "laptop", *, network: bool = False,
            scopes=("inference:openai", "inference:anthropic")) -> str:
    """SEC-28: a client's key: an API token of the account `label` (an operator, made once), so
    the ledger's user is `label` as a key's label was. An operator is no owner: local mode
    stays. `network`: the APIs are open to other devices (a request with X-Real-IP is one)."""
    import secrets as _secrets
    acc = proxy_app.STATE["accounts"]
    row = acc.store.by_username(label) or acc.store.create(label, "operator", OWNER_PW,
                                                           actor="test")
    if network:
        for proto in ("openai", "anthropic"):
            px.svc.cfg.set_setting(f"api.{proto}.exposure", "network", who="test")
    _view, token = acc.tokens.create(row, label=f"{label}-{_secrets.token_hex(3)}",
                                     scopes=list(scopes), actor="test")
    return token


def fake(**kw):
    return fake_backend.start(**kw)


def run(glb: dict) -> None:
    passed = 0
    t0 = time.time()
    for name, fn in sorted(glb.items()):
        if name.startswith("test_") and callable(fn):
            fn()
            realnode.check()
            print(f"  {name:64s} ok")
            passed += 1
    print(f"{passed} passed in {time.time() - t0:.1f} s")
