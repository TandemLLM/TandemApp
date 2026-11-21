"""SRV-845, SRV-846 (D1): one settings interface over the existing stores (proxy/settings.py,
proxy/settingscopes.py, proxy/settingsengine.py), against the real stores and a fake engine host:
the config database, the page's switches, a conductor document folder, `tandem remote` faked with
its overlay model (tests/fake_tandem_remote.py `sim`) and a hook adapter. Every answer is held to
contracts/settings-v1 (tools/contract_check.py).

  Gherkin (SRV-845) and the acceptance criteria:
  1  Plan then apply: Given a change set with a backend key and an engine setting, When it is
     planned, Then the plan lists each scope, its restart class and what will change; When it is
     applied, Then the scopes apply in dependency order and the audit holds one change-set id
                                                        test_plan_then_apply_key_and_engine
  2  Failure in the middle: Given a change set whose second scope fails, When it is applied, Then
     the first scope stays applied, the answer lists what was applied and what failed, the rest
     is not run and untouched, and the restore offered brings the first back
                                                        test_failure_in_the_middle
  3  No secret in the audit: Given a change set that sets an API key, Then the audit row holds the
     field name and no key value; nor does any answer, version, log line or the engine's secret
     keys' values                                       test_no_secret_anywhere
  4  Each of the six scopes answers schema, get, plan, apply, versions and restore with the same
     shapes; each key carries its restart class and the plan shows it (the plugins scope with
     one installed plugin, SRV-857)
                                                        test_every_scope_six_operations
  5  Versions and restore work for the engine and the conductor scopes
                                                        test_engine_and_conductor_restore
  6  Confirm tokens: none 428, stale 409, a fingerprint key's second confirm (428, wrong 409),
     the same confirm again answers the same result, a second change set meanwhile is busy, a
     change that holds already applies as unchanged    test_confirm_tokens
  7  Restart scheduling with a fake engine: the backends drain while the engine's job waits for
     idle with a countdown, restarting, one warm-up token, back; a failed start reports the
     engine's restore; a deadline that passes is a timeout with nothing changed; cancel while it
     waits replaces the engine's job, cancel while it acts is too late; audit rows
                                                        test_restart_scheduling
  8  The hooks scope: a stub with an empty schema until the registry plugs its adapter in, then
     its keys plan, apply, version and restore         test_hooks_adapter
  9  The app scope goes through the page's switches; the backends scope refuses what the
     environment sets, bad values and unknown keys; bad bodies and routes
                                                        test_app_backends_refusals
 10  The settings-v1 files: every file parses, every $ref resolves, every route's schema exists,
     every error type the code raises is listed with its status, the constants agree
                                                        test_contract_files

  Gherkin (SRV-845 D1b, the engine's mode as a key):
 11  Given an engine host in engine mode without the chat extension, When chat mode is planned,
     Then the plan names this controller's wheel and its install, the change set's id and the
     restart class engine; When it is applied, Then the wheel is installed (its sha256 checked)
     before `mode set chat --ext tandemapp.engine_ext`, the restart drains, warms up and ends, and
     the audit has the change set's rows. Planned again it holds; with the extension there no
     install is planned; with no wheel here or a foreign module the plan refuses with the fix;
     with other keys of the same engine the mode goes into one overlay change; QSE_MODE itself and
     mode.ext with the bare engine are refused on their key
                                                        test_engine_mode
 12  A plan that restarts an engine says how long the restart itself takes: the default before
     anything is known, the engine's own last start when its status reports one, what this
     controller measured once it followed a restart; a hot change and a store's change say none
                                                        test_restart_estimate
 13  A refusal while a part is planned names its scope and target besides the key, so the page
     marks that field: the same key on two engine hosts, a backend's value, a conductor document,
     a hook's check that names no key itself (a part of one key names it)
                                                        test_refusal_names_its_place
 14  Unsetting a key that is unset already is no change, in every scope: the plan drops it (a set
     beside it still plans), applying it is unchanged; the backends say set or unset as the
     database holds it
                                                        test_unset_of_an_unset_key

CPU only. Run: python tests/test_settings.py
"""

from __future__ import annotations

import glob
import json
import os
import re
import sqlite3
import sys
import tempfile
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fake_tandem_remote as ftr  # noqa: E402
from proxy import appparts, enginectl, pluginsettings, pluginstore  # noqa: E402
from proxy import config as config_mod  # noqa: E402
from proxy import settings as settings_mod  # noqa: E402
from proxy import settingscopes, settingsengine  # noqa: E402
from tandemapp.conductor import store as store_mod  # noqa: E402
from tools import contract_check  # noqa: E402

CON = os.path.join(ROOT, "contracts", "settings-v1")
SECRET = "sk-" + "Zq7" * 12
OLD_KEY = "sk-" + "Old9" * 9


def ok(doc, name: str):
    errs = contract_check.validate(doc, contract_check.load(name, CON), root=CON)
    assert not errs, (name, errs[:5], json.dumps(doc, default=str)[:600])
    return doc


def until(fn, s: float = 10.0):
    end = time.time() + s
    while time.time() < end:
        got = fn()
        if got:
            return got
        time.sleep(0.02)
    return None


class FakeApp:
    def __init__(self):
        self.calls = []

    def chat_status(self):
        return {"running": False, "url": None, "data": "/x/chat", "sandbox": "none",
                "sandbox_path": None}

    def set_chat(self, on, runner, who):
        self.calls.append(("chat", on, who))
        return "done"

    def set_conductor(self, root, who):
        self.calls.append(("conductor", root, who))
        return "done"

    def backends(self):
        return []

    def engines(self):
        return []


class FakeHooks(settingscopes.HooksAdapter):
    """A registry with two settings: one hook's mode and its webhook's secret."""

    def __init__(self, fail_write: bool = False):
        self.vals = {"hooks.audit.mode": "off"}
        self.secret = None
        self.fail_write = fail_write
        self.writes = 0

    def keys(self):
        return [settings_mod.key_spec("hooks.audit.mode", "enum", values=("off", "shadow", "on"),
                                      default="off"),
                settings_mod.key_spec("hooks.audit.token", "string", secret=True, nullable=True)]

    def values(self):
        return dict(self.vals)

    def is_set(self, key):
        return self.secret is not None

    def check(self, sets, unsets):
        bad = [k for k, v in sets.items()
               if k.endswith(".mode") and v not in ("off", "shadow", "on")]
        return [f"{bad[0]} is off, shadow or on"] if bad else []

    def write(self, sets, unsets, who):
        if self.fail_write:
            raise RuntimeError("the registry's table is locked")
        self.writes += 1
        for k, v in sets.items():
            if k.endswith(".token"):
                self.secret = v
            else:
                self.vals[k] = v
        for k in unsets:
            if k.endswith(".token"):
                self.secret = None


class Rig:
    """A controller's stores in a temporary folder, one engine host `h1` faked."""

    def __init__(self, label: str, sim: dict | None = None, hooks=None, warm=None, wheel=True):
        self.dir = tempfile.mkdtemp(prefix=f"settings-{label}-")
        self.wheel = os.path.join(self.dir, "pkg", "tandemapp-0.15.0-py3-none-any.whl")
        os.makedirs(os.path.dirname(self.wheel))
        with open(self.wheel, "wb") as fh:
            fh.write(b"PK\x03\x04 a wheel of this controller's own copy")
        self.wheel_ok = wheel
        self.cfg = config_mod.ConfigDB(os.path.join(self.dir, "config.sqlite3"), env={},
                                       use_keychain=False).open()
        self.cfg.create_backend({"id": "eng", "kind": "tandemllm",
                                 "url": "http://198.51.100.7:8000/v1", "label": "Engine",
                                 "max_inflight": None},   # said no limit: the field holds nothing
                                who="t")
        self.cfg.create_backend({"id": "cloud", "kind": "openai", "key": OLD_KEY}, who="t")
        self.user, self.user_key = self.cfg.create_user({"label": "laptop"}, who="t")
        self.app = FakeApp()
        self.parts = appparts.Parts(self.cfg, install_chat="off", install_conductor="off",
                                    state_dir=self.dir, app=self.app, log=lambda m: None)
        self.store = store_mod.Store(os.path.join(self.dir, "conductor"))
        self.eng = ftr.install(os.path.join(self.dir, "engine"))
        ftr.set_state(self.eng, sim=True, **(sim or {}))
        self.row = {"id": "h1", "alias": "box"}
        self.logs: list[str] = []
        self.warms: list = []
        self.ctl_for = lambda row: enginectl.LocalControl(self.eng, label=row["id"])
        self.restarts = settingsengine.Restarts(
            ctl_for=self.ctl_for, backends_of=lambda h: ["eng"],
            warm=warm or (lambda b, m: (self.warms.append((b, m)) or (True, None))),
            log=self.logs.append, poll_s=0.02, warm_wait_s=0.5)
        self.hooks = hooks
        # SRV-857: the plugin registry with the example tool plugin installed
        self.plugins = pluginstore.PluginStore(self.cfg)
        self.plugins.install({"type": "folder", "path": os.path.join(
            ROOT, "contracts", "plugin-v1", "examples", "hello-tool")}, who="t")
        self.scopes = {
            "app": settingscopes.AppScope(self.cfg, self.parts),
            "backends": settingscopes.BackendsScope(self.cfg),
            "conductor": settingscopes.ConductorScope(lambda: (self.store, True)),
            "engine": settingsengine.EngineScope(rows=lambda: [self.row], ctl_for=self.ctl_for,
                                                 restarts=self.restarts,
                                                 ext_wheel=self.own_wheel),
            "hooks": settingscopes.HooksScope(self.cfg, hooks),
            "plugins": pluginsettings.PluginsScope(self.cfg, self.plugins),
        }
        self.s = settings_mod.Settings(self.cfg, self.scopes, restarts=self.restarts,
                                       log=self.logs.append)
        self.restarts.audit = self.s._audit

    def own_wheel(self) -> str:
        if not self.wheel_ok:
            raise RuntimeError("this controller cannot build its wheel (no checkout); set "
                               "TANDEM_NODE_WHEEL to a tandemapp wheel")
        return self.wheel

    def call(self, method: str, path: str, body=None, q=None, who: str = "admin"):
        parts = [p for p in path.strip("/").split("/") if p]
        return self.s.handle(method, parts, q or {}, body, who)

    def plan(self, changes):
        st, p = self.call("POST", "plan", {"changes": changes})
        assert st == 200, (st, p)
        return ok(p, "plan")

    def apply(self, changes, plan=None, **kw):
        plan = plan or self.plan(changes)
        body = {"changes": changes, "confirm": plan["confirm"], **kw}
        if plan.get("confirm_fingerprint") and "confirm_fingerprint" not in kw:
            body["confirm_fingerprint"] = plan["confirm_fingerprint"]
        st, r = self.call("POST", "apply", body)
        assert st == 200, (st, r)
        return ok(r, "result")

    def wait_restart(self, rid: str, s: float = 15.0) -> dict:
        v = until(lambda: (lambda x: x if x["state"] in settingsengine.FINAL else None)(
            self.restarts.view(rid)), s)
        assert v, self.restarts.view(rid)
        return ok(v, "restart")

    def audit_rows(self) -> list[dict]:
        con = sqlite3.connect(self.cfg.path)
        con.row_factory = sqlite3.Row
        rows = [dict(r) for r in con.execute("SELECT * FROM audit ORDER BY id")]
        con.close()
        return rows

    def close(self):
        self.cfg.close()


# ---------------------------------------------------------------------------- 1 plan then apply

def test_plan_then_apply_key_and_engine():
    r = Rig("gherkin1")
    try:
        changes = [{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 32}},
                   {"scope": "backends", "set": {"backends.cloud.key": SECRET,
                                                 "backends.cloud.label": "Cloud"}}]
        p = r.plan(changes)
        assert [x["scope"] for x in p["parts"]] == ["backends", "engine"], "dependency order"
        b, e = p["parts"]
        assert b["restart"] == "none" and e["restart"] == "engine" and e["restart_needed"]
        assert p["restart"] == "engine" and p["restart_needed"] and p["needs_confirm"]
        assert {c["key"] for c in b["changes"]} == {"backends.cloud.key", "backends.cloud.label"}
        key = next(c for c in b["changes"] if c["key"] == "backends.cloud.key")
        assert key["secret"] and key["from"] is None and key["to"] is None and key["is_set"]
        assert e["changes"] == [{"key": "MAX_QUEUE", "from": "16", "to": "32", "was_set": True,
                                 "is_set": True, "secret": False, "restart": "engine",
                                 "fingerprint": False}], e
        assert r.cfg.key_for("cloud") == OLD_KEY, "a plan writes nothing"
        res = r.apply(changes, p)
        assert res["state"] == "applied" and res["change_set"] == p["change_set"], res
        assert [(x["scope"], x["state"]) for x in res["parts"]] == [("backends", "applied"),
                                                                     ("engine", "scheduled")]
        assert r.cfg.key_for("cloud") == SECRET and r.cfg.backend("cloud")["label"] == "Cloud"
        rs = res["parts"][1]["restart"]
        assert rs["backends"] == ["eng"] and rs["change_set"] == p["change_set"]
        done = r.wait_restart(rs["id"])
        assert done["state"] == "done" and done["warm"]["ok"] and not done["draining"], done
        assert r.warms == [("eng", "fake-model")], r.warms
        st, vals = r.call("GET", "engine", q={"target": "h1"})
        assert st == 200 and ok(vals, "values")["version"] == "1", vals
        assert next(v for v in vals["values"] if v["key"] == "MAX_QUEUE") == \
            {"key": "MAX_QUEUE", "value": "32", "set": True, "source": "managed"}
        # the audit: one change-set id over the store's own rows and the settings' rows
        cs = p["change_set"]
        rows = [x for x in r.audit_rows() if x["change_set"] == cs]
        objs = {(x["object"], x["action"]) for x in rows}
        assert ("backend", "update") in objs and ("backend_key", "update") in objs, objs
        assert ("settings", "settings.apply") in objs and \
            ("settings", "settings.change_set") in objs and \
            ("settings", "settings.restart") in objs, objs
        assert len({x["change_set"] for x in rows}) == 1
        st, aud = r.call("GET", "audit", q={"change_set": cs})
        assert st == 200 and ok(aud, "audit")["rows"], aud
        assert {x["change_set"] for x in aud["rows"]} == {cs}
        parts_rows = [x for x in aud["rows"] if x["action"] == "settings.apply"]
        assert [(x["scope"], x["target"]) for x in reversed(parts_rows)] == \
            [("backends", "controller"), ("engine", "h1")], "applied in dependency order"
    finally:
        r.close()
    return "plan names scopes and restart classes; applied in order; one change-set id"


# ---------------------------------------------------------------------------- 2 failure midway

def test_failure_in_the_middle():
    hooks = FakeHooks()
    r = Rig("gherkin2", hooks=hooks)
    try:
        # the engine refuses the apply on its host (an internal error after the plan)
        changes = [{"scope": "backends", "set": {"backends.eng.max_inflight": 8,
                                                 "users.%d.enabled" % r.user["id"]: False}},
                   {"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 24}},
                   {"scope": "hooks", "set": {"hooks.audit.mode": "shadow"}}]
        p = r.plan(changes)
        ftr.set_state(r.eng, script={"settings apply": {"exit": 5, "stdout": json.dumps({
            "ctl": "engine-ctl-v1", "version": "1.1", "verb": "settings apply", "ok": False,
            "error": {"code": "internal", "message": "the overlay could not be written"}}) + "\n"}})
        res = r.apply(changes, p)
        assert res["state"] == "partial", res
        assert res["applied"] == ["backends/controller"] and res["failed"] == "engine/h1" and \
            res["not_run"] == ["hooks/controller"], res
        b, e, h = res["parts"]
        assert b["state"] == "applied" and b["restore"] and b["version_before"], b
        assert e["state"] == "failed" and e["error"]["type"] == "engine_failed", e
        assert h["state"] == "not_run" and h["error"] is None
        assert r.cfg.backend("eng")["max_inflight"] == 8, "the first scope stays applied"
        assert hooks.writes == 0 and hooks.vals["hooks.audit.mode"] == "off", "the rest untouched"
        assert ftr._sim(ftr.state(r.eng))["current"] == 0, "the engine's overlay untouched"
        rows = [x for x in r.audit_rows() if x["change_set"] == p["change_set"]
                and x["object"] == "settings"]
        res_by = {(x["action"], x["object_id"]): json.loads(x["detail"])["result"] for x in rows}
        assert res_by[("settings.apply", "backends/controller")] == "applied"
        assert res_by[("settings.apply", "engine/h1")] == "failed"
        assert res_by[("settings.change_set", None)] == "partial", res_by
        # the restore offered brings the first part back
        rest = b["restore"][0]
        st, rp = r.call("POST", "backends/restore", {"version": rest["version"]})
        assert st == 200 and ok(rp, "plan")["changed"], rp
        assert {c["key"] for c in rp["parts"][0]["changes"]} == \
            {"backends.eng.max_inflight", f"users.{r.user['id']}.enabled"}, rp
        st, rr = r.call("POST", "backends/restore", {"version": rest["version"],
                                                     "confirm": rp["confirm"]})
        assert st == 200 and ok(rr, "result")["state"] == "applied", rr
        assert r.cfg.backend("eng")["max_inflight"] is None
        assert next(u for u in r.cfg.users() if u["id"] == r.user["id"])["enabled"] is True
        # planning the same set again: what applied is unchanged, the engine plans again
        ftr.set_state(r.eng, script={})
        st, again = r.call("POST", "plan", {"changes": changes[1:]})
        assert st == 200 and all(x["changed"] for x in again["parts"]), again
        res2 = r.apply(changes[1:], again)
        assert res2["state"] == "applied" and hooks.writes == 1
        r.wait_restart(res2["parts"][0]["restart"]["id"])
    finally:
        r.close()
    return "first applied, second failed, third not run; restore offered and works"


# ---------------------------------------------------------------------------- 3 secrets

def test_no_secret_anywhere():
    hooks = FakeHooks()
    r = Rig("gherkin3", hooks=hooks)
    hook_secret = "whsec_" + "Pp4" * 10
    try:
        changes = [{"scope": "backends", "set": {"backends.cloud.key": SECRET}},
                   {"scope": "hooks", "set": {"hooks.audit.token": hook_secret}}]
        p = r.plan(changes)
        res = r.apply(changes, p)
        assert res["state"] == "applied"
        answers = [p, res]
        for scope, q in (("app", {}), ("backends", {}), ("conductor", {}), ("hooks", {}),
                         ("engine", {"target": "h1"})):
            for sub in ("", "/schema", "/versions"):
                st, doc = r.call("GET", scope + sub, q=q)
                assert st == 200, (scope, sub, doc)
                answers.append(doc)
        st, aud = r.call("GET", "audit")
        answers.append(aud)
        st, ov = r.call("GET", "")
        answers.append(ok(ov, "overview"))
        blob = json.dumps(answers)
        for s in (SECRET, OLD_KEY, hook_secret, ftr.SIM_SECRET):
            assert s not in blob, s[:6]
        # the key's row says set, nothing more
        st, vals = r.call("GET", "backends")
        k = next(v for v in vals["values"] if v["key"] == "backends.cloud.key")
        assert k["value"] is None and k["set"] is True, k
        hv = next(v for v in r.call("GET", "hooks")[1]["values"] if v["key"] == "hooks.audit.token")
        assert hv == {"key": "hooks.audit.token", "value": None, "set": True, "source": "hooks"}
        # the engine's secret-looking keys: never read into an answer
        st, ev = r.call("GET", "engine", q={"target": "h1"})
        hf = next(v for v in ev["values"] if v["key"] == "HF_TOKEN")
        assert hf["value"] is None and hf["set"] is True
        # the tables: audit (every column), the versions; the logs
        con = sqlite3.connect(r.cfg.path)
        dump = "\n".join(con.iterdump())
        con.close()
        for s in (SECRET, hook_secret):
            assert s not in dump.replace(r.cfg.path, ""), "a secret in the config database's text"
        rows = [x for x in r.audit_rows() if x["change_set"] == p["change_set"]]
        key_rows = [x for x in rows if "backends.cloud.key" in (x["fields"] or "")]
        assert key_rows, rows
        assert all(SECRET not in json.dumps(x) for x in rows)
        assert SECRET not in "\n".join(r.logs) and hook_secret not in "\n".join(r.logs)
        # a version never keeps a secret's value
        for v in r.cfg.settings_versions("backends", "controller"):
            assert "backends.cloud.key" not in v["doc"], v
    finally:
        r.close()
    return "the audit names the key; no answer, row, version or log holds a value"


# ---------------------------------------------------------------------------- 4 six operations

def test_every_scope_six_operations():
    hooks = FakeHooks()
    r = Rig("six", hooks=hooks)
    try:
        st, ov = r.call("GET", "")
        # the auth and api scopes exist with TANDEM_AUTH=accounts only (SRV-871)
        assert st == 200 and ok(ov, "overview")["order"] == [
            x for x in settings_mod.SCOPES if x not in ("auth", "api")], ov
        cases = {
            "app": ({}, {"conductor": "on"}),
            "backends": ({}, {"backends.eng.enabled": False}),
            "conductor": ({}, None),
            "engine": ({"target": "h1"}, {"MAX_QUEUE": 20}),
            "hooks": ({}, {"hooks.audit.mode": "on"}),
            "plugins": ({"target": "hello-tool"}, {"settings.greeting": "Hi"}),
        }
        _, sdoc = r.store.get("settings")
        sdoc["journal"]["retention_days"] = 14
        cases["conductor"] = ({}, {"settings": sdoc})
        for scope, (q, sets) in cases.items():
            st, sch = r.call("GET", f"{scope}/schema", q=q)
            assert st == 200, (scope, sch)
            ok(sch, "schema")
            assert sch["keys"] and all(k["restart"] in settings_mod.RESTART for k in sch["keys"])
            st, vals = r.call("GET", scope, q=q)
            assert st == 200, (scope, vals)
            ok(vals, "values")
            body = {"set": sets, **q}
            st, p = r.call("POST", f"{scope}/plan", body)
            assert st == 200 and ok(p, "plan")["changed"], (scope, p)
            want = {s["key"]: s["restart"] for s in sch["keys"]}
            for c in p["parts"][0]["changes"]:
                assert c["restart"] == want[c["key"]], (scope, c, want.get(c["key"]))
            st, res = r.call("POST", f"{scope}/apply", dict(body, confirm=p["confirm"]))
            assert st == 200 and ok(res, "result")["state"] == "applied", (scope, res)
            part = res["parts"][0]
            if part["restart"]:
                r.wait_restart(part["restart"]["id"])
            st, vers = r.call("GET", f"{scope}/versions", q=q)
            assert st == 200 and ok(vers, "versions")["versions"], (scope, vers)
            before = part["restore"][0]["version"]
            st, rp = r.call("POST", f"{scope}/restore", {"version": before, **q})
            assert st == 200 and ok(rp, "plan")["changed"], (scope, rp)
            st, rr = r.call("POST", f"{scope}/restore", {"version": before,
                                                         "confirm": rp["confirm"], **q})
            assert st == 200 and ok(rr, "result")["state"] == "applied", (scope, rr)
            if rr["parts"][0]["restart"]:
                r.wait_restart(rr["parts"][0]["restart"]["id"])
            st, again = r.call("GET", scope, q=q)
            assert [v["value"] for v in again["values"]] == [v["value"] for v in vals["values"]], \
                (scope, again, vals)
        assert ("conductor", os.path.join(r.dir, "conductor"), "admin") in r.app.calls
    finally:
        r.close()
    return "app, backends, conductor, engine, hooks, plugins: schema get plan apply versions " \
        "restore"


# ---------------------------------------------------------------------------- 5 engine, conductor

def test_engine_and_conductor_restore():
    r = Rig("restore")
    try:
        # the engine: two versions, then back to the first
        for n in (20, 28):
            res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": n}}])
            r.wait_restart(res["parts"][0]["restart"]["id"])
        st, vers = r.call("GET", "engine/versions", q={"target": "h1"})
        assert [v["id"] for v in ok(vers, "versions")["versions"]] == ["2", "1", "0"], vers
        assert vers["current"] == "2" and vers["versions"][0]["good"] is True
        assert vers["versions"][1]["values"] == {"MAX_QUEUE": "20"}
        st, rp = r.call("POST", "engine/restore", {"target": "h1", "version": "1"})
        assert st == 200 and rp["parts"][0]["changes"][0]["to"] == "20", rp
        assert rp["parts"][0]["restart"] == "engine"
        st, rr = r.call("POST", "engine/restore", {"target": "h1", "version": "1",
                                                   "confirm": rp["confirm"]})
        assert st == 200 and rr["parts"][0]["state"] == "scheduled", rr
        assert rr["parts"][0]["restart"]["kind"] == "restore"
        r.wait_restart(rr["parts"][0]["restart"]["id"])
        cur = ftr._sim(ftr.state(r.eng))
        assert cur["current"] == 3 and ftr._set_of(cur, 3) == {"MAX_QUEUE": "20"}, cur
        calls = [c["tokens"][:2] for c in ftr.calls(r.eng)]
        assert ["settings", "restore"] in calls, calls
        # an unknown version, a version the engine no longer takes
        st, err = r.call("POST", "engine/restore", {"target": "h1", "version": "9"})
        assert st == 404 and err["error"]["type"] == "no_version", err
        # the conductor: a document changed, then its first version back
        v0, doc = r.store.get("settings")
        doc["journal"]["retention_days"] = 7
        res = r.apply([{"scope": "conductor", "set": {"settings": doc}}])
        assert res["parts"][0]["version_after"] == "settings@1", res
        doc["journal"]["retention_days"] = 9
        r.apply([{"scope": "conductor", "set": {"settings": doc}}])
        st, vers = r.call("GET", "conductor/versions")
        ids = [v["id"] for v in ok(vers, "versions")["versions"] if v["keys"] == ["settings"]]
        assert ids[:3] == ["settings@2", "settings@1", "settings@0"], ids
        st, rp = r.call("POST", "conductor/restore", {"version": "settings@1"})
        diff = rp["parts"][0]["changes"][0]["diff"]
        assert diff == [{"path": "$.journal.retention_days", "before": 9, "after": 7}], diff
        st, rr = r.call("POST", "conductor/restore", {"version": "settings@1",
                                                      "confirm": rp["confirm"]})
        assert st == 200 and rr["state"] == "applied", rr
        v, now = r.store.get("settings")
        assert v == 3 and now["journal"]["retention_days"] == 7
        assert r.store.meta("settings")["restored_from"] == 1
        # a document the store refuses: 422 with the store's words, nothing written
        bad = dict(now, default_profile="no-such-profile")
        st, err = r.call("POST", "conductor/plan", {"set": {"settings": bad}})
        assert st == 422 and err["error"]["type"] == "invalid_document", err
        # a locked profile
        st, err = r.call("POST", "conductor/plan", {"set": {"profiles.raw": {}}})
        assert st == 422 and err["error"]["type"] == "locked", err
    finally:
        r.close()
    return "engine overlay and conductor documents: versions listed, restored, checked"


# ---------------------------------------------------------------------------- 6 confirm tokens

def test_confirm_tokens():
    r = Rig("tokens")
    try:
        ch = [{"scope": "backends", "set": {"backends.eng.label": "Box"}}]
        p = r.plan(ch)
        st, err = r.call("POST", "apply", {"changes": ch})
        assert st == 428 and err["error"]["type"] == "confirm_required", err
        ok(err, "error")
        # the state moves between the plan and the apply: stale
        r.cfg.update_backend("eng", {"label": "Moved"}, who="t")
        st, err = r.call("POST", "apply", {"changes": ch, "confirm": p["confirm"]})
        assert st == 409 and err["error"]["type"] == "stale_plan", err
        assert err["error"]["plan"]["confirm"] != p["confirm"]
        st, err = r.call("POST", "apply", {"changes": ch, "confirm": "0" * 32})
        assert st == 409 and err["error"]["type"] == "stale_plan"
        # a fingerprint key: the second confirm
        fch = [{"scope": "engine", "target": "h1", "set": {"QWEN38_PREFIX_CHUNK": 4096}}]
        fp = r.plan(fch)
        assert fp["confirm_fingerprint"] and fp["fingerprint_keys"] == \
            ["engine/h1:QWEN38_PREFIX_CHUNK"], fp
        assert fp["parts"][0]["changes"][0]["fingerprint"] is True
        st, err = r.call("POST", "apply", {"changes": fch, "confirm": fp["confirm"]})
        assert st == 428 and err["error"]["type"] == "fingerprint_confirm_required", err
        # the sentence's verb agrees with the number of keys
        assert err["error"]["message"].startswith(
            "engine/h1:QWEN38_PREFIX_CHUNK feeds the disk cache's fingerprint"), err
        assert settings_mod.feeds(["A"]) == "A feeds"
        assert settings_mod.feeds(["A", "B"]) == "A, B feed"
        st, err = r.call("POST", "apply", {"changes": fch, "confirm": fp["confirm"],
                                           "confirm_fingerprint": "f" * 32})
        assert st == 409 and err["error"]["type"] == "stale_plan", err
        res = r.apply(fch, fp)
        assert res["parts"][0]["state"] == "scheduled"
        r.wait_restart(res["parts"][0]["restart"]["id"])
        tokens = [c["tokens"] for c in ftr.calls(r.eng) if c["tokens"][:2] == ["settings", "apply"]]
        assert tokens and "--confirm-fingerprint" in tokens[-1], tokens
        # the same confirm again: the same result, nothing applied twice
        p = r.plan(ch)
        first = r.apply(ch, p)
        st, again = r.call("POST", "apply", {"changes": ch, "confirm": p["confirm"]})
        assert st == 200 and again["replayed"] and not first["replayed"]
        assert again["parts"] == first["parts"]
        n_rows = len([x for x in r.audit_rows() if x["change_set"] == p["change_set"]])
        assert n_rows == 3, n_rows     # the backend's update, the part, the set: once
        # a change that holds already: unchanged, no confirm needed
        st, same = r.call("POST", "plan", {"changes": ch})
        assert st == 200 and not same["changed"] and same["confirm"] is None
        st, res = r.call("POST", "apply", {"changes": ch})
        assert st == 200 and res["state"] == "unchanged", res
        # a second change set while one applies: busy
        r.s._apply.acquire()
        try:
            p2 = r.plan([{"scope": "backends", "set": {"backends.eng.label": "Other"}}])
            st, err = r.call("POST", "apply", {"changes": [{"scope": "backends", "set": {
                "backends.eng.label": "Other"}}], "confirm": p2["confirm"]})
            assert st == 409 and err["error"]["type"] == "busy", err
        finally:
            r.s._apply.release()
    finally:
        r.close()
    return "428 without, 409 stale, second confirm for the fingerprint, replay, busy"


# ---------------------------------------------------------------------------- 7 restarts

def test_restart_scheduling():
    gateway = {}
    r = Rig("restarts", sim={"sim_steps": ["waiting"] * 6 + ["acting", "acting", "done"]})
    try:
        ch = [{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 40}}]
        res = r.apply(ch, deadline_s=120)
        rs = res["parts"][0]["restart"]
        assert rs["state"] == "draining" and rs["can_cancel"] and rs["remaining_s"] in (119, 120)
        # the gateway's question: drained while the engine's job waits; another backend serves
        assert r.restarts.drained("eng") == "box (restarting for a settings change)"
        assert r.restarts.drained("cloud") is None
        seen = set()

        def watch():
            v = r.restarts.view(rs["id"])
            seen.add((v["state"], v["can_cancel"], bool(r.restarts.drained("eng"))))
            gateway.setdefault(v["state"], v)
            return v if v["state"] in settingsengine.FINAL else None
        v = until(watch, 15)
        assert v and v["state"] == "done", (v, r.logs)
        assert ("draining", True, True) in seen and ("restarting", False, True) in seen, seen
        assert r.restarts.drained("eng") is None, "undrained once back"
        assert v["warm"] == {"ok": True, "backends": {"eng": {"ok": True, "why": None}}}
        st, lst = r.call("GET", "restarts")
        assert st == 200 and ok(lst, "restarts")["restarts"][0]["id"] == rs["id"]
        # a failed start: the engine's rollback is reported
        ftr.set_state(r.eng, sim_steps=["waiting", "acting", "failed_start"])
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 44}}])
        v = r.wait_restart(res["parts"][0]["restart"]["id"])
        assert v["state"] == "failed" and v["restored"] == {"version": 1, "healthy": True}, v
        assert "is back" in v["message"] and not v["draining"]
        assert ftr._sim(ftr.state(r.eng))["current"] == 1
        # a deadline that passes while the job waits: timeout, nothing changed
        ftr.set_state(r.eng, sim_steps=["waiting", "waiting", "timeout"])
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 45}}],
                      deadline_s=10)
        v = r.wait_restart(res["parts"][0]["restart"]["id"])
        assert v["state"] == "timeout" and "nothing changed" in v["message"], v
        assert r.restarts.drained("eng") is None
        # cancel while the job waits: the engine's job is replaced, the backends serve again
        ftr.set_state(r.eng, sim_steps=["waiting"], sim_hold=False)
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 46}}])
        rid = res["parts"][0]["restart"]["id"]
        assert until(lambda: r.restarts.view(rid)["state"] == "draining")
        st, cv = r.call("POST", f"restarts/{rid}/cancel", {})
        assert st == 200 and ok(cv, "restart")["state"] == "cancelled", cv
        assert r.restarts.drained("eng") is None
        sim = ftr._sim(ftr.state(r.eng))
        assert sim["job"]["kind"] == "start" and sim["current"] == 1, sim
        st, err = r.call("POST", f"restarts/{rid}/cancel", {})
        assert st == 409 and err["error"]["type"] == "ended", err
        # cancel while the job acts: too late
        ftr.set_state(r.eng, sim_steps=["acting", "done"], sim_hold=True)
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 47}}])
        rid = res["parts"][0]["restart"]["id"]
        assert until(lambda: r.restarts.view(rid)["state"] == "restarting"), \
            r.restarts.view(rid)
        st, err = r.call("POST", f"restarts/{rid}/cancel", {})
        assert st == 409 and err["error"]["type"] == "too_late", err
        ftr.set_state(r.eng, sim_hold=False)
        assert r.wait_restart(rid)["state"] == "done"
        # the audit: each end and the cancel, with their change sets
        acts = [(x["action"], json.loads(x["detail"])["result"]) for x in r.audit_rows()
                if x["object"] == "settings" and x["action"] in ("settings.restart",
                                                                 "settings.cancel")]
        assert ("settings.cancel", "cancelled") in acts and ("settings.restart", "failed") in acts \
            and ("settings.restart", "timeout") in acts and ("settings.restart", "done") in acts, \
            acts
        st, err = r.call("POST", "restarts/rs-nope/cancel", {})
        assert st == 404
    finally:
        r.close()
    return "drain, countdown, restarting, warm-up, back; failed start, timeout, cancel"


def test_restart_at_a_time():
    r = Rig("restart-at")
    try:
        at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(time.time() + 2))
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 30}}],
                      when=f"at={at}")
        rs = res["parts"][0]["restart"]
        assert rs["state"] == "scheduled" and r.restarts.drained("eng") is None, rs
        tok = [c["tokens"] for c in ftr.calls(r.eng) if c["tokens"][:2] == ["settings", "apply"]]
        assert f"at={at}" in tok[-1], tok
        v = r.wait_restart(rs["id"], 20)
        assert v["state"] == "done", v
        st, err = r.call("POST", "apply", {"changes": [{"scope": "engine", "target": "h1",
                                                        "set": {"MAX_QUEUE": 31}}],
                                           "when": "now", "deadline_s": 30})
        assert st == 400, err
    finally:
        r.close()
    return "when at=: scheduled until the time, then drained and restarted"


# ---------------------------------------------------------------------------- 8 hooks

def test_hooks_adapter():
    r = Rig("hooks")
    try:
        st, sch = r.call("GET", "hooks/schema")
        assert st == 200 and ok(sch, "schema")["keys"] == [] and sch["why"], sch
        st, err = r.call("POST", "hooks/plan", {"set": {"hooks.x.mode": "on"}})
        assert st == 422 and err["error"]["type"] == "unknown_key", err
        st, vers = r.call("GET", "hooks/versions")
        assert st == 200 and ok(vers, "versions")["versions"] == []
        hooks = FakeHooks()
        r.scopes["hooks"].plug(hooks)
        st, sch = r.call("GET", "hooks/schema")
        assert [k["key"] for k in ok(sch, "schema")["keys"]] == ["hooks.audit.mode",
                                                                 "hooks.audit.token"]
        st, err = r.call("POST", "hooks/plan", {"set": {"hooks.audit.mode": "loud"}})
        assert st == 422 and "off, shadow or on" in err["error"]["message"], err
        res = r.apply([{"scope": "hooks", "set": {"hooks.audit.mode": "on"}}])
        assert hooks.vals["hooks.audit.mode"] == "on" and res["parts"][0]["version_after"]
        st, rp = r.call("POST", "hooks/restore", {"version": res["parts"][0]["version_before"]})
        st, rr = r.call("POST", "hooks/restore", {"version": res["parts"][0]["version_before"],
                                                  "confirm": rp["confirm"]})
        assert st == 200 and hooks.vals["hooks.audit.mode"] == "off", rr
        # a registry that fails its write: the part fails and says so
        r.scopes["hooks"].plug(FakeHooks(fail_write=True))
        p = r.plan([{"scope": "hooks", "set": {"hooks.audit.mode": "shadow"}}])
        res = r.apply([{"scope": "hooks", "set": {"hooks.audit.mode": "shadow"}}], p)
        assert res["state"] == "failed" and res["parts"][0]["error"]["type"] == "internal", res
    finally:
        r.close()
    return "a stub until the registry plugs in; then plan, apply, versions, restore"


# ---------------------------------------------------------------------------- 9 app, backends

def test_app_backends_refusals():
    env = {"TANDEM_BACKENDS": "envb", "TANDEM_BACKEND_ENVB_KIND": "generic",
           "TANDEM_BACKEND_ENVB_URL": "http://127.0.0.1:9/v1", "TANDEM_BACKEND_ENVB_KEY": SECRET,
           "TANDEM_ROUTE_1": "env-*=envb"}
    r = Rig("refusals")
    try:
        # the app scope: through the page's switches, unset hands it back to the install
        res = r.apply([{"scope": "app", "set": {"conductor": "on", "chat": "off"}}])
        assert res["state"] == "applied" and ("conductor", os.path.join(r.dir, "conductor"),
                                              "admin") in r.app.calls, r.app.calls
        assert appparts.stored(r.cfg, "conductor") is True
        assert appparts.stored(r.cfg, "chat") is False
        st, vals = r.call("GET", "app")
        assert {v["key"]: (v["value"], v["source"]) for v in vals["values"]} == \
            {"chat": ("off", "page"), "conductor": ("on", "page")}
        res = r.apply([{"scope": "app", "unset": ["conductor", "chat"]}])
        assert appparts.stored(r.cfg, "conductor") is None
        assert appparts.stored(r.cfg, "chat") is None
        st, err = r.call("POST", "app/plan", {"set": {"chat": "maybe"}})
        assert st == 422 and err["error"]["type"] == "bad_value"
    finally:
        r.close()
    d = tempfile.mkdtemp(prefix="settings-env-")
    cfg = config_mod.ConfigDB(os.path.join(d, "config.sqlite3"), env=env, use_keychain=False).open()
    try:
        cfg.apply_env()
        cfg.create_backend({"id": "mine", "kind": "generic", "url": "http://127.0.0.1:10/v1"})
        s = settings_mod.Settings(cfg, {"backends": settingscopes.BackendsScope(cfg)},
                                  log=lambda m: None)

        def call(method, path, body=None, q=None):
            return s.handle(method, [p for p in path.split("/") if p], q or {}, body, "admin")
        st, sch = call("GET", "backends/schema")
        keys = {k["key"]: k for k in ok(sch, "schema")["keys"]}
        assert not keys["backends.envb.url"]["writable"] and \
            keys["backends.envb.url"]["variable"] == "TANDEM_BACKEND_ENVB_URL"
        assert not keys["backends.envb.key"]["writable"] and keys["backends.envb.key"]["secret"]
        st, err = call("POST", "backends/plan", {"set": {"backends.envb.url": "http://x:1/v1"}})
        assert st == 422 and err["error"]["type"] == "locked" and \
            err["error"]["variable"] == "TANDEM_BACKEND_ENVB_URL", err
        for bad, kind in (({"backends.mine.url": "ftp://x"}, "bad_value"),
                          ({"backends.mine.max_inflight": 0}, "bad_value"),
                          ({"backends.mine.fallback": ["mine"]}, "bad_value"),
                          ({"backends.nope.url": "http://x:1/v1"}, "unknown_key"),
                          ({"routes": [{"pattern": "a*", "backend": "nope"}]}, "bad_value"),
                          ({"routes": []}, "bad_value")):
            st, err = call("POST", "backends/plan", {"set": bad})
            assert st == 422 and err["error"]["type"] == kind, (bad, st, err)
        st, err = call("POST", "backends/plan", {"unset": ["backends.mine.url"]})
        assert st == 422, err
        # routes: the environment's rule keeps its place
        st, vals = call("GET", "backends")
        routes = next(v for v in vals["values"] if v["key"] == "routes")["value"]
        assert routes == [{"pattern": "env-*", "backend": "envb", "enabled": True,
                           "variable": "TANDEM_ROUTE_1"}], routes
        new = routes + [{"pattern": "m-*", "backend": "mine", "enabled": True}]
        st, p = call("POST", "backends/plan", {"set": {"routes": new}})
        assert st == 200 and p["changed"], p
        st, res = call("POST", "backends/apply", {"set": {"routes": new}, "confirm": p["confirm"]})
        assert st == 200 and [x["pattern"] for x in cfg.rules()] == ["env-*", "m-*"], res
        # bodies and routes
        for body in (None, [], {"changes": []}, {"changes": [{"scope": "nope", "set": {"a": 1}}]},
                     {"changes": [{"scope": "backends"}]},
                     {"changes": [{"scope": "backends", "set": {"a": 1}, "unset": ["a"]}]},
                     {"changes": [{"scope": "backends", "set": {"bad key": 1}}]},
                     {"changes": [{"scope": "backends", "set": {"a": 1}, "extra": 1}]},
                     {"changes": [{"scope": "backends", "set": {"a": 1}},
                                  {"scope": "backends", "set": {"b": 1}}]},
                     {"changes": [{"scope": "backends", "restore": "1", "set": {"a": 1}}]}):
            st, err = call("POST", "plan", body)
            assert st in (400, 404) and ok(err, "error"), (body, st, err)
        assert call("GET", "nope")[0] == 404
        assert call("DELETE", "backends")[0] == 404
        assert call("GET", "engine")[0] == 404, "a scope this server does not have"
        st, err = call("GET", "audit", q={"change_set": "x"})
        assert st == 400
        st, err = call("POST", "apply", {"changes": [{"scope": "backends",
                                                      "set": {"backends.mine.label": "M"}}],
                                         "deadline_s": 5})
        assert st == 400, err
    finally:
        cfg.close()
    return "app through the switches; env-locked, bad values, unknown keys, bad bodies refused"


# ---------------------------------------------------------------------------- 10 the contract

def test_contract_files():
    files = sorted(glob.glob(os.path.join(CON, "*.json")))
    assert files
    docs = {}
    for f in files:
        with open(f) as fh:
            docs[os.path.basename(f)] = json.load(fh)

    def refs(node):
        if isinstance(node, dict):
            if "$ref" in node:
                yield node["$ref"]
            for v in node.values():
                yield from refs(v)
        elif isinstance(node, list):
            for v in node:
                yield from refs(v)
    for name, doc in docs.items():
        for ref in refs(doc):
            file, _, frag = ref.partition("#")
            base = docs[file] if file else doc
            node = base
            for part in [p for p in frag.split("/") if p]:
                node = node[part]
            assert isinstance(node, dict), (name, ref)
    scopes = docs["scopes.json"]
    assert [s["name"] for s in scopes["scopes"]] == list(settings_mod.SCOPES)
    assert scopes["operations"] == list(settings_mod.OPS)
    assert scopes["restart_classes"] == list(settings_mod.RESTART)
    assert scopes["types"] == list(settings_mod.TYPES)
    for r in scopes["routes"]:
        for k in ("schema", "body"):
            for f in (r.get(k) or "").split(" | "):
                if f:
                    assert f in docs, (r, f)
    # every error type the code raises is listed, with the status it raises
    raised = {}
    for mod in ("settings", "settingscopes", "settingsengine", "hooksettings", "pluginsettings"):
        src = open(os.path.join(ROOT, "proxy", f"{mod}.py")).read()
        for st, kind in re.findall(r'SettingsError\(\s*(\d{3}),\s*"([a-z_]+)"', src):
            raised.setdefault(kind, set()).add(int(st))
    raised.setdefault("confirm_required", {428})
    raised.setdefault("grant_confirm_required", {428})
    for kind, sts in raised.items():
        assert kind in scopes["errors"], kind
        assert scopes["errors"][kind] in sts or len(sts) > 1, (kind, sts)
    # the validator says no to a broken answer
    assert contract_check.validate({"contract_version": "1.0"}, docs["plan.schema.json"], root=CON)
    bad_part = {"scope": "app", "target": "controller", "kind": "set", "state": "weird",
                "keys": [], "version_before": None, "version_after": None, "restore": [],
                "restart": None, "error": None}
    assert contract_check.validate(bad_part, docs["common.schema.json"]["$defs"]["part_result"],
                                   root=CON, doc=docs["common.schema.json"])
    readme = open(os.path.join(CON, "README.md")).read()
    for name in docs:
        assert f"`{name}`" in readme or name in readme, name
    return f"{len(docs)} files: refs resolve, routes name files, errors listed, constants agree"


# ---------------------------------------------------------------------------- 11 the engine's mode

def test_engine_mode():
    r = Rig("mode")
    try:
        st, sch = r.call("GET", "engine/schema", q={"target": "h1"})
        keys = {k["key"]: k for k in ok(sch, "schema")["keys"]}
        assert "QSE_MODE" not in keys and "QSE_EXT" not in keys, sorted(keys)
        assert keys["mode"]["type"] == "enum" and keys["mode"]["values"] == ["engine", "chat"]
        assert keys["mode"]["restart"] == "engine" and keys["mode.ext"]["type"] == "module"
        assert keys["mode.ext"]["default"] == "tandemapp.engine_ext" and keys["mode.ext"]["nullable"]
        st, vals = r.call("GET", "engine", q={"target": "h1"})
        got = {v["key"]: v for v in ok(vals, "values")["values"]}
        assert got["mode"] == {"key": "mode", "value": "engine", "set": True, "source": "default"}
        assert got["mode.ext"]["value"] is None and not got["mode.ext"]["set"]
        # chat mode: the extension is missing, this controller's wheel is installed first
        ch = [{"scope": "engine", "target": "h1", "set": {"mode": "chat"}}]
        p = r.plan(ch)
        part = p["parts"][0]
        assert part["restart"] == "engine" and part["restart_needed"], part
        assert [(c["key"], c["from"], c["to"]) for c in part["changes"]] == [
            ("mode", "engine", "chat"), ("mode.ext", None, "tandemapp.engine_ext")], part
        assert any("tandemapp-0.15.0-py3-none-any.whl" in w for w in part["warnings"]), part
        n = len(ftr.calls(r.eng))
        res = r.apply(ch, p)
        assert res["state"] == "applied" and res["parts"][0]["state"] == "scheduled", res
        rs = res["parts"][0]["restart"]
        assert rs["kind"] == "mode" and rs["change_set"] == p["change_set"]
        assert rs["job"]["kind"] == "mode" and rs["job"]["when"]["mode"] == "idle", rs
        assert rs["backends"] == ["eng"] and r.restarts.drained("eng"), "drained while it waits"
        assert r.wait_restart(rs["id"])["state"] == "done"
        assert r.warms and r.warms[-1][0] == "eng", "one warm-up token"
        toks = [c["tokens"] for c in ftr.calls(r.eng)[n:]]
        inst = [t for t in toks if t[:2] == ["ext", "install"]]
        mset = [t for t in toks if t[:2] == ["mode", "set"]]
        assert len(inst) == 1 and len(mset) == 1 and toks.index(inst[0]) < toks.index(mset[0]), toks
        with open(r.wheel, "rb") as fh:
            import hashlib
            sha = hashlib.sha256(fh.read()).hexdigest()
        assert inst[0][inst[0].index("--sha256") + 1] == sha
        assert mset[0] == ["mode", "set", "chat", "--ext", "tandemapp.engine_ext", "--when", "idle",
                           "--deadline", "600"], mset
        assert ftr.stdin_seen(r.eng)[-1]["sha256"] == sha
        st, vals = r.call("GET", "engine", q={"target": "h1"})
        got = {v["key"]: v for v in vals["values"]}
        assert got["mode"]["value"] == "chat" and got["mode"]["source"] == "managed"
        assert got["mode.ext"]["value"] == "tandemapp.engine_ext"
        rows = [x for x in r.audit_rows() if x["change_set"] == p["change_set"]]
        assert {x["action"] for x in rows} >= {"settings.apply", "settings.change_set",
                                               "settings.restart"}, rows
        # planned again it holds: nothing to change, nothing installed
        p2 = r.plan(ch)
        assert not p2["changed"] and p2["confirm"] is None, p2
        # back to the bare engine: no install, mode set engine
        n = len(ftr.calls(r.eng))
        res = r.apply([{"scope": "engine", "target": "h1", "set": {"mode": "engine"}}])
        r.wait_restart(res["parts"][0]["restart"]["id"])
        toks = [c["tokens"] for c in ftr.calls(r.eng)[n:]]
        assert ["mode", "set", "engine", "--when", "idle", "--deadline", "600"] in toks, toks
        assert not [t for t in toks if t[:2] == ["ext", "install"]]
        # the extension is there now (installed above): chat again plans no install
        p = r.plan(ch)
        assert p["changed"] and not any(".whl" in w for w in p["parts"][0]["warnings"]), p
        # no wheel here and the extension missing: refused with the fix, on its key
        ftr.set_state(r.eng, ext={})
        r.wheel_ok = False
        st, err = r.call("POST", "plan", {"changes": ch})
        assert st == 409 and err["error"]["type"] == "extension_missing", err
        assert err["error"]["key"] == "mode.ext" and "TANDEM_NODE_WHEEL" in err["error"]["message"]
        ok(err, "error")
        r.wheel_ok = True
        # a module that is not TandemApp's: refused, install it on the engine host
        st, err = r.call("POST", "plan", {"changes": [{"scope": "engine", "target": "h1",
                                                       "set": {"mode": "chat",
                                                               "mode.ext": "acme.ext"}}]})
        assert st == 409 and err["error"]["type"] == "extension_missing", err
        assert "tandem remote ext install" in err["error"]["message"]
        # with another key of the same engine: one overlay change, one restart
        n = len(ftr.calls(r.eng))
        ch2 = [{"scope": "engine", "target": "h1", "set": {"mode": "chat", "MAX_QUEUE": 24}}]
        p = r.plan(ch2)
        keys2 = {c["key"]: c for c in p["parts"][0]["changes"]}
        assert set(keys2) == {"mode", "mode.ext", "MAX_QUEUE"}, keys2
        assert keys2["mode"]["to"] == "chat" and keys2["mode.ext"]["to"] == "tandemapp.engine_ext"
        res = r.apply(ch2, p)
        rs = res["parts"][0]["restart"]
        assert rs["kind"] == "settings" and r.wait_restart(rs["id"])["state"] == "done"
        toks = [c["tokens"] for c in ftr.calls(r.eng)[n:]]
        assert len([t for t in toks if t[:2] == ["settings", "apply"]]) == 1, toks
        assert not [t for t in toks if t[:2] == ["mode", "set"]]
        sim = ftr._sim(ftr.state(r.eng))
        assert ftr._set_of(sim, sim["current"])["QSE_MODE"] == "chat"
        assert ftr._set_of(sim, sim["current"])["QSE_EXT"] == "tandemapp.engine_ext"
        # QSE_MODE itself, the extension with the bare engine, a mode that is none: refused
        for sets, key, kind in (({"QSE_MODE": "chat"}, "QSE_MODE", "locked"),
                                ({"mode": "engine", "mode.ext": "x.y"}, "mode.ext", "bad_value"),
                                ({"mode": "setup"}, "mode", "bad_value")):
            st, err = r.call("POST", "plan", {"changes": [{"scope": "engine", "target": "h1",
                                                           "set": sets}]})
            assert st == 422 and err["error"]["type"] == kind, (sets, err)
            assert err["error"]["key"] == key, (sets, err)
    finally:
        r.close()
    return "chat with the wheel installed first, engine back, refusals on their key, one restart"


def test_restart_estimate():
    r = Rig("estimate")
    try:
        ch = [{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 30}}]
        e = r.plan(ch)["parts"][0]
        assert e["restart_estimate"] == {"seconds": settingsengine.DEFAULT_RESTART_S,
                                         "source": "default"}, e
        # hot keys only, a store's change: no restart, no estimate
        p = r.plan([{"scope": "engine", "target": "h1", "set": {"QSE_ALLOW_FROM": "10.0.0.0/8"}},
                    {"scope": "backends", "set": {"backends.cloud.label": "Cloud 2"}}])
        assert [x["restart_estimate"] for x in p["parts"]] == [None, None], p
        # the engine's own last start: a job that acted at once, created to ended
        ftr.set_state(r.eng, sim_job={"id": "20261115T090000-abcdef", "kind": "restart",
                                      "state": "done", "phase": "done", "when": {"mode": "now"},
                                      "created_at": "2026-11-15T09:00:00Z",
                                      "ended_at": "2026-11-15T09:02:24Z", "message": "healthy",
                                      "_steps": ["done"], "_i": 0})
        e = r.plan(ch)["parts"][0]
        assert e["restart_estimate"] == {"seconds": 144, "source": "engine"}, e
        # a job that waited for idle first says nothing of the start alone
        assert settingsengine.engine_start_s({"job": {
            "kind": "settings", "state": "done", "when": {"mode": "idle"},
            "created_at": "2026-11-15T09:00:00Z", "ended_at": "2026-11-15T09:30:00Z"}}) is None
        # once this controller followed a restart of it: what it measured
        res = r.apply(ch)
        assert r.wait_restart(res["parts"][0]["restart"]["id"])["state"] == "done"
        took = r.restarts.took("h1")
        assert isinstance(took, int) and 0 <= took < 30, took
        e = r.plan([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 31}}])["parts"][0]
        assert e["restart_estimate"] == {"seconds": took, "source": "measured"}, e
        # the mode's own plan says it too
        e = r.plan([{"scope": "engine", "target": "h1", "set": {"mode": "chat"}}])["parts"][0]
        assert e["restart_estimate"]["source"] == "measured", e
    finally:
        r.close()
    return "default, then the engine's own last start, then what this controller measured"


def test_refusal_names_its_place():
    r = Rig("place", hooks=FakeHooks())
    try:
        # two engine hosts with the same keys: the refusal says which one
        eng2 = ftr.install(os.path.join(r.dir, "engine2"))
        ftr.set_state(eng2, sim=True)
        rows = [r.row, {"id": "h2", "alias": "box2"}]
        r.scopes["engine"].rows = lambda: rows
        dirs = {"h1": r.eng, "h2": eng2}
        r.scopes["engine"].ctl_for = lambda row: enginectl.LocalControl(dirs[row["id"]],
                                                                        label=row["id"])
        cases = [
            ([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 20}},
              {"scope": "engine", "target": "h2", "set": {"MAX_QUEUE": 21, "PORT": 1}}],
             ("locked", "engine", "h2", "PORT")),
            ([{"scope": "engine", "target": "h2", "set": {"NOPE_KEY": 1}}],
             ("unknown_key", "engine", "h2", "NOPE_KEY")),
            ([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 20}},
              {"scope": "backends", "set": {"backends.cloud.max_inflight": "many"}}],
             ("bad_value", "backends", "controller", "backends.cloud.max_inflight")),
            ([{"scope": "conductor", "set": {"settings": {"default_profile": "no-such"}}}],
             ("invalid_document", "conductor", "controller", "settings")),
            ([{"scope": "hooks", "set": {"hooks.audit.mode": "loud"}}],
             ("bad_value", "hooks", "controller", "hooks.audit.mode")),
        ]
        for changes, want in cases:
            st, err = r.call("POST", "plan", {"changes": changes})
            e = ok(err, "error")["error"]
            assert st == 422 and (e["type"], e["scope"], e["target"], e["key"]) == want, (want, err)
        # a refusal that is not about a value names the part, not a key
        r.scopes["engine"].ctl_for = lambda row: (_ for _ in ()).throw(
            enginectl.CtlTransport("ssh: connect timed out", verb="settings get"))
        st, err = r.call("POST", "plan", {"changes": [{"scope": "engine", "target": "h2",
                                                       "set": {"MAX_QUEUE": 9}}]})
        e = err["error"]
        assert st == 502 and (e["type"], e["scope"], e["target"]) == ("engine_unreachable",
                                                                       "engine", "h2"), err
        assert "key" not in e, err
    finally:
        r.close()
    return "scope and target on every part's refusal; the key of a value's refusal"


def test_unset_of_an_unset_key():
    r = Rig("unset", hooks=FakeHooks())
    try:
        # the backends: a field that holds nothing is unset, as the database has it
        st, vals = r.call("GET", "backends")
        got = {v["key"]: v for v in ok(vals, "values")["values"]}
        assert got["backends.eng.max_inflight"]["value"] is None
        assert got["backends.eng.max_inflight"]["set"] is False, got["backends.eng.max_inflight"]
        assert got["backends.eng.url"]["set"] is True and got["backends.eng.key"]["set"] is False
        cases = [
            {"scope": "backends", "unset": ["backends.eng.max_inflight"]},
            {"scope": "backends", "set": {"backends.eng.host.url": None}},
            {"scope": "backends", "unset": ["backends.eng.key"]},
            {"scope": "app", "unset": ["chat"]},
            {"scope": "hooks", "unset": ["hooks.audit.token"]},
            {"scope": "engine", "target": "h1", "unset": ["QWEN38_PREFIX_CHUNK"]},
            {"scope": "engine", "target": "h1", "unset": ["mode"]},
            {"scope": "engine", "target": "h1", "unset": ["mode.ext"]},
        ]
        n = len(ftr.calls(r.eng))
        for c in cases:
            p = r.plan([c])
            assert not p["changed"] and p["parts"][0]["changes"] == [], (c, p)
            st, res = r.call("POST", "apply", {"changes": [c]})
            assert st == 200 and ok(res, "result")["state"] == "unchanged", (c, res)
        toks = [t["tokens"][:2] for t in ftr.calls(r.eng)[n:]]
        assert ["settings", "plan"] not in toks and ["settings", "apply"] not in toks, toks
        # beside a set, the unset is dropped and the set plans
        p = r.plan([{"scope": "engine", "target": "h1", "set": {"MAX_QUEUE": 18},
                     "unset": ["QWEN38_PREFIX_CHUNK"]},
                    {"scope": "backends", "set": {"backends.eng.label": "E"},
                     "unset": ["backends.eng.max_inflight"]}])
        assert [[c["key"] for c in x["changes"]] for x in p["parts"]] == [
            ["backends.eng.label"], ["MAX_QUEUE"]], p
        assert p["fingerprint_keys"] == [] and p["confirm_fingerprint"] is None, p
        # a key that holds a value is still unset
        r.apply([{"scope": "backends", "set": {"backends.eng.max_inflight": 4}}])
        p = r.plan([{"scope": "backends", "unset": ["backends.eng.max_inflight"]}])
        assert [(c["key"], c["from"], c["is_set"]) for c in p["parts"][0]["changes"]] == [
            ("backends.eng.max_inflight", 4, False)], p
    finally:
        r.close()
    return "the plan drops an unset of an unset key in every scope; the backends tell set apart"


if __name__ == "__main__":
    from scriptmode import run
    run(globals())
