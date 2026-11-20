"""The engine autodiscovery (proxy/autodetect.py): a paired node's engine on a custom port
joins as a backend on its own, and nothing else moves.

A fake TandemNode agent (tests/test_node_agent.py's real agent over a fake machine) lists one
`tandemllm` engine — a fake backend answering /health and /models on its own port. The sync,
run in the test's thread, must: register the engine under `node-<node>-<engine>` at
http://127.0.0.1:<port>/v1; never touch a backend the env declares or a person made, nor
duplicate one standing at the engine's url; drop a discovered backend after its engine goes
silent for DROP_AFTER cycles while a manual one at the same dead url stands; fill the default
backend only when the discovered engine is the only one that answers, and restore it when a
manual one answers again; and leave everything alone with TANDEM_ENGINE_AUTODETECT=off.

Run: python tests/test_proxy_autodetect.py
"""

from __future__ import annotations

import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fake_backend  # noqa: E402
import test_fleet as tf  # noqa: E402
import test_node_agent as tna  # noqa: E402
from ports import free_port  # noqa: E402
from proxy import autodetect  # noqa: E402
from proxy_util import Proxy, run  # noqa: E402


class Node:
    """A virtual node: its agent on a loopback port, one `tandemllm` engine `e` — a fake
    backend answering /health and /models on `eport`."""

    def __init__(self, serial: str, name: str, models=("probe-model",), port: int = 0):
        self.srv, self.eport = fake_backend.start(models=list(models), port=port)
        doc = tna.engine_doc("e", self.eport, kind="tandemllm")
        self.agent = tna.agent_on(tna.machine(serial), engines=[doc], address="127.0.0.1",
                                  virtual=name)
        self.run = tna.Running(self.agent)
        self.url = self.run.url

    def silence(self):
        """The engine stops answering: its /health finds nothing on the port now."""
        self.srv.shutdown()
        self.srv.server_close()
        self.srv = None

    def close(self):
        self.run.close()
        if self.srv is not None:
            self.srv.shutdown()
            self.srv.server_close()


class Ctl:
    """A controller (autodetect off: the test drives `sync()` itself) with these nodes paired,
    and one EngineAutoDetect over its fleet for the test to step by hand."""

    def __init__(self, nodes: dict, env=None):
        self.nodes = nodes                       # alias -> Node
        self.px = Proxy(env={"TANDEM_ENGINE_AUTODETECT": "off", **(env or {})}).__enter__()
        self.f = tf.fleet_on(self.px, tf.local_node(tf.fake_root(), h=tf.host(gpu=False)))
        self.ids = {}
        for alias, n in nodes.items():
            nid = n.agent.node_id()
            self.px.svc.cfg.add_node({"id": nid, "alias": alias, "endpoint": n.url,
                                      "address": "127.0.0.1",
                                      "trust": {"kind": "loopback-secret",
                                                "file": n.agent.secret_path}})
            self.ids[alias] = nid
        self.ad = autodetect.EngineAutoDetect(self.px.svc, self.f, log=self.px.logs.append)

    def sync(self):
        """One discovery cycle, then every backend health-checked once, as the poller would.
        (The default rule reads the health table, so a decision needs the cycle after the one
        that made a backend: the poller runs forever on a live controller, here it runs when the
        test says.)"""
        out = self.ad.sync()
        self.px.sync()
        return out

    def settle(self):
        """A second cycle over the health figures the first one left."""
        out = self.ad.sync()
        self.px.sync()
        return out

    def discovered(self):
        return sorted(b["id"] for b in self.px.svc.cfg.backends() if b.get("discovered"))

    def close(self):
        for src in [s for _, s in self.f.agents.values()]:
            src.close()
        self.px.__exit__(None, None, None)
        for n in self.nodes.values():
            n.close()


def test_an_engine_on_a_custom_port_joins_as_a_backend():
    node = Node("S-A1", "Desk")
    ctl = Ctl({"Desk": node})
    try:
        bid = autodetect.backend_id(ctl.ids["Desk"], "e")
        out = ctl.sync()
        assert out["added"] == [bid], out
        b = ctl.px.svc.cfg.backend(bid)
        assert b["kind"] == "tandemllm" and b["enabled"], b
        assert b["url"] == f"http://127.0.0.1:{node.eport}/v1", b["url"]
        assert b["label"] == "e on Desk", b["label"]
        assert b["discovered"] == {"node": ctl.ids["Desk"], "engine": "e"}, b["discovered"]
        assert b["updated_by"] == "autodetect", b["updated_by"]
        # the next cycle changes nothing: one row, the same url
        out = ctl.sync()
        assert out == {"added": [], "dropped": [], "seen": [bid]}, out
        assert len(ctl.px.svc.cfg.backends()) == 1
    finally:
        ctl.close()
        node.close()


def test_the_env_pinned_backend_is_left_alone():
    dead = free_port()
    node = Node("S-A2", "Desk")
    ctl = Ctl({"Desk": node}, env={
        "TANDEM_BACKENDS": "engine",
        "TANDEM_BACKEND_ENGINE_KIND": "tandemllm",
        "TANDEM_BACKEND_ENGINE_URL": f"http://127.0.0.1:{dead}/v1"})
    try:
        ctl.sync()
        pinned = ctl.px.svc.cfg.backend("engine")
        assert pinned["url"] == f"http://127.0.0.1:{dead}/v1", pinned["url"]
        assert pinned["discovered"] is None
        # the discovered engine joins beside it, under its own id
        assert ctl.discovered() == [autodetect.backend_id(ctl.ids["Desk"], "e")]
    finally:
        ctl.close()
        node.close()


def test_manual_backends_are_never_touched_or_duplicated():
    node = Node("S-A3", "Desk")
    ctl = Ctl({"Desk": node})
    try:
        cfg = ctl.px.svc.cfg
        # one manual backend stands at the engine's own url: no second one is made for it
        cfg.create_backend({"id": "mine", "kind": "tandemllm",
                            "url": f"http://127.0.0.1:{node.eport}/v1"}, who="gui")
        # one manual backend holds the id the discovery would use: its row is not moved
        taken = autodetect.backend_id(ctl.ids["Desk"], "e")
        cfg.create_backend({"id": taken, "kind": "tandemllm",
                            "url": "http://127.0.0.1:1/v1"}, who="gui")
        ctl.sync()
        assert ctl.discovered() == []
        assert cfg.backend("mine")["url"] == f"http://127.0.0.1:{node.eport}/v1"
        assert cfg.backend(taken)["url"] == "http://127.0.0.1:1/v1"
        assert cfg.backend(taken)["discovered"] is None
        assert sorted(b["id"] for b in cfg.backends()) == sorted(["mine", taken])
    finally:
        ctl.close()
        node.close()


def test_a_silent_engine_is_dropped_and_a_manual_one_stands():
    dead_engine = Node("S-A4", "Desk")
    ctl = Ctl({"Desk": dead_engine})
    try:
        bid = autodetect.backend_id(ctl.ids["Desk"], "e")
        ctl.sync()
        assert ctl.discovered() == [bid]
        # a manual backend at the same engine's url: the user's, never dropped
        ctl.px.svc.cfg.create_backend({"id": "mine", "kind": "tandemllm",
                                       "url": f"http://127.0.0.1:{dead_engine.eport}/v1"},
                                      who="gui")
        dead_engine.silence()
        for _ in range(autodetect.DROP_AFTER - 1):
            ctl.sync()
            assert ctl.discovered() == [bid], "still inside the silence budget"
        ctl.sync()
        assert ctl.discovered() == [], "silent past the budget: the discovered row is gone"
        assert ctl.px.svc.cfg.backend("mine") is not None, "the manual row stands"
    finally:
        ctl.close()
        dead_engine.close()


def test_the_default_is_filled_only_when_the_discovered_engine_is_the_only_one_up():
    dead = free_port()
    node = Node("S-A5", "Desk")
    ctl = Ctl({"Desk": node}, env={
        "TANDEM_BACKENDS": "local",
        "TANDEM_BACKEND_LOCAL_KIND": "tandemllm",
        "TANDEM_BACKEND_LOCAL_URL": f"http://127.0.0.1:{dead}/v1",
        "TANDEM_DEFAULT_BACKEND": "local"})
    try:
        bid = autodetect.backend_id(ctl.ids["Desk"], "e")
        ctl.sync()
        ctl.settle()
        # the configured default is down and the discovered engine is the only one up
        assert ctl.px.svc.cfg.default_backend == bid, ctl.px.logs
        # two discovered engines up is ambiguous: nothing is filled
        other = Node("S-B6", "Other")
        ctl.nodes["Other"] = other
        ctl.px.svc.cfg.add_node({"id": other.agent.node_id(), "alias": "Other",
                                 "endpoint": other.url, "address": "127.0.0.1",
                                 "trust": {"kind": "loopback-secret",
                                           "file": other.agent.secret_path}})
        ctl.sync()
        ctl.settle()
        assert ctl.px.svc.cfg.default_backend == "local", ctl.px.logs
        assert len(ctl.discovered()) == 2
        # a manual engine answers again: the default goes back to what app.env says
        live = fake_backend.start(models=["manual-model"])
        try:
            ctl.px.svc.cfg.create_backend({"id": "manual", "kind": "tandemllm",
                                           "url": f"http://127.0.0.1:{live[1]}/v1"}, who="gui")
            ctl.sync()
            ctl.settle()
            assert ctl.px.svc.cfg.default_backend == "local", ctl.px.logs
        finally:
            live[0].shutdown()
            live[0].server_close()
    finally:
        ctl.close()
        node.close()


def test_the_off_switch_leaves_everything_alone():
    node = Node("S-A7", "Desk")
    try:
        with Proxy(env={"TANDEM_ENGINE_AUTODETECT": "off"}) as px:
            assert px.svc.autodetect is None
            f = tf.fleet_on(px, tf.local_node(tf.fake_root(), h=tf.host(gpu=False)))
            px.svc.cfg.add_node({"id": node.agent.node_id(), "alias": "Desk",
                                 "endpoint": node.url, "address": "127.0.0.1",
                                 "trust": {"kind": "loopback-secret",
                                           "file": node.agent.secret_path}})
            assert [b for b in px.svc.cfg.backends() if b.get("discovered")] == []
            f.agents.clear()
        # and the wiring is there: with the switch on, the build owns one and Service.stop
        # stops it (the Proxy helper defaults the switch off, as it does the node agent)
        with Proxy(env={"TANDEM_ENGINE_AUTODETECT": "on"}) as px:
            assert px.svc.autodetect is not None
            thread = px.svc.autodetect._thread
            assert thread is not None and thread.is_alive()
        assert not thread.is_alive(), "Service.stop stopped the thread"
    finally:
        node.close()


if __name__ == "__main__":
    run(globals())