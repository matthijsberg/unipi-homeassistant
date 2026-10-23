"""T44: the local rules (button -> output) must not depend on the network or the MQTT broker.

Found in production (2026-10-22/23): the bridge reached evok through the box's own LAN address, so when the interface
lost its address (network outage / new DHCP address) the bridge lost evok - and every local rule with it - although
evok itself ran fine on the same machine."""
import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest
from aiohttp import WSMsgType, web
from conftest import REAL_SLEEP, drain, load_fixture, run

ROOT = Path(__file__).resolve().parent.parent


# ---- URL mapping ----------------------------------------------------------------------------------------------------------
def test_urls_pointing_at_this_machine_are_mapped_to_loopback(mod):
    f = mod.evok_url_to_loopback
    assert f("ws://127.0.0.2:8080/ws") == "ws://127.0.0.1:8080/ws"                         # a local address (stands for the LAN IP)  # pii-ok (test data)
    assert f("http://127.0.0.2:8080/rest/all") == "http://127.0.0.1:8080/rest/all"  # pii-ok (test data)
    assert f(f"ws://{socket.gethostname()}:8080/ws") == "ws://127.0.0.1:8080/ws"           # this machine's own name
    assert f("ws://127.0.0.2/ws") == "ws://127.0.0.1/ws"                                    # no port stays no port  # pii-ok (test data)


def test_other_machines_and_loopback_are_left_alone(mod):
    f = mod.evok_url_to_loopback
    for u in ("ws://127.0.0.1:8080/ws", "ws://localhost:8080/ws", "ws://[::1]:8080/ws", "ws://192.0.2.77:8080/ws",  # pii-ok (test data)
              "ws://some-other-host.example.invalid:8080/ws", "not a url", ""):
        assert f(u) == u


def test_bridge_uses_loopback_for_evok_but_keeps_the_real_host_for_the_ha_link(mod, tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    cfg = mod.AppConfig(mqtt={"broker": "mqtt.invalid"}, websocket={"url": "ws://127.0.0.2:8080/ws"},  # pii-ok (test data)
                        unipi_http={"url": "http://127.0.0.2:8080/rest/all"}, logging={"file_path": str(tmp_path / "b.log")},  # pii-ok (test data)
                        web_server={"enabled": False})
    b = mod.UnipiBridge(cfg, str(tmp_path / "config.json"))
    assert b.config.websocket.url == "ws://127.0.0.1:8080/ws" and str(b.config.unipi_http.url) == "http://127.0.0.1:8080/rest/all"
    assert b._build_configuration_url() == "http://127.0.0.2:8088"                          # HA's "Visit" link must not become 127.0.0.1  # pii-ok (test data)
    cfg.web_server.public_host = "unipi.example.invalid"
    assert b._build_configuration_url() == "http://unipi.example.invalid:8088"


def test_ha_link_uses_the_current_lan_address_when_evok_is_configured_on_loopback(mod, tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    cfg = mod.AppConfig(mqtt={"broker": "mqtt.invalid"}, websocket={"url": "ws://127.0.0.1:8080/ws"},
                        unipi_http={"url": "http://127.0.0.1:8080/rest/all"}, logging={"file_path": str(tmp_path / "b.log")},
                        web_server={"enabled": False})
    b = mod.UnipiBridge(cfg, str(tmp_path / "config.json"))
    monkeypatch.setattr(b, "_lan_ip", lambda: "192.0.2.5")  # pii-ok (test data)
    assert b._build_configuration_url() == "http://192.0.2.5:8088"  # pii-ok (test data)
    monkeypatch.setattr(b, "_lan_ip", lambda: None)                                          # no route right now
    assert b._build_configuration_url() == "http://localhost:8088"


def test_lan_ip_is_the_address_that_routes_to_the_broker_and_needs_no_traffic(bridge):
    bridge.config.mqtt.broker, bridge.config.mqtt.port = "127.0.0.1", 1883                   # nothing listens: UDP connect sends nothing
    assert bridge._lan_ip() == "127.0.0.1"
    bridge.config.mqtt.broker = "256.256.256.256"  # pii-ok (test data)
    assert bridge._lan_ip() is None


def test_a_long_broker_outage_cannot_grow_the_mqtt_queue_without_limit(mod, tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    cfg = mod.AppConfig(mqtt={"broker": "mqtt.invalid"}, websocket={"url": "ws://127.0.0.1:1/ws"},
                        unipi_http={"url": "http://127.0.0.1:1/rest/all"}, logging={"file_path": str(tmp_path / "b.log")},
                        web_server={"enabled": False})
    b = mod.UnipiBridge(cfg, str(tmp_path / "config.json"))
    assert b.mqtt_client._max_queued_messages == 2000


# ---- discovery never gives up ----------------------------------------------------------------------------------------------
def test_startup_keeps_retrying_discovery_instead_of_leaving_the_rules_dead(bridge, fast_sleep):
    calls = {"n": 0}

    async def flaky(self_=None):
        calls["n"] += 1
        return None if calls["n"] < 4 else "Neuron_TEST_1"
    bridge.perform_discovery_and_mqtt_subscribe = flaky
    bridge.publish_bridge_discovery = lambda: None
    bridge.initial_websocket_ready.set()

    async def go():
        await asyncio.wait_for(bridge._async_startup(), timeout=5)
    run(bridge, go())
    assert calls["n"] == 4 and bridge.initial_discovery_complete.is_set() and bridge.device_name == "Neuron_TEST_1"


# ---- the MQTT watchdog only restarts the bridge when a restart can help ---------------------------------------------------
def _run_monitor(b, monkeypatch, reachable, seconds=2.5):
    monkeypatch.setattr(b.mod, "MQTT_WATCHDOG_SECONDS", 1)
    b.mqtt_client.connected = False
    probes = {"n": 0}

    async def probe():
        probes["n"] += 1
        return reachable
    b._broker_reachable = probe
    stops, real_stop = [], b.loop.stop
    b.loop.stop = lambda: stops.append(1)                        # the monitor must not stop the test's loop (restored below)

    async def go():
        async def stopper():
            end = time.monotonic() + seconds
            while time.monotonic() < end and not b.should_stop.is_set():
                await REAL_SLEEP(0.05)
            b.should_stop.set()
        s = asyncio.ensure_future(stopper())
        await b._mqtt_monitor()
        b.loop.stop = real_stop                                  # run_until_complete needs the real one to finish
        await s
    run(b, go())
    return b.exit_code, probes["n"]


def test_mqtt_monitor_stays_up_while_the_network_or_broker_is_down(bridge, mod, monkeypatch, fast_sleep):
    bridge.mod = mod
    code, probes = _run_monitor(bridge, monkeypatch, reachable=False)
    assert code == 0                                                                       # no exit code 1: no restart
    assert probes == 1                                                                     # and it does not hammer the network: one probe per 5 min


def test_mqtt_monitor_still_restarts_a_stuck_client_when_the_broker_is_reachable(bridge, mod, monkeypatch, fast_sleep):
    bridge.mod = mod
    code, probes = _run_monitor(bridge, monkeypatch, reachable=True)
    assert code == 1 and probes == 1


def test_broker_reachability_probe(bridge):
    srv = socket.socket(); srv.bind(("127.0.0.1", 0)); srv.listen(1)
    bridge.config.mqtt.broker, bridge.config.mqtt.port = "127.0.0.1", srv.getsockname()[1]
    assert run(bridge, bridge._broker_reachable()) is True
    srv.close()
    assert run(bridge, bridge._broker_reachable()) is False


# ---- end to end: real bridge process, fake evok, NO broker ----------------------------------------------------------------
class FakeEvok:
    """evok stand-in on 127.0.0.1: /rest/all (recorded S103 snapshot) and /ws (collects what the bridge sends, can push events)."""

    def __init__(self):
        self.received, self.clients, self.rest_hits = [], [], 0
        self.loop = asyncio.new_event_loop()
        self.ready = threading.Event()
        self.rest_fail = 0                      # answer the next N REST requests with 500
        threading.Thread(target=self._serve, daemon=True).start()
        assert self.ready.wait(10)

    def _serve(self):
        asyncio.set_event_loop(self.loop)

        async def rest(request):
            self.rest_hits += 1
            if self.rest_fail > 0:
                self.rest_fail -= 1
                return web.Response(status=500)
            return web.json_response(load_fixture("s103_rest_all.json"))

        async def ws(request):
            w = web.WebSocketResponse()
            await w.prepare(request)
            self.clients.append(w)
            async for msg in w:
                if msg.type == WSMsgType.TEXT:
                    self.received.append(json.loads(msg.data))
            if w in self.clients:
                self.clients.remove(w)
            return w

        async def start():
            app = web.Application(); app.router.add_get("/rest/all", rest); app.router.add_get("/ws", ws)
            self.runner = web.AppRunner(app); await self.runner.setup()
            site = web.TCPSite(self.runner, "127.0.0.1", 0); await site.start()
            self.port = site._server.sockets[0].getsockname()[1]
            self.ready.set()
        self.loop.run_until_complete(start()); self.loop.run_forever()

    def push(self, payload):
        async def _p():
            for c in list(self.clients):
                await c.send_str(json.dumps(payload))
        asyncio.run_coroutine_threadsafe(_p(), self.loop).result(5)

    def drop_clients(self):
        async def _d():
            for c in list(self.clients):
                await c.close()
        asyncio.run_coroutine_threadsafe(_d(), self.loop).result(5)


def sets_for_led(evok):
    return [m for m in evok.received if m.get("cmd") == "set" and m.get("dev") in ("led", "ro", "do") and m.get("circuit") == "1_01"]


def wait_until(pred, timeout, step=0.5, poke=None):
    end = time.time() + timeout
    while time.time() < end:
        if poke:
            poke()
        if pred():
            return True
        time.sleep(step)
    return False


@pytest.fixture
def live_bridge(tmp_path):
    evok = FakeEvok()
    cfg = {"mqtt": {"broker": "10.255.255.1", "port": 1883},                              # nothing answers there: no broker, no network  # pii-ok (test data)
           "websocket": {"url": f"ws://127.0.0.2:{evok.port}/ws"},                         # 127.0.0.2 = "the box's own LAN address"  # pii-ok (test data)
           "unipi-http": {"url": f"http://127.0.0.2:{evok.port}/rest/all"},  # pii-ok (test data)
           "logging": {"file_path": str(tmp_path / "bridge.log"), "level": "INFO"}, "web_server": {"enabled": False},
           "extensions": {"watchdog": False}}
    (tmp_path / "config.json").write_text(json.dumps(cfg)); (tmp_path / "config.json").chmod(0o600)
    rule = dict(name="Button lights the LED", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
                action_type="set", action_dev="led", action_circuit="1_01", action_value="1")
    (tmp_path / "local_rules.json").write_text(json.dumps([rule]))
    state = tmp_path / "state"; state.mkdir()
    proc = subprocess.Popen([sys.executable, str(ROOT / "hass-unipi.py"), "--config", str(tmp_path / "config.json")],
                            env=dict(os.environ, UNIPI_HA_STATE_DIR=str(state)), stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)      # never a PIPE nobody reads: the bridge would block on a full pipe
    yield evok, proc, tmp_path
    proc.terminate()
    try:
        proc.wait(10)
    except subprocess.TimeoutExpired:
        proc.kill()


def log_tail(tmp_path, n=40):
    try:
        return "\n".join((tmp_path / "bridge.log").read_text().splitlines()[-n:])
    except OSError:
        return "(no log)"


def press_button(evok):
    """One press = 0 then 1 (the bridge ignores repeats of an unchanged value)."""
    evok.push([{"dev": "di", "circuit": "1_01", "value": 0}]); time.sleep(0.2)
    evok.push([{"dev": "di", "circuit": "1_01", "value": 1}])


def test_button_switches_the_led_with_no_network_and_no_broker_and_survives_an_evok_reconnect(live_bridge):
    evok, proc, tmp_path = live_bridge
    assert wait_until(lambda: len(evok.clients) > 0, 30), "bridge never connected to evok over loopback"
    # the rule becomes active once startup (5 s settle + discovery) is done; keep pressing until the output command arrives
    assert wait_until(lambda: len(sets_for_led(evok)) > 0, 45, step=1.5, poke=lambda: press_button(evok) if evok.clients else None), \
        "the local rule did not fire without network/broker; log tail:\n" + log_tail(tmp_path)
    assert proc.poll() is None                                                              # no restart loop, still the same process
    # evok drops the connection (like a restart of evok): the bridge reconnects by itself and the rule works again
    n = len(sets_for_led(evok))
    evok.drop_clients()
    assert wait_until(lambda: len(evok.clients) > 0, 30), "bridge did not reconnect to evok"
    assert wait_until(lambda: len(sets_for_led(evok)) > n, 30, step=1.5, poke=lambda: press_button(evok) if evok.clients else None), \
        "rule did not fire again after the evok reconnect"
    assert proc.poll() is None
    assert not (ROOT / ".device_name").exists() or (ROOT / ".device_name").stat().st_mtime < time.time() - 5     # state went to the state dir
