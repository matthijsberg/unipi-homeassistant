"""Offline test harness for hass-unipi.py.

Everything external is faked: MQTT (FakeMqtt), the evok WebSocket (FakeWebSocket) and the
evok REST API (patched get_unipi_data). Tests are synchronous; async bridge methods are run
on the bridge's own loop with `run(bridge, coro)`. Nothing here may touch the live runtime
dir: the cached device-name file and the working directory (for local_rules.json) are
redirected to a tmp dir.
"""
from __future__ import annotations

import asyncio
import importlib.util
import json
import logging
import os
import queue
from pathlib import Path
from types import SimpleNamespace

import sys

import pytest

REAL_SLEEP = asyncio.sleep
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))  # lets hass-unipi.py import unipi_core, as when run from its directory
FIXTURES = Path(__file__).resolve().parent / "fixtures"


@pytest.fixture(scope="session")
def mod():
    spec = importlib.util.spec_from_file_location("hass_unipi", ROOT / "hass-unipi.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


class FakeMqtt:
    """Stands in for paho's Client: records everything, sends nothing."""

    def __init__(self):
        self.published: list[tuple[str, str, int, bool]] = []
        self.subscribed: list[str] = []
        self.will = None
        self.connected = False

    def publish(self, topic, payload=None, qos=0, retain=False):
        self.published.append((topic, payload, qos, retain))
        return SimpleNamespace(wait_for_publish=lambda timeout=None: None)

    def subscribe(self, topic, qos=0):
        self.subscribed.append(topic)
        return (0, 1)

    def will_set(self, topic, payload=None, qos=0, retain=False):
        self.will = (topic, payload, qos, retain)

    def is_connected(self):
        return self.connected

    def username_pw_set(self, *a, **k): ...
    def reconnect_delay_set(self, *a, **k): ...
    def connect_async(self, *a, **k): ...
    def loop_start(self): ...
    def loop_stop(self): ...
    def disconnect(self): ...


class FakeWebSocket:
    def __init__(self, mod):
        self.state = mod.State.OPEN
        self.sent: list[str] = []

    async def send(self, message):
        self.sent.append(message)


def drain(q: queue.Queue) -> list:
    out = []
    while True:
        try:
            out.append(q.get_nowait())
        except queue.Empty:
            return out


def run(bridge, coro):
    """Run a coroutine on the bridge's own event loop (tests are synchronous)."""
    return bridge.loop.run_until_complete(coro)


def load_fixture(name: str):
    return json.loads((FIXTURES / name).read_text())


@pytest.fixture
def bridge(mod, tmp_path, monkeypatch):
    # Never read/write the live runtime dir.
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    monkeypatch.chdir(tmp_path)  # local_rules.json is resolved relative to CWD (known issue K6)
    cfg = mod.AppConfig(
        mqtt={"broker": "mqtt.invalid", "topic": "unipi"},
        websocket={"url": "ws://127.0.0.1:1/ws"},
        unipi_http={"url": "http://127.0.0.1:1/rest/all"},
        logging={"file_path": str(tmp_path / "bridge.log")},
        web_server={"enabled": False},
    )
    b = mod.UnipiBridge(cfg, str(tmp_path / "config.json"))
    b.mqtt_client = FakeMqtt()
    b.websocket_connection = FakeWebSocket(mod)
    # Discovery data comes from the recorded S103 snapshot instead of REST.
    data = load_fixture("s103_rest_all.json")

    async def fake_get_unipi_data(dev=None, circuit=None, scope=None):
        if scope == "all":
            return data
        if scope in ("value", "circuit") and dev and circuit:
            for item in data:
                if item.get("dev") == dev and item.get("circuit") == circuit:
                    return {"value": b.device_states.get(f"{dev}_{circuit}", item.get("value"))}
        return None

    b.get_unipi_data = fake_get_unipi_data
    yield b
    # Release file handles / loop so tests don't leak between each other.
    for h in list(logging.getLogger().handlers):
        h.close()
        logging.getLogger().removeHandler(h)
    if not b.loop.is_closed():
        b.loop.close()
    asyncio.set_event_loop(None)


@pytest.fixture
def discovered(bridge):
    """Bridge after initial discovery on the S103 snapshot; queue drained into a dict."""
    name = run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    assert name
    msgs = {}
    for topic, payload in drain(bridge.websocket_to_mqtt_queue):
        msgs[topic] = payload
    bridge.discovered_messages = msgs
    return bridge


def settle(bridge, seconds: float = 0.05):
    """Let scheduled tasks on the bridge loop run for a moment (real time)."""
    bridge.loop.run_until_complete(REAL_SLEEP(seconds))


@pytest.fixture
def fast_sleep(monkeypatch):
    """asyncio.sleep(x) -> yields once, never waits. Makes fades/verification instant."""

    async def _fast(delay=0, result=None):
        await REAL_SLEEP(0)
        return result

    monkeypatch.setattr(asyncio, "sleep", _fast)


def fx(dev: str, circuit: str, key: str = "value"):
    """Value of a circuit in the recorded S103 snapshot."""
    for item in load_fixture("s103_rest_all.json"):
        if item.get("dev") == dev and item.get("circuit") == circuit:
            return item[key]
    raise KeyError((dev, circuit))


def fake_message(topic: str, payload: str, retain: bool = False):
    return SimpleNamespace(topic=topic, payload=payload.encode(), retain=retain)
