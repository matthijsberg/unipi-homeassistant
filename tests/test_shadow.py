"""T18: shadow mode - a read-only twin that can run next to the live bridge."""
import json
import threading
import time
from types import SimpleNamespace

import pytest
from conftest import FakeMqtt, FakeWebSocket, drain, fake_message, load_fixture, run, settle

DN = "Neuron_S103_2258"
SDN = DN + "_shadow"


class Recorder:
    """Stands in for the bridge logger so we can assert on SHADOW lines (the bridge resets root handlers)."""
    def __init__(self):
        self.lines = []

    def __getattr__(self, level):
        return lambda msg, *a, **k: self.lines.append(f"{level}: {msg}")


def make(mod, tmp_path, monkeypatch, mode, name, **extra):
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    d = tmp_path / name
    d.mkdir()
    cfg = mod.AppConfig(mqtt={"broker": "mqtt.invalid", "topic": "unipi"}, websocket={"url": "ws://127.0.0.1:1/ws"},
                        unipi_http={"url": "http://127.0.0.1:1/rest/all"}, logging={"file_path": str(d / "b.log")},
                        web_server={"enabled": True}, mode=mode, **extra)
    b = mod.UnipiBridge(cfg, str(d / "config.json"))
    b.mqtt_client, b.websocket_connection = FakeMqtt(), FakeWebSocket(mod)
    data = load_fixture("s103_rest_all.json")

    async def rest(dev=None, circuit=None, scope=None):
        if scope == "all":
            return data
        return {"value": b.device_states.get(f"{dev}_{circuit}", 0)} if scope == "value" else None
    b.get_unipi_data = rest
    return b


@pytest.fixture
def shadow(mod, tmp_path, monkeypatch):
    b = make(mod, tmp_path, monkeypatch, "shadow", "shadow")
    run(b, b.perform_discovery_and_mqtt_subscribe())
    drain(b.websocket_to_mqtt_queue)
    b.logger = Recorder()
    yield b
    b.loop.close()


def pump(b):
    """Run the real MQTT worker thread until the queue is empty; return what reached the (fake) broker."""
    t = threading.Thread(target=b.mqtt_worker_thread, daemon=True); t.start()
    for _ in range(100):
        if b.websocket_to_mqtt_queue.unfinished_tasks == 0:
            break
        time.sleep(0.05)
    b.should_stop.set(); t.join(3); b.should_stop.clear()
    return b.mqtt_client.published


# ---- identity and isolation --------------------------------------------------------------------------------
def test_mode_defaults_to_live_and_validates(mod, tmp_path, monkeypatch):
    base = dict(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"})
    assert mod.AppConfig(**base).mode == "live"
    with pytest.raises(Exception):
        mod.AppConfig(**base, mode="shaddow")
    cfg_file = tmp_path / "c.json"; cfg_file.write_text(json.dumps(base))
    monkeypatch.setenv("UNIPI_MODE", "shadow")
    assert mod.AppConfig.load_from_env_and_file(str(cfg_file)).mode == "shadow"


def test_shadow_gets_its_own_names_and_never_touches_the_live_name_cache(mod, tmp_path, monkeypatch):
    (tmp_path / ".device_name").write_text(DN)
    b = make(mod, tmp_path, monkeypatch, "shadow", "s")
    assert b.device_name == ""                                           # does not read the live cache
    run(b, b.perform_discovery_and_mqtt_subscribe())
    assert b.device_name == SDN
    assert (tmp_path / ".device_name").read_text() == DN                 # and does not overwrite it
    assert b.mqtt_client.will[0] == f"unipi/{SDN}/status"
    assert all(t.startswith(f"unipi/{SDN}/") for t in b.mqtt_subscribe_topics)
    b.loop.close()


def test_shadow_state_publishing_keeps_working_under_shadow_topics(shadow):
    shadow.device_states["di_1_01"] = 0
    shadow.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    assert drain(shadow.websocket_to_mqtt_queue) == [(f"unipi/{SDN}/di/1_01/state", "ON")]


def test_no_web_ui_in_shadow(mod, tmp_path, monkeypatch):
    live = make(mod, tmp_path, monkeypatch, "live", "l")
    sh = make(mod, tmp_path, monkeypatch, "shadow", "s")
    assert live._web_server_enabled() is True and sh._web_server_enabled() is False
    live.loop.close(); sh.loop.close()


# ---- Home Assistant discovery -------------------------------------------------------------------------------
def test_no_discovery_configs_leave_a_shadow_by_default(shadow):
    run(shadow, shadow.perform_discovery_and_mqtt_subscribe())
    published = pump(shadow)
    assert published and not [t for t, *_ in published if t.endswith("/config")]
    assert [t for t, *_ in published if t.startswith(f"unipi/{SDN}/")]            # states still flow


def test_shadow_discovery_cannot_collide_with_the_live_entities(mod, tmp_path, monkeypatch):
    live = make(mod, tmp_path, monkeypatch, "live", "l")
    sh = make(mod, tmp_path, monkeypatch, "shadow", "s", shadow_discovery=True)
    for b in (live, sh):
        run(b, b.perform_discovery_and_mqtt_subscribe())
    lp, sp = pump(live), pump(sh)
    live_cfg = {t: json.loads(p) for t, p, *_ in lp if t.endswith("/config")}
    sh_cfg = {t: json.loads(p) for t, p, *_ in sp if t.endswith("/config")}
    assert live_cfg and len(sh_cfg) == len(live_cfg)
    assert not set(live_cfg) & set(sh_cfg)                                          # no shared discovery topic: nothing is overwritten
    assert not {c["unique_id"] for c in live_cfg.values()} & {c["unique_id"] for c in sh_cfg.values()}
    assert not {tuple(c["dev"]["identifiers"]) for c in live_cfg.values()} & {tuple(c["dev"]["identifiers"]) for c in sh_cfg.values()}
    di = sh_cfg[f"homeassistant/binary_sensor/{SDN}/di_1_01/config"]
    assert di["name"].endswith("(shadow)") and di["state_topic"] == f"unipi/{SDN}/di/1_01/state"
    assert all(c["availability_topic"].startswith(f"unipi/{SDN}/") for c in sh_cfg.values() if "availability_topic" in c)
    live.loop.close(); sh.loop.close()


# ---- the safety claim: nothing is ever written to evok ------------------------------------------------------------
def test_no_command_path_writes_to_evok(shadow, mod, fast_sleep):
    b = shadow
    cfg = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                        circuits={"led/1_01": {"failsafe_off": True, "max_on_s": 5,
                                               "presets": {"ring": {"pulse": {"count": 2}}}}})
    b.config.circuits = cfg.circuits
    rules = [
        dict(name="set", trigger_circuit="1_01", action_type="set", action_dev="ro", action_circuit="xS51_01", action_value=1),
        dict(name="tog", trigger_circuit="1_01", action_type="toggle", action_dev="led", action_circuit="1_01"),
        dict(name="pulse", trigger_circuit="1_01", action_type="pulse", action_dev="led", action_circuit="1_01", action_preset="ring"),
        dict(name="dim", trigger_circuit="1_01", trigger_operator="any", action_type="dimmer", action_dev="ao", action_circuit="xS51_01", dimmer_hold=False, action_value=5),
    ]
    b.local_logic.rules = [mod.LocalLogicRule(trigger_dev="di", trigger_operator=r.pop("trigger_operator", "eq"), trigger_value=1, **r) for r in rules]
    for topic, payload in [
        (f"unipi/{SDN}/ro/xS51_01/set", "ON"), (f"unipi/{SDN}/ro/xS51_01/set", "OFF"),
        (f"unipi/{SDN}/ao/xS51_01/set", json.dumps({"state": "ON", "brightness": 500, "transition": 0})),
        (f"unipi/{SDN}/led/1_01/set", json.dumps({"pulse": {"count": 2}})),
        (f"unipi/{SDN}/led/1_01/set", json.dumps({"state": "ON", "duration_s": 3})),
        (f"unipi/{SDN}/led/1_01/set", json.dumps({"preset": "ring"})),
        (f"unipi/{SDN}/led/1_01/set", "OFF"),
    ]:
        b.on_mqtt_message(None, None, fake_message(topic, payload))
        settle(b, 0.05)
    b.device_states["di_1_01"] = 0
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})        # fires all four rules
    settle(b, 0.2)
    assert b.commands.send_ws("ro", "xS51_01", 1) is not None
    run(b, b.sequencer.failsafe_all([("led", "1_01")]))
    run(b, b._ws_write_now("ro", "xS51_01", 1))
    run(b, b.send_to_websocket('{"cmd": "set", "dev": "ro", "circuit": "xS51_01", "value": 1}'))
    run(b, b.commands.set_digital("ro", "xS51_01", True))
    run(b, b.commands.transition("ao", "xS51_01", 500, 0)); settle(b, 0.2)
    assert b.websocket_connection.sent == []                                          # nothing reached evok
    assert drain(b.mqtt_to_websocket_queue) == []                                     # nothing is even queued for it
    log = "\n".join(b.logger.lines)
    assert "SHADOW: would write" in log and "SHADOW: rule action would run" in log and "SHADOW: would queue" in log


def test_rules_still_see_inputs_in_shadow_but_only_log(shadow, mod):
    shadow.local_logic.rules = [mod.LocalLogicRule(name="r", trigger_dev="di", trigger_circuit="1_01", trigger_value=1,
                                                   action_dev="ro", action_circuit="xS51_01", action_value=1)]
    shadow.device_states["di_1_01"] = 0
    shadow.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    assert shadow.device_states["di_1_01"] == 1
    assert any("rule action would run" in l for l in shadow.logger.lines)
    assert drain(shadow.mqtt_to_websocket_queue) == [] and shadow.websocket_connection.sent == []


def test_a_live_bridge_is_unaffected(mod, tmp_path, monkeypatch):
    live = make(mod, tmp_path, monkeypatch, "live", "l")
    run(live, live.perform_discovery_and_mqtt_subscribe())
    assert live.shadow is False and live.device_name == DN
    live.commands.send_ws("ro", "xS51_01", 1)
    assert len(drain(live.mqtt_to_websocket_queue)) == 1
    run(live, live._ws_write_now("ro", "xS51_01", 1))
    assert len(live.websocket_connection.sent) == 1
    live.loop.close()


# ---- tools/shadow_compare.py ---------------------------------------------------------------------------------------------
def test_shadow_compare_reports_differences():
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location("shadow_compare", Path(__file__).resolve().parent.parent / "tools" / "shadow_compare.py")
    sc = importlib.util.module_from_spec(spec); spec.loader.exec_module(sc)
    r = sc.diff_states({"di/1_01/state": "ON", "ai/1_01/state": "1", "ro/x/state": "OFF"},
                       {"di/1_01/state": "OFF", "ai/1_01/state": "1", "do/y/state": "ON"})
    assert r["same"] == 1 and r["different"] == {"di/1_01/state": ("ON", "OFF")}
    assert r["only_live"] == ["ro/x/state"] and r["only_shadow"] == ["do/y/state"]


def test_example_service_runs_in_shadow_mode_and_example_config_is_valid(mod):
    from pathlib import Path
    root = Path(__file__).resolve().parent.parent
    unit = (root / "tools" / "hass-unipi-shadow.service.example").read_text()
    assert "UNIPI_MODE=shadow" in unit and "WorkingDirectory=/home/unipi/shadow" in unit and "--config /home/unipi/shadow/config.json" in unit
    cfg = json.loads((root / "config.shadow.example.json").read_text())
    app = mod.AppConfig(**cfg)
    assert app.mode == "shadow" and app.shadow_discovery is False and app.web_server.enabled is False


def test_clear_tool_can_only_ever_match_shadow_topics():
    import importlib.util
    from pathlib import Path
    spec = importlib.util.spec_from_file_location("cst", Path(__file__).resolve().parent.parent / "tools" / "clear_shadow_topics.py")
    cst = importlib.util.module_from_spec(spec); spec.loader.exec_module(cst)
    for t in (f"unipi/{SDN}/di/1_01/state", f"homeassistant/sensor/{SDN}/di_1_04_counter/config",
              f"homeassistant/binary_sensor/{SDN}_bridge_startup_error/config", f"unipi/{SDN}_bridge/startup_error"):
        assert cst.is_shadow_topic(t), t
    for t in (f"unipi/{DN}/di/1_01/state", f"homeassistant/sensor/{DN}/di_1_04_counter/config", "unipi/shadowfax/x",
              f"unipi/{DN}_bridge/startup_error", "homeassistant/status", "unipi/other_shadowless/state"):
        assert not cst.is_shadow_topic(t), t
