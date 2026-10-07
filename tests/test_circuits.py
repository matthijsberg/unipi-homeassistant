"""T11: per-circuit configuration, names/device classes, logical inversion, rules path."""
import json
import os
from types import SimpleNamespace

import pytest
from conftest import drain, fx, run
from pydantic import ValidationError

from unipi_core.circuits import canonical_key

DN = "Neuron_S103_2258"


def make_config(mod, tmp_path, **extra):
    return mod.AppConfig(
        mqtt={"broker": "mqtt.invalid"}, websocket={"url": "ws://127.0.0.1:1/ws"},
        unipi_http={"url": "http://127.0.0.1:1/rest/all"},
        logging={"file_path": str(tmp_path / "b.log")}, web_server={"enabled": False}, **extra)


# ---- config validation -------------------------------------------------------------------------
@pytest.mark.parametrize("key,expected", [
    ("di/1_01", "di/1_01"), ("input/1_01", "di/1_01"), ("relay/2_02", "ro/2_02"),
    ("analogoutput/3_01", "ao/3_01"), ("output/1_01", "do/1_01"),
    ("1wdevice/28D1EFA708000052/temp", "1wdevice/28D1EFA708000052/temp"),
])
def test_canonical_keys(key, expected):
    assert canonical_key(key) == expected


@pytest.mark.parametrize("key", ["1_01", "foo/1_01", "di/", "di/1_01/temp", "1wdevice/abc/nonsense", "a/b/c/d"])
def test_bad_keys_rejected_and_named(mod, tmp_path, key):
    with pytest.raises(ValidationError) as e:
        make_config(mod, tmp_path, circuits={key: {"name": "x"}})
    assert key in str(e.value)


def test_alias_duplicates_rejected(mod, tmp_path):
    with pytest.raises(ValidationError) as e:
        make_config(mod, tmp_path, circuits={"di/1_01": {}, "input/1_01": {}})
    assert "configured twice" in str(e.value)


@pytest.mark.parametrize("circuit,opts,msg", [
    ("di/1_01", {"area": "Hal"}, "suggested_area"),
    ("di/1_01", {"off_delay_s": 20}, "planned (T14)"),
    ("ro/xS51_01", {"presets": {}}, "planned"),
    ("di/1_01", {"bogus": 1}, "bogus"),
    ("ro/xS51_01", {"inverted": True}, "only applies to digital inputs"),
    ("led/1_01", {"device_class": "motion"}, "not supported for led"),
])
def test_unsupported_options_fail_loudly(mod, tmp_path, circuit, opts, msg):
    with pytest.raises(ValidationError) as e:
        make_config(mod, tmp_path, circuits={circuit: opts})
    assert msg in str(e.value)


def test_valid_config_is_canonicalised(mod, tmp_path):
    cfg = make_config(mod, tmp_path, circuits={"input/1_01": {"name": "Hal PIR", "device_class": "motion"}})
    assert list(cfg.circuits) == ["di/1_01"] and cfg.circuits["di/1_01"].name == "Hal PIR"


# ---- discovery: names and device classes -------------------------------------------------------
def build(bridge, **circuits):
    bridge.config.circuits = bridge.config.__class__(**{**bridge.config.model_dump(by_alias=False), "circuits": circuits}).circuits
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    return dict(drain(bridge.websocket_to_mqtt_queue))


def test_name_and_device_class_in_discovery(bridge):
    m = build(bridge, **{"di/1_01": {"name": "Hal PIR", "device_class": "motion"},
                         "ai/1_01": {"name": "Hal lux", "device_class": "illuminance"},
                         "1wdevice/268CCC30020000F2/temp": {"name": "Bijkeuken temperatuur"}})
    di = json.loads(m[f"homeassistant/binary_sensor/{DN}/di_1_01/config"])
    assert (di["name"], di["device_class"]) == ("Hal PIR", "motion")
    assert di["unique_id"] == f"{DN}_di_1_01"  # identity never depends on the display name
    ai = json.loads(m[f"homeassistant/sensor/{DN}/ai_1_01/config"])
    assert (ai["name"], ai["device_class"]) == ("Hal lux", "illuminance")
    ow = json.loads(m[f"homeassistant/sensor/{DN}/1-wire_268CCC30020000F2_temp/config"])
    assert ow["name"] == "Bijkeuken temperatuur" and ow["device_class"] == "temperature"
    other = json.loads(m[f"homeassistant/binary_sensor/{DN}/di_1_02/config"])
    assert other["name"] == "di 1_02" and "device_class" not in other  # untouched circuits unchanged


def test_unknown_device_class_only_warns(bridge):
    warnings = []
    bridge.circuits._log = SimpleNamespace(warning=warnings.append)
    m = build(bridge, **{"di/1_01": {"device_class": "motionn"}})   # no exception: HA may add classes later
    assert json.loads(m[f"homeassistant/binary_sensor/{DN}/di_1_01/config"])["device_class"] == "motionn"
    assert len(warnings) == 1 and "motionn" in warnings[0]


# ---- logical inversion -------------------------------------------------------------------------
def test_inversion_applies_to_state_cache_rules_and_events(discovered, mod):
    b = discovered
    b.config.inputs = {"1_01": {"inverted": True}}
    events = []
    b.events.subscribe("input_changed", lambda **d: events.append(d))
    b.device_states["di_1_01"] = 0
    b.local_logic.rules = [mod.LocalLogicRule(name="t", trigger_dev="di", trigger_circuit="1_01", trigger_value=1,
                                              action_dev="ro", action_circuit="xS51_01", action_value=1)]
    drain(b.websocket_to_mqtt_queue)
    # physical contact goes 1 -> 0 (e.g. an NC contact opening); logical value becomes 1
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 0})
    assert b.device_states["di_1_01"] == 1
    assert (events[0]["value"], events[0]["raw"]) == (1, 0)
    assert dict(drain(b.websocket_to_mqtt_queue))[f"unipi/{DN}/di/1_01/state"] == "ON"
    assert [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)] == [1]  # rule fired on LOGICAL 1


@pytest.mark.parametrize("physical", [0, 1])
def test_home_assistant_sees_the_same_state_as_before_t11(bridge, physical):
    """Old code: raw state published + swapped payloads (on = 'OFF'). New code: logical state + plain
    payloads. What HA displays must be identical for every contact position."""
    bridge.config.inputs = {"1_01": {"inverted": True}}
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    drain(bridge.websocket_to_mqtt_queue)
    bridge.device_states["di_1_01"] = physical  # cache holds LOGICAL values: this differs from the new logical value (1 - physical)
    bridge.process_websocket_message({"dev": "di", "circuit": "1_01", "value": physical})
    payload = dict(drain(bridge.websocket_to_mqtt_queue))[f"unipi/{DN}/di/1_01/state"]
    ha_on_new = payload == "ON"
    old_payload = "ON" if physical == 1 else "OFF"          # raw state topic of v2.x
    ha_on_old = old_payload == "OFF"                        # old discovery: payload_on = "OFF"
    assert ha_on_new == ha_on_old


def test_circuits_setting_and_ui_precedence(discovered, mod, tmp_path):
    b = discovered
    b.config.circuits = make_config(mod, tmp_path, circuits={"di/1_01": {"inverted": True}}).circuits
    assert b.circuits.is_inverted("di", "1_01") is True and b.circuits.is_inverted("input", "1_01") is True
    assert b.circuits.is_inverted("di", "1_02") is False
    b.config.inputs = {"1_01": {"inverted": False}}      # the web UI's runtime switch wins
    assert b.circuits.is_inverted("di", "1_01") is False


# ---- web handlers -------------------------------------------------------------------------------
def test_web_toggle_keeps_ha_state_consistent(discovered, tmp_path):
    b = discovered
    (tmp_path / "config.json").write_text("{}")
    physical = fx("di", "1_01")
    drain(b.websocket_to_mqtt_queue)
    req = SimpleNamespace(match_info={"circuit": "1_01"}, json=lambda: _aval({"inverted": True}))
    resp = run(b, b.web_handler_update_input(req))
    assert json.loads(resp.text)["inverted"] is True
    out = dict(drain(b.websocket_to_mqtt_queue))
    assert out[f"unipi/{DN}/di/1_01/state"] == ("OFF" if physical == 1 else "ON")   # logical, inverted
    assert b.device_states["di_1_01"] == 1 - physical
    assert json.loads((tmp_path / "config.json").read_text())["inputs"]["1_01"] == {"inverted": True}
    inputs = json.loads(run(b, b.web_handler_get_inputs(SimpleNamespace())).text)
    row = [r for r in inputs if r["circuit"] == "1_01"][0]
    assert (row["inverted"], row["value"]) == (True, physical)       # screen shows the physical contact


async def _aval(v):
    return v


# ---- K6: rules file next to the config file ------------------------------------------------------
def test_rules_file_lives_next_to_config_not_cwd(mod, tmp_path, monkeypatch):
    cfgdir = tmp_path / "etc"; cfgdir.mkdir()
    elsewhere = tmp_path / "elsewhere"; elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)
    monkeypatch.setattr(mod, "DEVICE_NAME_CACHE_FILE", str(tmp_path / ".device_name"))
    b = mod.UnipiBridge(make_config(mod, tmp_path), str(cfgdir / "config.json"))
    try:
        assert os.path.dirname(b.local_logic.rules_file) == str(cfgdir)
    finally:
        b.loop.close()


def test_state_cache_is_seeded_with_logical_values_at_startup(bridge, mod):
    """Rule conditions read device_states, so an inverted input must be logical from the very first snapshot."""
    bridge.config.inputs = {"1_01": {"inverted": True}}
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    assert bridge.device_states["di_1_01"] == 1 - fx("di", "1_01")
    assert bridge.device_states["di_1_02"] == fx("di", "1_02")        # others untouched
    cond = mod.RuleCondition(dev="di", circuit="1_01", operator="eq", value=1 - fx("di", "1_01"))
    rule = mod.LocalLogicRule(name="t", trigger_dev="di", trigger_circuit="1_02", trigger_operator="any",
                              conditions=[cond], action_dev="ro", action_circuit="xS51_01", action_value=1)
    bridge.local_logic.rules = [rule]
    assert len(bridge.local_logic.evaluate({"dev": "di", "circuit": "1_02", "value": 1}, bridge.device_states)) == 1
