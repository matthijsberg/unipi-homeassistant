"""Characterization: MQTT commands -> evok WebSocket commands + acknowledgements."""
import json

import pytest
from conftest import drain, fake_message, fx, run, settle

DN = "Neuron_S103_2258"
RO = f"unipi/{DN}/ro/xS51_01/set"
AO = f"unipi/{DN}/ao/xS51_01/set"


def ws_cmds(b):
    return [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)]


@pytest.mark.parametrize("payload", ["ON", "off", "Off"])
def test_onoff_command_sends_ws_and_acks(discovered, fast_sleep, payload):
    b = discovered
    drain(b.websocket_to_mqtt_queue)
    want = 1 if payload.upper() == "ON" else 0
    b.device_states["ro_xS51_01"] = want  # evok read-back confirms
    b.on_mqtt_message(None, None, fake_message(RO, payload))
    settle(b)
    assert ws_cmds(b) == [{"cmd": "set", "dev": "ro", "circuit": "xS51_01", "value": want}]
    assert drain(b.websocket_to_mqtt_queue) == [(f"unipi/{DN}/ro/xS51_01/state", payload.upper())]


def test_retained_set_is_ignored(discovered):
    b = discovered
    b.is_first_connect = False
    b.on_mqtt_message(None, None, fake_message(RO, "ON", retain=True))
    settle(b)
    assert ws_cmds(b) == [] and drain(b.websocket_to_mqtt_queue) == []


def test_invalid_payload_is_rejected(discovered, fast_sleep):
    b = discovered
    b.on_mqtt_message(None, None, fake_message(RO, "banana"))
    settle(b)
    assert ws_cmds(b) == [] and drain(b.websocket_to_mqtt_queue) == []


def test_ao_json_brightness_instant(discovered, fast_sleep):
    b = discovered
    drain(b.websocket_to_mqtt_queue)
    b.on_mqtt_message(None, None, fake_message(AO, json.dumps({"state": "ON", "brightness": 500, "transition": 0})))
    settle(b, 0.2)
    assert ws_cmds(b) == [{"cmd": "set", "dev": "ao", "circuit": "xS51_01", "value": 5.0}]
    (t, p), = drain(b.websocket_to_mqtt_queue)
    assert t == f"unipi/{DN}/ao/xS51_01/state"
    assert json.loads(p) == {"state": "ON", "brightness": 500, "color_mode": "brightness"}


def test_ao_fade_steps_are_monotonic_and_end_on_target(discovered, fast_sleep):
    b = discovered
    b.on_mqtt_message(None, None, fake_message(AO, json.dumps({"state": "ON", "brightness": 500, "transition": 2})))
    settle(b, 0.5)
    values = [c["value"] for c in ws_cmds(b)]
    assert len(values) == 20  # 2 s at the 0.1 s minimum step
    assert values == sorted(values) and values[-1] == 5.0


def test_ao_off_without_brightness_fades_to_zero(discovered, fast_sleep):
    b = discovered
    b.device_states["ao_xS51_01"] = 4.0
    b.on_mqtt_message(None, None, fake_message(AO, json.dumps({"state": "OFF", "transition": 0})))
    settle(b, 0.2)
    assert ws_cmds(b)[-1]["value"] == 0.0


def test_ao_out_of_range_brightness_rejected(discovered, fast_sleep):
    b = discovered
    b.on_mqtt_message(None, None, fake_message(AO, json.dumps({"state": "ON", "brightness": 5000})))
    settle(b, 0.2)
    assert ws_cmds(b) == []


def test_unsupported_json_to_relay_is_rejected_not_sent_to_fade_path(discovered, fast_sleep):
    """T12 (was K1): JSON for a digital output no longer goes to the analog fade path; unsupported keys
    are rejected with an explanation and nothing is switched."""
    b = discovered
    b.on_mqtt_message(None, None, fake_message(RO, json.dumps({"state": "ON", "brightness": 1})))
    settle(b, 0.2)
    assert ws_cmds(b) == []
    out = dict(drain(b.websocket_to_mqtt_queue))
    assert "brightness" in json.loads(out[f"unipi/{DN}/ro/xS51_01/attributes"])["last_error"]


def test_topic_split(discovered):
    parts = discovered.mqtt_split_items_dataclass(RO, "ON")
    assert (parts.dev, parts.circuit, parts.cmd, parts.payload) == ("ro", "xS51_01", "set", "ON")


@pytest.mark.parametrize("raw,expected", [
    ("ON", ("ON", "onoff")),
    ("off", ("OFF", "onoff")),
    ("42", ({"brightness": 42}, "json")),  # a bare int is treated as a brightness command
    ('{"a": 1}', ({"a": 1}, "json")),
    ("1.5", (1.5, "json")),
    ("hello", ("hello", "string")),
])
def test_process_payload(bridge, raw, expected):
    assert bridge.process_payload(raw) == expected
