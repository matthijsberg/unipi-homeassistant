"""Characterization: evok WebSocket updates -> MQTT state messages (dedup, deadband, republish)."""
import json

from conftest import drain, fx

DN = "Neuron_S103_2258"


def push(b, msg, **kw):
    b.process_websocket_message(msg, **kw)
    return drain(b.websocket_to_mqtt_queue)


def test_digital_input_change_publishes_once(discovered):
    cur = fx("di", "1_01")
    new = 0 if cur == 1 else 1
    out = push(discovered, {"dev": "di", "circuit": "1_01", "value": new})
    assert out == [(f"unipi/{DN}/di/1_01/state", "ON" if new == 1 else "OFF")]
    # identical value again: filtered
    assert push(discovered, {"dev": "di", "circuit": "1_01", "value": new}) == []


def test_analog_input_deadband(discovered):
    base = fx("ai", "1_01")
    ai = lambda v: {"dev": "ai", "circuit": "1_01", "value": v}
    assert push(discovered, ai(base + 0.04)) == []       # just under the 0.05 deadband
    out = push(discovered, ai(base + 0.1))               # just over it
    assert [(t, json.loads(p)) for t, p in out] == [(f"unipi/{DN}/ai/1_01/state", {"value": base + 0.1})]


def test_analog_output_state_json(discovered):
    out = push(discovered, {"dev": "ao", "circuit": "xS51_01", "value": 5.0})
    (topic, payload), = out
    assert topic == f"unipi/{DN}/ao/xS51_01/state"
    assert json.loads(payload) == {"state": "ON", "brightness": 500, "color_mode": "brightness"}
    out = push(discovered, {"dev": "ao", "circuit": "xS51_01", "value": 0.0})
    assert json.loads(out[0][1])["state"] == "OFF"


def test_ao_echo_suppressed_during_transition(discovered):
    discovered.active_ao_transitions[("ao", "xS51_01")] = 800
    assert push(discovered, {"dev": "ao", "circuit": "xS51_01", "value": 3.0}) == []


def test_relay_state_publishes_on_off(discovered):
    cur = fx("ro", "xS51_01")
    new = 0 if cur == 1 else 1
    out = push(discovered, {"dev": "ro", "circuit": "xS51_01", "value": new})
    assert out == [(f"unipi/{DN}/ro/xS51_01/state", "ON" if new else "OFF")]


def test_onewire_deadband_and_publish(discovered):
    t = fx("1wdevice", "268CCC30020000F2", "temp")
    msg = lambda v: {"dev": "1wdevice", "circuit": "268CCC30020000F2", "temp": v}
    assert push(discovered, msg(t + 0.01)) == []
    out = push(discovered, msg(t + 1.0))
    assert out == [(f"unipi/{DN}/1-wire/268CCC30020000F2/temp", json.dumps({"value": t + 1.0}))]


def test_list_of_updates_is_processed_item_by_item(discovered):
    cur = fx("di", "1_02")
    new = 0 if cur == 1 else 1
    out = push(discovered, [{"dev": "di", "circuit": "1_02", "value": new},
                            {"dev": "di", "circuit": "1_03", "value": 1 - fx("di", "1_03")}])
    assert len(out) == 2


def test_force_republishes_unchanged_values(discovered):
    out = push(discovered, {"dev": "di", "circuit": "1_01", "value": fx("di", "1_01")}, force=True)
    assert len(out) == 1
