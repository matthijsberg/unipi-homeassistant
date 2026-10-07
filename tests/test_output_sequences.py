"""T12 end to end inside the bridge: MQTT JSON -> sequencer -> WebSocket writes, acks, attributes, safety."""
import json
import os

import pytest
from conftest import REAL_SLEEP, drain, fake_message, fx, run, settle
from pydantic import ValidationError
from test_sequencer import FakeTime

DN = "Neuron_S103_2258"
BELL = f"unipi/{DN}/led/1_01"           # a safe front-panel output stands in for the bell relay


def configure(b, mod, **led):
    cfg = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                        circuits={"led/1_01": led})
    b.config.circuits = cfg.circuits


@pytest.fixture
def rig(discovered, mod):
    b = discovered
    ft = FakeTime()
    b.sequencer.clock, b.sequencer.sleep = ft.clock, ft.sleep
    configure(b, mod, max_on_s=5, max_count=4, failsafe_off=True,
              presets={"ring_front": {"label": "Bel (3x)", "pulse": {"count": 3}}})
    drain(b.websocket_to_mqtt_queue)
    b.ft = ft
    b.ws_writes = lambda: [(round(t, 3), json.loads(m)["value"]) for t, m in zip(b.times, b.websocket_connection.sent)]
    b.times = []
    real_send = b.websocket_connection.send

    async def timed_send(m):
        b.times.append(ft.t)
        await real_send(m)
    b.websocket_connection.send = timed_send
    return b


def send(b, payload, topic=BELL + "/set"):
    b.on_mqtt_message(None, None, fake_message(topic, payload if isinstance(payload, str) else json.dumps(payload)))


def mqtt_out(b):
    return [(t.replace(f"unipi/{DN}/", ""), p) for t, p in drain(b.websocket_to_mqtt_queue)]


def test_doorbell_message_rings_three_times_with_the_requested_timing(rig):
    send(rig, {"pulse": {"count": 3, "on_ms": 100, "off_ms": 250}})
    run(rig, rig.ft.advance_to(2))
    assert rig.ws_writes() == [(0.0, 1), (0.1, 0), (0.35, 1), (0.45, 0), (0.7, 1), (0.8, 0)]
    states = [p for t, p in mqtt_out(rig) if t == "led/1_01/state"]
    assert states[0] == "ON" and states[-1] == "OFF"                       # HA: ON while ringing, OFF when done


def test_preset_and_duration_commands(rig):
    send(rig, {"preset": "ring_front"})
    run(rig, rig.ft.advance_to(2))
    assert [v for _, v in rig.ws_writes()] == [1, 0, 1, 0, 1, 0]
    rig.websocket_connection.sent.clear(); rig.times.clear()
    send(rig, {"state": "ON", "duration_s": 3})
    run(rig, rig.ft.advance_to(10))
    assert rig.ws_writes() == [(2.0, 1), (5.0, 0)]


def test_limits_reject_and_report_without_switching(rig):
    for bad in ({"pulse": {"count": 5}}, {"state": "ON", "duration_s": 60}, {"preset": "nope"}):
        send(rig, bad)
        run(rig, rig.ft.advance_to(1))
    assert rig.websocket_connection.sent == []
    errs = [json.loads(p)["last_error"] for t, p in mqtt_out(rig) if t.endswith("/attributes")]
    assert len(errs) == 3 and "count 5" in errs[0] and "duration_s" in errs[1] and "unknown preset" in errs[2]


def test_not_scheduled_when_websocket_is_down(rig, mod):
    rig.websocket_connection.state = mod.State.CLOSED
    send(rig, {"pulse": {"count": 1}})
    run(rig, rig.ft.advance_to(1))
    assert rig.websocket_connection.sent == []
    assert "not open" in [json.loads(p) for t, p in mqtt_out(rig) if t.endswith("/attributes")][0]["last_error"]


def test_plain_off_cancels_a_running_sequence_through_off(rig):
    send(rig, {"state": "ON", "duration_s": 4})
    run(rig, rig.ft.advance_to(1))
    rig.device_states["led_1_01"] = 0
    send(rig, "OFF")
    run(rig, rig.ft.advance_to(8))
    assert rig.ws_writes() == [(0.0, 1), (1.0, 0)]             # the OFF happens NOW (t=1, the cancel), not at the natural end (t=4)
    assert [json.loads(m)["value"] for m in drain(rig.mqtt_to_websocket_queue)] == [0]   # then the plain OFF via the normal queue
    assert not rig.sequencer.is_running("led", "1_01")


def test_state_echo_is_suppressed_while_a_sequence_runs(rig):
    send(rig, {"state": "ON", "duration_s": 4})
    run(rig, rig.ft.advance_to(1))
    drain(rig.websocket_to_mqtt_queue)
    rig.process_websocket_message({"dev": "led", "circuit": "1_01", "value": 1})
    assert [t for t, _ in mqtt_out(rig) if t == "led/1_01/state"] == []
    run(rig, rig.ft.advance_to(9))
    drain(rig.websocket_to_mqtt_queue)
    rig.process_websocket_message({"dev": "led", "circuit": "1_01", "value": 0})
    assert [p for t, p in mqtt_out(rig) if t == "led/1_01/state"] == ["OFF"]   # normal again afterwards


def test_ws_echo_feeds_the_watchdog_and_it_forces_off(rig):
    rig.process_websocket_message({"dev": "led", "circuit": "1_01", "value": 1})   # something else switched it on
    run(rig, rig.ft.advance_to(5.5)); run(rig, rig.sequencer.watchdog_tick())
    assert [v for _, v in rig.ws_writes()] == [0]
    assert any("max_on_s" in json.loads(p).get("last_error", "") for t, p in mqtt_out(rig) if t.endswith("/attributes"))


def test_failsafe_outputs_are_driven_off_at_shutdown_and_reconnect(rig):
    assert rig.circuits.failsafe_keys() == [("led", "1_01")]
    run(rig, rig.sequencer.failsafe_all(rig.circuits.failsafe_keys(), "fail-safe after WebSocket (re)connect"))
    assert [v for _, v in rig.ws_writes()] == [0]
    send(rig, {"state": "ON", "duration_s": 4})
    run(rig, rig.ft.advance_to(1))
    rig.websocket_connection.sent.clear(); rig.times.clear()
    run(rig, rig._shutdown_outputs())
    assert [v for _, v in rig.ws_writes()] == [0, 0]            # sequence cancelled (OFF) + fail-safe OFF


def test_discovery_adds_attributes_topic_only_for_configured_outputs(bridge, mod):
    from conftest import run as _run
    configure(bridge, mod, failsafe_off=True)
    _run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    m = dict(drain(bridge.websocket_to_mqtt_queue))
    configured = json.loads(m[f"homeassistant/light/{DN}/led_1_01/config"])
    other = json.loads(m[f"homeassistant/light/{DN}/led_1_02/config"])
    assert configured["json_attributes_topic"] == f"unipi/{DN}/led/1_01/attributes"
    assert "json_attributes_topic" not in other


def test_analog_json_is_still_routed_to_the_fade_path(discovered, fast_sleep):
    discovered.on_mqtt_message(None, None, fake_message(f"unipi/{DN}/ao/xS51_01/set", json.dumps({"state": "ON", "brightness": 500, "transition": 0})))
    settle(discovered, 0.2)
    assert json.loads(discovered.websocket_connection.sent[-1] if discovered.websocket_connection.sent else
                      drain(discovered.mqtt_to_websocket_queue)[-1])["value"] == 5.0


# ---- config validation -----------------------------------------------------------------------------
def make(mod, **c):
    return mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"}, circuits=c)


@pytest.mark.parametrize("circuit,opts,msg", [
    ("di/1_01", {"failsafe_off": True}, "only apply to digital outputs"),
    ("ao/1_01", {"presets": {}}, "only apply to digital outputs"),
    ("ro/2_02", {"max_count": 2, "presets": {"x": {"pulse": {"count": 3}}}}, "count 3"),
    ("ro/2_02", {"max_on_s": 2, "presets": {"x": {"timed": {"state": "ON", "duration_s": 9}}}}, "duration_s"),
    ("ro/2_02", {"presets": {"Bad Name": {"pulse": {"count": 1}}}}, "preset name"),
    ("ro/2_02", {"presets": {"x": {}}}, "exactly one"),
    ("ro/2_02", {"max_count": 0}, "greater than or equal"),
    ("ro/2_02", {"pulse_defaults": {"on_ms": 5}}, "on_ms"),
])
def test_invalid_sequencer_settings_fail_at_startup(mod, circuit, opts, msg):
    with pytest.raises(ValidationError) as e:
        make(mod, **{circuit: opts})
    assert msg in str(e.value)


def test_valid_doorbell_config_loads(mod):
    cfg = make(mod, **{"relay/2_02": {"name": "Bel", "failsafe_off": True, "max_on_s": 2, "max_count": 6,
                                      "pulse_defaults": {"on_ms": 100, "off_ms": 250},
                                      "presets": {"ring_back": {"label": "Bel achter (2x)", "pulse": {"count": 2}},
                                                  "ring_front": {"label": "Bel voor (3x)", "pulse": {"count": 3}}}}})
    c = cfg.circuits["ro/2_02"]
    assert c.limits().max_count == 6 and c.limits().watchdog and set(c.presets) == {"ring_back", "ring_front"}
