"""EventBus + CommandService (T10): the internal API used by later features."""
import asyncio
import json
import queue
import threading

import pytest
from conftest import REAL_SLEEP, drain, fx, run, settle

from unipi_core.events import AVAILABILITY, INPUT_CHANGED, OUTPUT_CHANGED, EventBus

DN = "Neuron_S103_2258"


def collect(bridge, kind):
    got = []
    bridge.events.subscribe(kind, lambda **d: got.append(d))
    return got


# ---- EventBus --------------------------------------------------------------------------------
def test_unknown_kind_rejected(bridge):
    with pytest.raises(ValueError):
        bridge.events.subscribe("nope", lambda **d: None)


def test_emit_without_subscribers_is_noop(bridge):
    bridge.events.emit(INPUT_CHANGED, dev="di")  # must not raise


def test_subscriber_exception_does_not_break_others_or_emitter(bridge):
    got = []
    bridge.events.subscribe(INPUT_CHANGED, lambda **d: 1 / 0)
    bridge.events.subscribe(INPUT_CHANGED, lambda **d: got.append(d))
    bridge.events.emit(INPUT_CHANGED, dev="di")
    assert got == [{"dev": "di"}]


def test_emit_from_foreign_thread_runs_on_loop_thread(bridge):
    seen = []
    bridge.events.subscribe(AVAILABILITY, lambda **d: seen.append(threading.current_thread()))
    t = threading.Thread(target=lambda: bridge.events.emit(AVAILABILITY, online=True))

    async def scenario():
        t.start()
        await REAL_SLEEP(0.1)  # loop is running here, so the emit must be marshalled onto it

    run(bridge, scenario())
    t.join()
    assert seen == [threading.main_thread()]


def test_unsubscribe(bridge):
    got = []
    cb = lambda **d: got.append(d)
    bridge.events.subscribe(AVAILABILITY, cb)
    bridge.events.unsubscribe(AVAILABILITY, cb)
    bridge.events.emit(AVAILABILITY, online=True)
    assert got == []


# ---- events emitted by the bridge --------------------------------------------------------------
def test_input_changed_event_on_change_only(discovered):
    b = discovered
    got = collect(b, INPUT_CHANGED)
    new = 0 if fx("di", "1_01") == 1 else 1
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": new})
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": new})  # deduped: no event
    assert len(got) == 1
    e = got[0]
    assert (e["dev"], e["circuit"], e["value"], e["raw"], e["source"], e["subkey"]) == ("di", "1_01", new, new, "ws", None)
    assert isinstance(e["ts"], float)


def test_input_changed_marks_republish(discovered):
    b = discovered
    got = collect(b, INPUT_CHANGED)
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": fx("di", "1_01")}, force=True)
    assert [e["source"] for e in got] == ["republish"]


def test_input_changed_for_onewire_has_subkey(discovered):
    b = discovered
    got = collect(b, INPUT_CHANGED)
    t = fx("1wdevice", "268CCC30020000F2", "temp")
    b.process_websocket_message({"dev": "1wdevice", "circuit": "268CCC30020000F2", "temp": t + 2.0})
    assert [(e["dev"], e["subkey"], e["value"]) for e in got] == [("1wdevice", "temp", t + 2.0)]


def test_output_changed_from_mqtt_command(discovered, fast_sleep):
    from conftest import fake_message
    b = discovered
    got = collect(b, OUTPUT_CHANGED)
    b.device_states["ro_xS51_01"] = 1
    b.on_mqtt_message(None, None, fake_message(f"unipi/{DN}/ro/xS51_01/set", "ON"))
    settle(b)
    assert [(e["dev"], e["circuit"], e["value"], e["origin"]) for e in got] == [("ro", "xS51_01", 1, "mqtt")]


def test_output_changed_from_ao_fade_is_origin_fade(discovered, fast_sleep):
    from conftest import fake_message
    b = discovered
    got = collect(b, OUTPUT_CHANGED)
    b.on_mqtt_message(None, None, fake_message(f"unipi/{DN}/ao/xS51_01/set", json.dumps({"state": "ON", "brightness": 500, "transition": 0})))
    settle(b, 0.2)
    assert [(e["dev"], e["value"], e["origin"]) for e in got] == [("ao", 500, "fade")]


def test_output_changed_from_local_rule_is_origin_rule(discovered, mod):
    b = discovered
    got = collect(b, OUTPUT_CHANGED)
    b.device_states["di_1_01"] = 0
    b.local_logic.rules = [mod.LocalLogicRule(name="t", trigger_dev="di", trigger_circuit="1_01", trigger_value=1,
                                              action_dev="ro", action_circuit="xS51_01", action_value=1)]
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    assert [(e["dev"], e["value"], e["origin"]) for e in got] == [("ro", 1, "rule")]


def test_availability_event(discovered):
    b = discovered
    got = collect(b, AVAILABILITY)
    b.publish_availability("online")
    b.publish_availability("offline")
    assert [e["online"] for e in got] == [True, False]


def test_failing_subscriber_does_not_stop_mqtt_publishing(discovered):
    b = discovered
    b.events.subscribe(INPUT_CHANGED, lambda **d: 1 / 0)
    new = 0 if fx("di", "1_01") == 1 else 1
    drain(b.websocket_to_mqtt_queue)
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": new})
    assert drain(b.websocket_to_mqtt_queue) == [(f"unipi/{DN}/di/1_01/state", "ON" if new else "OFF")]


# ---- CommandService ----------------------------------------------------------------------------
def test_send_ws_queues_exact_json(bridge):
    cmd = bridge.commands.send_ws("ro", "xS51_01", 1)
    assert cmd == {"cmd": "set", "dev": "ro", "circuit": "xS51_01", "value": 1}
    assert [json.loads(m) for m in drain(bridge.mqtt_to_websocket_queue)] == [cmd]


def test_send_ws_full_queue_drops_and_reports(bridge):
    bridge.mqtt_to_websocket_queue = queue.Queue(maxsize=1)
    assert bridge.commands.send_ws("ro", "xS51_01", 1) is not None
    assert bridge.commands.send_ws("ro", "xS51_01", 0) is None  # dropped, not raised


def test_set_digital_goes_through_verified_ack_path(discovered, fast_sleep):
    b = discovered
    got = collect(b, OUTPUT_CHANGED)
    b.device_states["ro_xS51_02"] = 1
    run(b, b.commands.set_digital("ro", "xS51_02", True))
    assert [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)] == [{"cmd": "set", "dev": "ro", "circuit": "xS51_02", "value": 1}]
    assert got and got[0]["origin"] == "mqtt"


def test_transition_via_command_service(discovered, fast_sleep):
    b = discovered
    run(b, b.commands.transition("ao", "xS51_01", 500, 0))
    settle(b, 0.2)
    assert [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)] == [5.0]
