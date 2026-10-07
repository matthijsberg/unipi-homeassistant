"""T15: hardware pulse counters (water meter etc.) as total_increasing sensors."""
import asyncio
import json
import threading
import time

import pytest
from conftest import REAL_SLEEP, drain, fx, run
from pydantic import ValidationError

DN = "Neuron_S103_2258"
CIRCUIT = "1_04"          # a real, wired input with a running hardware counter on the S103


def setup(bridge, mod, **opts):
    cfg = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                        circuits={f"di/{CIRCUIT}": {"counter": True, "counter_interval_s": 10, "unit": "L", **opts}})
    bridge.config.circuits = cfg.circuits
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    return dict(drain(bridge.websocket_to_mqtt_queue))


@pytest.fixture
def b(bridge, mod):
    bridge.disc = setup(bridge, mod)
    return bridge


def counter_msgs(b):
    return [json.loads(p)["value"] for t, p in drain(b.websocket_to_mqtt_queue) if t == f"unipi/{DN}/di/{CIRCUIT}/counter"]


def push(b, counter, **kw):
    b.process_websocket_message({"dev": "di", "circuit": CIRCUIT, "value": fx("di", CIRCUIT), "counter": counter}, **kw)


def age(b, seconds=11):
    b._counters[CIRCUIT]["published_at"] -= seconds       # pretend time has passed


# ---- discovery -------------------------------------------------------------------------------------
def test_counter_sensor_is_discovered_next_to_the_input(b):
    c = json.loads(b.disc[f"homeassistant/sensor/{DN}/di_{CIRCUIT}_counter/config"])
    assert c["state_class"] == "total_increasing" and c["unit_of_measurement"] == "L"
    assert c["state_topic"] == f"unipi/{DN}/di/{CIRCUIT}/counter" and c["value_template"] == "{{ value_json.value }}"
    assert c["unique_id"] == f"{DN}_di_{CIRCUIT}_counter" and c["name"] == f"di {CIRCUIT} counter"
    assert c["availability_topic"] == f"unipi/{DN}/status" and c["dev"]["identifiers"] == [DN]
    assert f"homeassistant/binary_sensor/{DN}/di_{CIRCUIT}/config" in b.disc            # the input itself is unchanged


def test_only_configured_inputs_get_a_counter(b):
    assert not [t for t in b.disc if t.endswith("_counter/config") and f"di_{CIRCUIT}_" not in t]


def test_initial_counter_value_is_published_at_discovery(b):
    assert json.loads(b.disc[f"unipi/{DN}/di/{CIRCUIT}/counter"]) == {"value": fx("di", CIRCUIT, "counter")}


# ---- publishing rules -------------------------------------------------------------------------------
def test_rate_limited_and_only_on_change(b):
    base = fx("di", CIRCUIT, "counter")
    push(b, base)                                             # unchanged: nothing
    assert counter_msgs(b) == []
    for n in range(1, 6):
        push(b, base + n)                                     # five pulses within the interval
    assert counter_msgs(b) == []                              # held back...
    age(b); push(b, base + 6)
    assert counter_msgs(b) == [base + 6]                      # ...then exactly one message with the latest value


def test_an_unchanged_value_is_never_republished_even_after_the_interval(b):
    base = fx("di", CIRCUIT, "counter")
    age(b); push(b, base)                                     # interval over, but nothing changed
    assert counter_msgs(b) == []
    b._counter_flush()
    assert counter_msgs(b) == []


def test_pending_value_is_flushed_when_the_interval_has_passed(b):
    base = fx("di", CIRCUIT, "counter")
    push(b, base + 3)
    assert counter_msgs(b) == []
    b._counter_flush()
    assert counter_msgs(b) == []                              # still too early
    age(b); b._counter_flush()
    assert counter_msgs(b) == [base + 3]


def test_a_counter_reset_is_published_as_it_is(b):
    age(b); push(b, 0)
    assert counter_msgs(b) == [0]                             # HA treats a drop of a total_increasing sensor as a reset


def test_republish_forces_the_current_value(b):
    base = fx("di", CIRCUIT, "counter")
    push(b, base, force=True)
    assert counter_msgs(b) == [base]


def test_other_inputs_and_garbage_are_ignored(b):
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 0, "counter": 99})
    push(b, "not a number")
    assert counter_msgs(b) == []


def test_input_changed_event_carries_the_counter(b):
    seen = []
    b.events.subscribe("input_changed", lambda **d: seen.append(d))
    age(b); push(b, fx("di", CIRCUIT, "counter") + 7)
    ev = [e for e in seen if e["subkey"] == "counter"]
    assert len(ev) == 1 and ev[0]["value"] == fx("di", CIRCUIT, "counter") + 7 and ev[0]["circuit"] == CIRCUIT


def test_counter_messages_are_retained_by_the_mqtt_worker(b):
    b.websocket_to_mqtt_queue.put((f"unipi/{DN}/di/{CIRCUIT}/counter", '{"value": 1}'))
    t = threading.Thread(target=b.mqtt_worker_thread, daemon=True); t.start()
    for _ in range(40):
        if b.mqtt_client.published:
            break
        time.sleep(0.05)
    b.should_stop.set(); t.join(3)
    assert b.mqtt_client.published[0][3] is True              # retain flag


# ---- REST safety net ------------------------------------------------------------------------------------
def test_poller_picks_up_counters_that_were_not_pushed(b, fast_sleep):
    base = fx("di", CIRCUIT, "counter")
    age(b)

    async def rest(dev=None, circuit=None, scope=None):
        return [{"dev": "di", "circuit": CIRCUIT, "value": 0, "counter": base + 42}]
    b.get_unipi_data = rest

    async def scenario():
        task = asyncio.get_running_loop().create_task(b._counter_poller())
        await REAL_SLEEP(0.1)
        b.should_stop.set(); task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    run(b, scenario())
    assert counter_msgs(b) == [base + 42]


# ---- config validation ------------------------------------------------------------------------------------
@pytest.mark.parametrize("circuit,opts,msg", [
    ("ro/xS51_01", {"counter": True}, "only applies to digital inputs"),
    ("di/1_04", {"unit": "L"}, "only applies to counters"),
    ("di/1_04", {"counter": True, "counter_interval_s": 0}, "greater than or equal"),
    ("di/1_04", {"counter": True, "counter_interval_s": 99999}, "less than or equal"),
])
def test_invalid_counter_config_fails_at_startup(mod, circuit, opts, msg):
    with pytest.raises(ValidationError) as e:
        mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"}, circuits={circuit: opts})
    assert msg in str(e.value)
