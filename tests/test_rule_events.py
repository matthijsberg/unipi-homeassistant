"""T17d: every executed Unipi rule shows up in Home Assistant (event entity -> Logbook)."""
import json
import threading
import time

import pytest
from conftest import drain, run, settle
from test_push_to_dim import setup as dim_setup  # noqa: F401  (shared helpers)

DN = "Neuron_S103_2258"
EVENT_TOPIC = f"unipi/{DN}/rules/activity"
CFG_TOPIC = f"homeassistant/event/{DN}/rule_activity/config"


def msgs(b):
    return drain(b.websocket_to_mqtt_queue)


def events(b):
    return [json.loads(p) for t, p in msgs(b) if t == EVENT_TOPIC]


def rule(b, name, **kw):
    f = dict(name=name, trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
             action_type="set", action_dev="ao", action_circuit="xS51_01", action_value="5", action_transition=0.0)
    f.update(kw)
    return b.mod.LocalLogicRule(**f)


@pytest.fixture
def b(discovered, mod, fast_sleep):
    discovered.mod = mod
    msgs(discovered)
    return discovered


def press(b, v=1, circuit="1_01"):
    b.process_websocket_message({"dev": "di", "circuit": circuit, "value": v})


# ---- discovery ------------------------------------------------------------------------------------------------------
def test_event_entity_is_discovered_with_the_rule_names_as_event_types(b):
    b.local_logic.replace_rules([rule(b, "Serre Light AAN"), rule(b, "Doorbell front")])
    cfgs = [json.loads(p) for t, p in msgs(b) if t == CFG_TOPIC]
    c = cfgs[-1]
    assert c["event_types"] == ["Doorbell front", "Serre Light AAN", "other"]
    assert c["state_topic"] == EVENT_TOPIC and c["unique_id"] == f"{DN}_rule_activity" and c["name"] == "Rule activity"
    assert c["dev"]["identifiers"] == [DN] and c["availability_topic"] == f"unipi/{DN}/status"      # same device as the lights


def test_discovery_is_refreshed_whenever_the_rules_change(b):
    b.local_logic.replace_rules([rule(b, "A")])
    first = [json.loads(p)["event_types"] for t, p in msgs(b) if t == CFG_TOPIC][-1]
    b.local_logic.replace_rules([rule(b, "A"), rule(b, "B renamed")])
    second = [json.loads(p)["event_types"] for t, p in msgs(b) if t == CFG_TOPIC][-1]
    assert first == ["A", "other"] and second == ["A", "B renamed", "other"]


def test_discovery_is_part_of_the_normal_start_up_and_republish(bridge):
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    assert any(t == CFG_TOPIC for t, _ in drain(bridge.websocket_to_mqtt_queue))


# ---- events ---------------------------------------------------------------------------------------------------------
def test_an_executed_rule_produces_exactly_one_event_naming_the_rule_and_its_target(b):
    b.local_logic.replace_rules([rule(b, "Serre Light AAN")])
    msgs(b)
    b.device_states["di_1_01"] = 0
    press(b)
    ev = events(b)
    assert len(ev) == 1
    e = ev[0]
    assert e["event_type"] == "Serre Light AAN" and e["rule"] == "Serre Light AAN"
    assert e["target"] == "ao/xS51_01" and e["action"] == "set" and "5" in e["detail"]


def test_nothing_that_did_not_run_is_reported(b):
    cond = b.mod.RuleCondition(dev="di", circuit="1_02", operator="eq", value=1)
    b.local_logic.replace_rules([rule(b, "blocked", conditions=[cond]), rule(b, "wrong value", trigger_value=0),
                                 rule(b, "gated", when="ha_offline"), rule(b, "bad", action_value="125")])
    msgs(b)
    b.ha_online, b.mqtt_client.connected = True, True
    b.device_states["di_1_01"] = 0
    b.device_states["di_1_02"] = 0
    press(b)
    assert events(b) == []                                           # failed condition, no match, gated, disabled: no event


def test_a_rule_unknown_to_the_declared_list_is_reported_as_other(b):
    b.local_logic.rules = [rule(b, "added without refresh")]            # bypasses the refresh hook on purpose
    b.device_states["di_1_01"] = 0
    press(b)
    e = events(b)[0]
    assert e["event_type"] == "other" and e["rule"] == "added without refresh"      # HA would reject an undeclared type


def test_push_to_dim_reports_the_tap_and_the_end_of_a_dim_but_not_every_step(b):
    from test_sequencer import FakeTime
    ft = FakeTime(); b._dim_sleep = ft.sleep
    b.local_logic.replace_rules([rule(b, "Lamp dimmer", action_type="dimmer", dimmer_hold_ms=500)])
    b.device_states["ao_xS51_01"] = 5.0
    b.device_states["di_1_01"] = 0
    msgs(b)
    press(b, 1); run(b, ft.advance_to(0.2)); press(b, 0)                       # tap -> off
    assert [e["rule"] for e in events(b)] == ["Lamp dimmer"]
    b.device_states["ao_xS51_01"] = 5.0
    press(b, 1); run(b, ft.advance_to(3.0)); press(b, 0)                       # hold ~2.5 s = ~25 dimming steps
    ev = events(b)
    assert len(ev) == 1 and "dimming stopped" in ev[0]["detail"]


def test_events_are_never_retained(b):
    b.local_logic.replace_rules([rule(b, "R")])
    b.device_states["di_1_01"] = 0
    press(b)
    t = threading.Thread(target=b.mqtt_worker_thread, daemon=True); t.start()
    for _ in range(60):
        if b.websocket_to_mqtt_queue.unfinished_tasks == 0 and b.mqtt_client.published:
            break
        time.sleep(0.05)
    b.should_stop.set(); t.join(3)
    published = {topic: retain for topic, _, _, retain in b.mqtt_client.published}
    assert published[EVENT_TOPIC] is False and published[CFG_TOPIC] is True    # the event must not replay after a restart


def test_trace_entries_carry_the_target(b):
    b.local_logic.replace_rules([rule(b, "T")])
    b.device_states["di_1_01"] = 0
    press(b)
    ex = [e for e in b.local_logic.trace_since(0)["events"] if e["step"] == "executed"][0]
    assert ex["extra"] == {"target": "ao/xS51_01", "action": "set"}


def test_a_failing_event_callback_never_breaks_rule_execution(b):
    b.local_logic.replace_rules([rule(b, "R")])
    b.local_logic.on_executed = lambda entry: 1 / 0
    b.device_states["di_1_01"] = 0
    press(b)
    assert drain(b.mqtt_to_websocket_queue)                                   # the lamp command still went out
    steps = [e["step"] for e in b.local_logic.trace_since(0)["events"]]
    assert "error" not in steps and steps.count("executed") == 1             # and the rule is not reported as having failed
