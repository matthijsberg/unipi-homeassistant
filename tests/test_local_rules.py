"""Characterization: local logic rules (work without Home Assistant)."""
import json

from conftest import drain, fx, settle

DN = "Neuron_S103_2258"


def rule(mod, **kw):
    base = dict(name="t", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
                action_type="set", action_dev="ro", action_circuit="xS51_01", action_value=1)
    base.update(kw)
    return mod.LocalLogicRule(**base)


def ws(b):
    return [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)]


def test_engine_matches_trigger_only(bridge, mod):
    bridge.local_logic.rules = [rule(mod)]
    hit = bridge.local_logic.evaluate({"dev": "di", "circuit": "1_01", "value": 1})
    assert [(a["type"], a["dev"], a["circuit"], a["value"]) for a in hit] == [("set", "ro", "xS51_01", 1)]
    assert bridge.local_logic.evaluate({"dev": "di", "circuit": "1_01", "value": 0}) == []
    assert bridge.local_logic.evaluate({"dev": "di", "circuit": "1_02", "value": 1}) == []


def test_engine_condition_on_other_device(bridge, mod):
    r = rule(mod, conditions=[mod.RuleCondition(dev="di", circuit="1_02", operator="eq", value=1)])
    bridge.local_logic.rules = [r]
    msg = {"dev": "di", "circuit": "1_01", "value": 1}
    assert bridge.local_logic.evaluate(msg, {"di_1_02": 0}) == []
    assert len(bridge.local_logic.evaluate(msg, {"di_1_02": 1})) == 1


def test_rule_fires_end_to_end_and_acks(discovered, mod):
    b = discovered
    drain(b.websocket_to_mqtt_queue)
    b.device_states["di_1_01"] = 0
    b.local_logic.rules = [rule(mod)]
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    assert ws(b) == [{"cmd": "set", "dev": "ro", "circuit": "xS51_01", "value": 1}]
    topics = dict(drain(b.websocket_to_mqtt_queue))
    assert topics[f"unipi/{DN}/ro/xS51_01/state"] == "ON"  # optimistic ack to HA


def test_rules_do_not_fire_on_republish(discovered, mod):
    b = discovered
    b.local_logic.rules = [rule(mod)]
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1}, force=True)
    assert ws(b) == []


def test_dimmer_short_press_toggles_to_default_level(discovered, mod):
    b = discovered
    b.device_states["ao_xS51_01"] = 0.0
    r = rule(mod, trigger_operator="any", trigger_value=None, action_type="dimmer",
             action_dev="ao", action_circuit="xS51_01", action_value=None)
    b.local_logic.rules = [r]
    b.device_states["di_1_01"] = 0
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})   # press
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 0})   # release < 500 ms
    settle(b)
    assert ws(b) == [{"cmd": "set", "dev": "ao", "circuit": "xS51_01", "value": 10.0}]  # default level is fixed 10 V (K5)
    # second short press turns it off again
    b.device_states["ao_xS51_01"] = 10.0
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 0})
    settle(b)
    assert ws(b) == [{"cmd": "set", "dev": "ao", "circuit": "xS51_01", "value": 0.0}]


def test_rules_file_roundtrip(bridge, mod, tmp_path):
    bridge.local_logic.replace_rules([rule(mod)])
    reloaded = mod.LocalLogicEngine(str(tmp_path / "local_rules.json"), bridge.logger)
    assert [r.name for r in reloaded.rules] == ["t"]
