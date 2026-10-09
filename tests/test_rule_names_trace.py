"""Rules saved with evok-2 device names must work on evok 3, bad values are refused up front, and every
step the engine takes is visible in the activity trace."""
import json
from types import SimpleNamespace

import pytest
from conftest import drain, run, settle

DN = "Neuron_S103_2258"

# The rule as the editor saved it on the S103 (evok-2 names), with the voltage corrected to what the UI showed.
USER_RULE = {
    "id": "6f2d15f9-2f86-4d62-8f2d-8466e9d61321", "name": "Serre Light",
    "trigger_dev": "input", "trigger_circuit": "xS51_03", "trigger_operator": "eq", "trigger_value": "1",
    "conditions": [], "action_type": "set", "action_dev": "analogoutput", "action_circuit": "1_01",
    "action_value": "0.5", "action_transition": 3000.0, "action_delay": 0.0,
    "action_pulse": None, "action_preset": None, "when": "always", "dimmer_hold": True,
}


@pytest.fixture
def b(discovered, mod, fast_sleep):
    discovered.mod = mod
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def rule(b, **kw):
    return b.mod.LocalLogicRule(**{**USER_RULE, **kw})


def press(b, value=1, circuit="xS51_03", dev="di"):
    b.process_websocket_message({"dev": dev, "circuit": circuit, "value": value})


def steps(b, since=0):
    return [(e["step"], e["ok"]) for e in b.local_logic.trace_since(since)["events"]]


# ---- the bug you hit ---------------------------------------------------------------------------------------
def test_rule_with_evok2_names_fires_on_evok3_and_drives_ao(b):
    b.local_logic.rules = [rule(b)]
    b.local_logic.revalidate()
    assert not b.local_logic.disabled
    b.device_states["di_xS51_03"] = 0
    b.device_states["ao_1_01"] = 0.0
    press(b)
    settle(b, 0.5)
    cmds = [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)]
    assert cmds and all(c["dev"] == "ao" and c["circuit"] == "1_01" for c in cmds)    # evok 3 name, not "analogoutput"
    assert cmds[-1]["value"] == 0.5 and len(cmds) > 5                                 # a 3 s fade, ending on the target
    assert steps(b) == [("trigger", True), ("executed", True)]


def test_without_transition_it_is_a_single_set(b):
    b.local_logic.rules = [rule(b, action_transition=0.0)]
    b.device_states["di_xS51_03"] = 0
    press(b)
    assert [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)] == [{"cmd": "set", "dev": "ao", "circuit": "1_01", "value": 0.5}]


@pytest.mark.parametrize("trigger_dev,msg_dev,fires", [("input", "di", True), ("di", "di", True), ("di", "input", True),
                                                       ("input", "ai", False), ("ai", "di", False)])
def test_trigger_names_are_compared_in_canonical_form(b, trigger_dev, msg_dev, fires):
    b.local_logic.rules = [rule(b, trigger_dev=trigger_dev, action_transition=0.0)]
    b.device_states.pop("di_xS51_03", None)
    press(b, dev=msg_dev) if msg_dev == "di" else b.local_logic.evaluate({"dev": msg_dev, "circuit": "xS51_03", "value": 1})
    got = b.local_logic.evaluate({"dev": msg_dev, "circuit": "xS51_03", "value": 1}, b.device_states)
    assert bool(got) is fires and (not got or got[0]["dev"] == "ao")


def test_legacy_action_names_become_evok3_names(b):
    b.local_logic.rules = [rule(b, action_dev="relay", action_circuit="xS51_01", action_value="1", action_transition=0.0)]
    acts = b.local_logic.evaluate({"dev": "di", "circuit": "xS51_03", "value": 1})
    assert acts[0]["dev"] == "ro"


def test_conditions_with_legacy_names_read_the_right_state(b):
    cond = b.mod.RuleCondition(dev="input", circuit="1_02", operator="eq", value=1)
    b.local_logic.rules = [rule(b, conditions=[cond], action_transition=0.0)]
    assert b.local_logic.evaluate({"dev": "di", "circuit": "xS51_03", "value": 1}, {"di_1_02": 0}) == []
    assert len(b.local_logic.evaluate({"dev": "di", "circuit": "xS51_03", "value": 1}, {"di_1_02": 1})) == 1


# ---- bad values are refused with a reason --------------------------------------------------------------------
@pytest.mark.parametrize("kw,why", [
    (dict(action_value="125"), "between 0 and 10"),                      # what was in your saved file
    (dict(action_value="-1"), "between 0 and 10"), (dict(action_value="abc"), "between 0 and 10"),
    (dict(action_dev="relay", action_value="2"), "1/0 or ON/OFF"),
    (dict(action_dev="input"), "needs an output device"),
    (dict(trigger_dev="inpt"), "unknown trigger device"),
    (dict(conditions=[{"dev": "nope", "circuit": "1", "operator": "eq", "value": 1}]), "unknown device 'nope'"),
])
def test_invalid_rules_are_refused_with_a_reason(b, kw, why):
    err = b.validate_rule(rule(b, **kw))
    assert err and why in err


@pytest.mark.parametrize("kw", [dict(), dict(action_value="10"), dict(action_value="0"),
                                dict(action_dev="relay", action_circuit="xS51_01", action_value="ON"),
                                dict(action_dev="led", action_circuit="1_01", action_value="off")])
def test_valid_rules_pass(b, kw):
    assert b.validate_rule(rule(b, **kw)) is None


def test_the_saved_rule_with_125_is_disabled_and_explained_in_the_trace(b):
    b.local_logic.rules = [rule(b, action_value="125")]
    b.local_logic.revalidate()
    b.device_states["di_xS51_03"] = 0
    press(b)
    ev = b.local_logic.trace_since(0)["events"][-1]
    assert (ev["step"], ev["ok"]) == ("disabled", False) and "between 0 and 10" in ev["detail"]
    assert drain(b.mqtt_to_websocket_queue) == []


# ---- the trace ------------------------------------------------------------------------------------------------------
def test_trace_explains_a_value_that_did_not_match(b):
    b.local_logic.rules = [rule(b, action_transition=0.0)]
    b.device_states["di_xS51_03"] = 1
    press(b, value=0)
    ev = b.local_logic.trace_since(0)["events"]
    assert [(e["step"], e["ok"]) for e in ev] == [("trigger", False)]
    assert "xS51_03 = 0" in ev[0]["detail"] and "needs = 1" in ev[0]["detail"] and ev[0]["rule"] == "Serre Light"


def test_trace_does_not_flood_on_fast_changing_values(b):
    b.local_logic.rules = [rule(b, trigger_dev="ai", trigger_circuit="1_01", trigger_operator="gt", trigger_value="9",
                                action_transition=0.0)]
    for v in (1.0, 1.1, 1.2, 1.3):
        b.local_logic.evaluate({"dev": "ai", "circuit": "1_01", "value": v})
    assert len(b.local_logic.trace_since(0)["events"]) == 1                    # one "no match" per 2 s per rule
    b.local_logic.evaluate({"dev": "ai", "circuit": "1_01", "value": 9.5})
    assert steps(b)[-1] == ("trigger", True)                                    # but a match is always recorded


def test_trace_shows_which_condition_stopped_the_rule(b):
    cond = b.mod.RuleCondition(dev="di", circuit="1_02", operator="eq", value=1)
    b.local_logic.rules = [rule(b, conditions=[cond], action_transition=0.0)]
    b.device_states["di_xS51_03"] = 0
    b.device_states["di_1_02"] = 0
    press(b)
    ev = b.local_logic.trace_since(0)["events"]
    assert [(e["step"], e["ok"]) for e in ev] == [("trigger", True), ("conditions", False)]
    assert "di/1_02" in ev[1]["detail"] and "needs = 1" in ev[1]["detail"]


def test_trace_shows_gating_delay_shadow_and_refusals(b):
    b.local_logic.rules = [rule(b, when="ha_offline", action_transition=0.0)]
    b.ha_online, b.mqtt_client.connected = True, True
    b.device_states["di_xS51_03"] = 0
    press(b)
    assert steps(b)[-1] == ("gated", False) and "Home Assistant is reachable" in b.local_logic.trace_since(0)["events"][-1]["detail"]
    b.local_logic.rules = [rule(b, action_transition=0.0, action_delay=5.0)]
    b.device_states["di_xS51_03"] = 0
    n = b.local_logic._seq
    press(b)
    assert [s for s, _ in steps(b, n)] == ["trigger", "delayed"]
    for t in b.active_delayed_actions.values():
        t.cancel()
    settle(b, 0.05)
    b.shadow = True
    n = b.local_logic._seq
    b.local_logic.rules = [rule(b, action_transition=0.0)]
    b.device_states["di_xS51_03"] = 0
    press(b)
    assert steps(b, n)[-1] == ("shadow", False)
    b.shadow = False


def test_trace_records_a_refused_pulse(b):
    b.local_logic.rules = [rule(b, action_type="pulse", action_dev="led", action_circuit="1_01", action_value=None,
                                action_pulse={"count": 3}, action_transition=0.0)]
    b.config.circuits = b.mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                                        circuits={"led/1_01": {"max_count": 2}}).circuits      # limits tightened after the rule was saved
    b.device_states["di_xS51_03"] = 0
    press(b)
    settle(b, 0.1)
    last = b.local_logic.trace_since(0)["events"][-1]
    assert (last["step"], last["ok"]) == ("rejected", False) and "count 3" in last["detail"]


def test_trace_api_returns_only_new_events(b):
    b.local_logic.rules = [rule(b, action_transition=0.0)]
    b.device_states["di_xS51_03"] = 0
    press(b)
    first = json.loads(run(b, b.web_handler_rule_trace(SimpleNamespace(query={}))).text)
    assert first["last"] >= 2 and len(first["events"]) >= 2
    b.device_states["di_xS51_03"] = 0
    press(b)
    newer = json.loads(run(b, b.web_handler_rule_trace(SimpleNamespace(query={"since": str(first["last"])}))).text)
    assert newer["events"] and all(e["seq"] > first["last"] for e in newer["events"])
    assert json.loads(run(b, b.web_handler_rule_trace(SimpleNamespace(query={"since": "junk"}))).text)["events"]


def test_trace_is_bounded(b):
    for i in range(1000):
        b.local_logic.record("x", "x", "trigger", True, "x")
    assert len(b.local_logic.trace) == 300 and b.local_logic.trace_since(0)["last"] == 1000


def test_disabled_rule_is_a_warning_not_an_error_and_is_explained_in_the_api(b):
    lines = []
    b.logger = type("L", (), {"__getattr__": lambda self, lvl: (lambda m, *a, **k: lines.append((lvl, m)))})()
    bad = rule(b, action_value="125")
    b.local_logic.logger = b.logger
    b.local_logic.replace_rules([bad, rule(b, id="ok-rule", name="fine", action_value="0.5")])
    assert [lvl for lvl, m in lines if "DISABLED" in m] == ["warning"]           # no ERROR: no alarm, no failed deploy
    rows = {r["name"]: r for r in json.loads(run(b, b.web_handler_get_rules(SimpleNamespace())).text)}
    assert "between 0 and 10" in rows["Serre Light"]["disabled_reason"] and "disabled_reason" not in rows["fine"]
