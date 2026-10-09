"""Editor "Test" button: run the ACTION of a saved rule on demand (trigger, conditions and `when` skipped)."""
import json
from types import SimpleNamespace

import pytest
from conftest import drain, run, settle
from test_push_to_dim import setup as dim_setup, AO  # noqa: F401  (shared helpers)

DN = "Neuron_S103_2258"
EVENT_TOPIC = f"unipi/{DN}/rules/activity"


@pytest.fixture
def b(discovered, mod, fast_sleep):
    discovered.mod = mod
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def rule(b, **kw):
    f = dict(name="Lamp", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
             action_type="set", action_dev="ao", action_circuit="xS51_01", action_value="5", action_transition=0.0)
    f.update(kw)
    return b.mod.LocalLogicRule(**f)


def req(rule_id, body=None):
    async def j():
        if body is None:
            raise ValueError("no body")
        return body
    return SimpleNamespace(match_info={"id": rule_id}, json=j)


def run_test(b, r, body=None):
    resp = run(b, b.web_handler_test_rule(req(r.id, body)))
    return resp.status, json.loads(resp.text)


def sent(b):
    return [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)]


def test_the_action_runs_without_any_trigger(b):
    r = rule(b)
    b.local_logic.rules = [r]
    drain(b.mqtt_to_websocket_queue)
    status, body = run_test(b, r)
    assert status == 200 and body["ok"]
    assert sent(b) == [{"cmd": "set", "dev": "ao", "circuit": "xS51_01", "value": 5.0}]
    steps = [(e["step"], e["ok"]) for e in body["events"]]
    assert steps == [("test", True), ("executed", True)]


def test_conditions_and_the_ha_offline_gate_are_skipped(b):
    cond = b.mod.RuleCondition(dev="di", circuit="1_02", operator="eq", value=1)
    r = rule(b, conditions=[cond], when="ha_offline")
    b.local_logic.rules = [r]
    b.device_states["di_1_02"] = 0                                     # the condition is NOT met
    b.ha_online, b.mqtt_client.connected = True, True                  # and HA is up and reachable
    assert b._ha_reachable()
    drain(b.mqtt_to_websocket_queue)
    status, _ = run_test(b, r)
    assert status == 200 and len(sent(b)) == 1


def test_it_is_marked_as_a_test_in_trace_and_in_the_ha_logbook_event(b):
    r = rule(b)
    b.local_logic.rules = [r]
    drain(b.websocket_to_mqtt_queue)
    _, body = run_test(b, r)
    assert all(e["detail"].startswith("TEST") for e in body["events"])
    assert body["events"][-1]["extra"]["test"] is True
    ev = [json.loads(p) for t, p in drain(b.websocket_to_mqtt_queue) if t == EVENT_TOPIC]
    assert len(ev) == 1 and ev[0]["test"] is True and ev[0]["detail"].startswith("TEST:")
    # ... and a real trigger afterwards is NOT marked
    b.device_states["di_1_01"] = 0
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 1})
    ev = [json.loads(p) for t, p in drain(b.websocket_to_mqtt_queue) if t == EVENT_TOPIC]
    assert len(ev) == 1 and "test" not in ev[0] and not ev[0]["detail"].startswith("TEST")


def test_unknown_or_disabled_rules_are_refused_and_nothing_is_sent(b):
    r = rule(b, action_type="pulse", action_dev="led", action_circuit="1_01", action_pulse={"count": 50, "on_ms": 100, "off_ms": 100})
    b.local_logic.rules = [r]
    b.local_logic.revalidate()
    drain(b.mqtt_to_websocket_queue)
    status, body = run_test(b, r)
    assert status == 409 and "disabled" in body["error"] and "count" in body["error"]
    status, body = run_test(b, rule(b, name="not saved"))
    assert status == 404 and "save" in body["error"]
    assert sent(b) == []


def test_bad_mode_and_hold_on_a_plain_rule_are_refused(b):
    r = rule(b)
    b.local_logic.rules = [r]
    assert run_test(b, r, {"mode": "bogus"})[0] == 400
    status, body = run_test(b, r, {"mode": "hold"})
    assert status == 400 and "push-to-dim" in body["error"]
    assert sent(b) == []


def test_a_toggle_rule_flips_the_output_each_test(b):
    r = rule(b, action_type="toggle", action_dev="do", action_circuit="1_01", action_value=None)
    b.local_logic.rules = [r]
    b.device_states["do_1_01"] = 0
    drain(b.mqtt_to_websocket_queue)
    run_test(b, r)
    assert [c["value"] for c in sent(b)] == [1]
    b.device_states["do_1_01"] = 1
    run_test(b, r)
    assert [c["value"] for c in sent(b)] == [0]


def test_a_pulse_rule_rings_through_the_sequencer(b):
    from test_sequencer import FakeTime
    ft = FakeTime()
    b.sequencer.clock, b.sequencer.sleep = ft.clock, ft.sleep
    times = []
    real = b.websocket_connection.send

    async def timed(m):
        times.append(round(ft.t, 3))
        await real(m)
    b.websocket_connection.send = timed
    r = rule(b, action_type="pulse", action_dev="led", action_circuit="1_01", action_value=None,
             action_pulse={"count": 2, "on_ms": 100, "off_ms": 300})
    b.local_logic.rules = [r]
    status, body = run_test(b, r)
    run(b, ft.advance_to(3))
    assert status == 200
    assert [json.loads(m)["value"] for m in b.websocket_connection.sent] == [1, 0, 1, 0]
    assert times == [0.0, 0.1, 0.4, 0.5]                              # exactly the timing a real button press gets


def test_shadow_instance_never_acts(b):
    r = rule(b)
    b.local_logic.rules = [r]
    b.shadow = True
    drain(b.mqtt_to_websocket_queue)
    assert run_test(b, r)[0] == 409 and sent(b) == []


def test_the_route_is_registered_and_protected(b):
    import inspect
    src = inspect.getsource(b.mod.UnipiBridge.setup_web_server)
    assert 'add_post("/api/rules/{id}/test"' in src
    # not in the unauthenticated list of the middleware
    mw = inspect.getsource(b.mod.UnipiBridge.auth_middleware)
    assert "/test" not in mw


# ---- push-to-dim: tap and hold ------------------------------------------------------------------------------------
@pytest.fixture
def d(discovered, mod):
    from test_push_to_dim import FakeTime
    ft = FakeTime()
    discovered._dim_sleep = ft.sleep
    discovered.ft, discovered.mod = ft, mod
    return discovered


def dim_rule(d, **kw):
    dim_setup(d, **kw)
    return d.local_logic.rules[0]


def test_dimmer_tap_toggles_like_a_short_press(d):
    r = dim_rule(d, ao_level=0.0)
    drain(d.mqtt_to_websocket_queue)
    assert run_test(d, r)[0] == 200
    run(d, d.ft.advance_to(d.ft.t + 0.1))
    assert [c["value"] for c in sent(d)] == [6.0]                    # on at the configured level, no dimming
    d.device_states["ao_xS51_01"] = 6.0
    run_test(d, r)
    assert [c["value"] for c in sent(d)] == [0.0]


def test_dimmer_hold_dims_and_lets_go_by_itself(d):
    r = dim_rule(d, ao_level=5.0, dimmer_hold_ms=1000)
    drain(d.mqtt_to_websocket_queue)
    assert run_test(d, r, {"mode": "hold"})[0] == 200
    run(d, d.ft.advance_to(d.ft.t + 0.5))
    assert sent(d) == []                                              # still within the hold time: nothing yet
    run(d, d.ft.advance_to(d.ft.t + 4.0))                             # hold time + 2 s dimming + release
    vals = [c["value"] for c in sent(d)]
    assert vals and vals == sorted(vals) and vals[0] > 5.0 and 0.0 not in vals
    st = d.dimmer_states[r.id]
    assert st["is_dimming"] is False and not st.get("pressed")        # it released
    n = len(vals)
    run(d, d.ft.advance_to(d.ft.t + 5.0))
    assert sent(d) == []                                              # and stays released: no endless dimming
