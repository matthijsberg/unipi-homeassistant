"""T17: local rules that must keep working without Home Assistant (doorbell, wall switches)."""
import json
from types import SimpleNamespace

import pytest
from conftest import drain, fake_message, run, settle
from test_sequencer import FakeTime

DN = "Neuron_S103_2258"


@pytest.fixture
def b(discovered, mod):
    ft = FakeTime()
    discovered.sequencer.clock, discovered.sequencer.sleep = ft.clock, ft.sleep
    discovered.ft = ft
    discovered.sent_times = []
    real = discovered.websocket_connection.send

    async def timed(m):
        discovered.sent_times.append(round(ft.t, 3))
        await real(m)
    discovered.websocket_connection.send = timed
    cfg = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                        circuits={"led/1_01": {"max_count": 6, "max_on_s": 30,
                                               "presets": {"ring3": {"pulse": {"count": 3}},
                                                           "open": {"timed": {"state": "ON", "duration_s": 20}}}}})
    discovered.config.circuits = cfg.circuits
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def rule(mod, **kw):
    base = dict(name="t", trigger_dev="di", trigger_circuit="1_01", trigger_operator="eq", trigger_value=1,
                action_type="set", action_dev="led", action_circuit="1_01", action_value=1)
    base.update(kw)
    return mod.LocalLogicRule(**base)


def press(b, circuit="1_01", value=1):
    b.process_websocket_message({"dev": "di", "circuit": circuit, "value": value})


def writes(b):
    return [json.loads(m)["value"] for m in b.websocket_connection.sent]


def queued(b):
    return [json.loads(m) for m in drain(b.mqtt_to_websocket_queue)]


# ---- validation ---------------------------------------------------------------------------------
@pytest.mark.parametrize("kw,ok", [
    (dict(), True),
    (dict(action_type="toggle", action_dev="relay", action_circuit="xS51_01"), True),
    (dict(action_type="pulse", action_pulse={"count": 3}), True),
    (dict(action_type="pulse", action_preset="ring3"), True),
    (dict(action_type="pulse", action_preset="open"), True),                      # a timed preset works too
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01", trigger_operator="any", action_value=5), True),
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01", dimmer_hold=False), True),
    (dict(action_type="bogus"), False),
    (dict(action_type="toggle", action_dev="ao"), False),
    (dict(action_type="pulse", action_pulse={"count": 3}, action_preset="ring3"), False),
    (dict(action_type="pulse"), False),
    (dict(action_type="pulse", action_pulse={"count": 7}), False),                  # over the circuit's max_count (6)
    (dict(action_type="pulse", action_preset="nope"), False),
    (dict(action_type="pulse", action_pulse={"count": 2}, action_dev="ao"), False),
    (dict(action_type="dimmer", action_dev="ro", trigger_operator="any"), False),
    (dict(action_type="dimmer", action_dev="ao", trigger_operator="any", action_value=11), False),
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01"), True),   # hold-to-dim always follows press AND release
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01", dimmer_hold_ms=100), False),
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01", dimmer_speed=0.01), False),
    (dict(action_type="dimmer", action_dev="ao", action_circuit="xS51_01", dimmer_min=7), False),
    (dict(when="sometimes"), False),
])
def test_rule_validation(b, mod, kw, ok):
    err = b.validate_rule(rule(mod, **kw))
    assert (err is None) == ok, err


def test_invalid_rules_are_disabled_but_kept_in_the_file(b, mod, tmp_path):
    good = rule(mod, name="good")
    bad = rule(mod, name="bad", when="sometimes", action_value=0)     # invalid, but would visibly act if evaluated
    b.local_logic.replace_rules([good, bad])
    assert list(b.local_logic.disabled) == [bad.id] and "when" in b.local_logic.disabled[bad.id]
    assert [r["name"] for r in json.loads((tmp_path / "local_rules.json").read_text())] == ["good", "bad"]   # not deleted
    b.device_states["di_1_01"] = 0
    press(b)                                                  # only the good rule acts
    assert [c["value"] for c in queued(b)] == [1]
    b.local_logic.load_rules()                                # and it is still disabled after a restart
    assert bad.id in b.local_logic.disabled


# ---- the doorbell: button -> N rings, no Home Assistant involved ------------------------------------
def test_doorbell_button_rings_three_times_locally(b, mod):
    b.local_logic.rules = [rule(mod, action_type="pulse", action_pulse={"count": 3, "on_ms": 100, "off_ms": 300})]
    b.device_states["di_1_01"] = 0
    b.mqtt_client.connected = False                           # no MQTT at all
    press(b)
    run(b, b.ft.advance_to(3))
    assert writes(b) == [1, 0, 1, 0, 1, 0]
    assert b.sent_times == [0.0, 0.1, 0.4, 0.5, 0.8, 0.9]     # legacy local timing: 100 ms on / 300 ms off


def test_doorbell_with_a_preset_and_release_edge_ignored(b, mod):
    b.local_logic.rules = [rule(mod, action_type="pulse", action_preset="ring3")]
    b.device_states["di_1_01"] = 0
    press(b, value=1); press(b, value=0)                       # release does not match 'eq 1'
    run(b, b.ft.advance_to(3))
    assert writes(b) == [1, 0] * 3


def test_pressing_again_restarts_the_ring_instead_of_stacking(b, mod):
    b.local_logic.rules = [rule(mod, trigger_operator="any", trigger_value=None, action_type="pulse", action_pulse={"count": 4})]
    b.device_states["di_1_01"] = 0
    press(b)
    run(b, b.ft.advance_to(0.2))
    b.device_states["di_1_01"] = 0
    press(b)                                                   # second press 0.2 s into the first ring
    run(b, b.ft.advance_to(5))
    assert writes(b)[:3] == [1, 0, 0]                          # ON, cancel-OFF, then the new sequence starts
    assert writes(b).count(1) == 1 + 4 and not b.sequencer.is_running("led", "1_01")


# ---- toggle ---------------------------------------------------------------------------------------------
def test_toggle_rule_flips_a_light_even_if_evok_never_reports_it(b, mod):
    b.local_logic.rules = [rule(mod, action_type="toggle")]
    seen = []
    for _ in range(3):
        b.device_states["di_1_01"] = 0
        press(b)
        seen += [c["value"] for c in queued(b)]
    assert seen == [1, 0, 1]                                    # tracked from our own acks, no evok echo needed


# ---- 'only when Home Assistant is unreachable' -------------------------------------------------------------
@pytest.mark.parametrize("ha_online,mqtt_up,acts", [
    (True, True, False),      # HA is up: HA is in charge, the local rule stays out of the way
    (False, True, True),      # HA announced 'offline'
    (None, True, True),       # never heard from HA
    (True, False, True),      # HA said online earlier but our MQTT link is down
])
def test_when_ha_offline_gating(b, mod, ha_online, mqtt_up, acts):
    b.local_logic.rules = [rule(mod, when="ha_offline")]
    b.ha_online, b.mqtt_client.connected = ha_online, mqtt_up
    b.device_states["di_1_01"] = 0
    press(b)
    assert (len(queued(b)) == 1) is acts


def test_when_always_ignores_ha_state(b, mod):
    b.local_logic.rules = [rule(mod, when="always")]
    b.ha_online, b.mqtt_client.connected = True, True
    b.device_states["di_1_01"] = 0
    press(b)
    assert len(queued(b)) == 1


@pytest.mark.parametrize("payload,retained,expected", [("online", False, True), ("offline", False, False), ("online", True, True)])
def test_ha_status_messages_are_tracked(b, payload, retained, expected):
    b.on_mqtt_message(None, None, fake_message(b.ha_status_topic, payload, retain=retained))
    assert b.ha_online is expected


# ---- dimmer -------------------------------------------------------------------------------------------------
def dim_rule(mod, **kw):
    return rule(mod, action_type="dimmer", action_dev="ao", action_circuit="xS51_01", trigger_operator="any",
                trigger_value=None, action_value=5, **kw)


def test_dimmer_uses_the_configured_level_and_remembers_it(b, mod):
    r = dim_rule(mod)
    b.local_logic.rules = [r]
    b.device_states["ao_xS51_01"] = 0.0
    for expect, ao in ((5.0, 0.0), (0.0, 5.0), (5.0, 0.0)):
        b.device_states["ao_xS51_01"] = ao
        b.device_states["di_1_01"] = 0
        press(b, value=1); press(b, value=0)
        settle(b)
        assert [c["value"] for c in queued(b)] == [expect]
    assert b.dimmer_states[r.id]["previous_level"] == 5.0


def test_dimmer_level_survives_a_restart(b, mod, tmp_path):
    r = dim_rule(mod)
    b.local_logic.rules = [r]
    b.device_states["ao_xS51_01"] = 7.0                         # it was dimmed to 7 V, then switched off
    b.device_states["di_1_01"] = 0
    press(b, value=1); press(b, value=0); settle(b)            # OFF: remembers 7 V
    saved = json.loads((tmp_path / "local_rules_state.json").read_text())
    assert saved[r.id]["previous_level"] == 7.0
    b.dimmer_states.clear(); b.dimmer_persist = b._load_dimmer_state()      # "restart"
    b.device_states["ao_xS51_01"] = 0.0
    press(b, value=1); press(b, value=0); settle(b)
    assert [c["value"] for c in queued(b)][-1] == 7.0           # comes back at the last level, not at the default


def test_dimmer_without_hold_toggles_on_press_and_ignores_release(b, mod):
    r = rule(mod, action_type="dimmer", action_dev="ao", action_circuit="xS51_01", action_value=10, dimmer_hold=False)
    b.local_logic.rules = [r]
    b.device_states["ao_xS51_01"] = 0.0
    b.device_states["di_1_01"] = 0
    press(b, value=1)
    assert [c["value"] for c in queued(b)] == [10.0]           # immediately, no waiting for the release
    b.process_websocket_message({"dev": "di", "circuit": "1_01", "value": 0})
    assert queued(b) == []


# ---- rules vs. running sequences -----------------------------------------------------------------------------
def test_a_set_rule_overrides_a_running_sequence(b, mod):
    run(b, b.sequencer.start("led", "1_01", __import__("unipi_core.sequencer", fromlist=["Timed"]).Timed(True, 20)))
    run(b, b.ft.advance_to(1))
    b.local_logic.rules = [rule(mod, action_value=0)]
    b.device_states["di_1_01"] = 0
    press(b)
    run(b, b.ft.advance_to(30))
    assert not b.sequencer.is_running("led", "1_01")
    assert b.sent_times == [0.0, 1.0] and writes(b) == [1, 0]  # cancelled at t=1 (OFF), never "late OFF at 20"


# ---- API -----------------------------------------------------------------------------------------------------------
def req(body):
    async def j():
        return body
    return SimpleNamespace(json=j)


def test_api_rejects_invalid_rules_and_keeps_the_old_set(b, mod, tmp_path):
    ok = rule(mod, name="keep")
    b.local_logic.replace_rules([ok])
    resp = run(b, b.web_handler_update_rules(req([rule(mod, name="x").model_dump(),
                                                  rule(mod, name="bad", action_type="pulse", action_pulse={"count": 50}).model_dump()])))
    assert resp.status == 400 and "bad" in resp.text and "count" in resp.text
    assert [r.name for r in b.local_logic.rules] == ["keep"]                       # unchanged
    resp = run(b, b.web_handler_add_rule(req(rule(mod, action_type="toggle", action_dev="ao").model_dump())))
    assert resp.status == 400
    resp = run(b, b.web_handler_update_rules(req([rule(mod, name="a", action_type="pulse", action_preset="ring3").model_dump()])))
    assert resp.status == 200 and [r.name for r in b.local_logic.rules] == ["a"]


def test_shipped_example_rules_are_valid(b, mod):
    from pathlib import Path
    data = json.loads((Path(__file__).resolve().parent.parent / "local_rules.example.json").read_text())
    b.config.circuits = {}                                            # defaults only
    for item in data:
        assert b.validate_rule(mod.LocalLogicRule(**item)) is None, item["name"]


def test_rule_group_is_kept_through_the_api_and_limited(b, mod, tmp_path):
    r = rule(mod, name="g", group="Serre")
    assert b.validate_rule(r) is None
    resp = run(b, b.web_handler_update_rules(req([r.model_dump()])))
    assert resp.status == 200
    saved = json.loads((tmp_path / "local_rules.json").read_text())
    assert saved[0]["group"] == "Serre"
    shown = json.loads(run(b, b.web_handler_get_rules(SimpleNamespace())).text)
    assert shown[0]["group"] == "Serre"
    assert "40 characters" in b.validate_rule(rule(mod, name="long", group="x" * 41))
    assert rule(mod, name="old").group == ""                           # rules saved before groups existed still load
