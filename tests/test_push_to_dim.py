"""Push-to-dim like a Z-Wave dimmer: short press toggles, press-and-hold dims, release stops."""
import json

import pytest
from conftest import drain, fx, run
from test_sequencer import FakeTime

AO = ("ao", "xS51_01")
BTN = "1_01"


@pytest.fixture
def b(discovered, mod):
    ft = FakeTime()
    discovered._dim_sleep = ft.sleep
    discovered.ft, discovered.mod = ft, mod
    discovered.device_states[f"di_{BTN}"] = 0
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def setup(b, ao_level=0.0, **kw):
    """One dimmer rule; the lamp starts at ao_level volts."""
    fields = dict(name="dim", trigger_dev="di", trigger_circuit=BTN, trigger_operator="eq", trigger_value=1,
                  action_type="dimmer", action_dev="ao", action_circuit=AO[1], action_value=6,
                  dimmer_hold_ms=800, dimmer_speed=2.5, dimmer_min=1.0)
    fields.update(kw)
    b.local_logic.rules = [b.mod.LocalLogicRule(**fields)]
    b.device_states["ao_xS51_01"] = ao_level
    drain(b.mqtt_to_websocket_queue)


def button(b, value):
    b.process_websocket_message({"dev": "di", "circuit": BTN, "value": value})


def hold(b, seconds):
    """Press, keep it down for `seconds`, release. Returns the volts written to the lamp, in order."""
    button(b, 1)
    run(b, b.ft.advance_to(b.ft.t + seconds))
    button(b, 0)
    run(b, b.ft.advance_to(b.ft.t + 0.05))
    return [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)]


def tap(b, seconds=0.2):
    return hold(b, seconds)


# ---- short press = toggle -------------------------------------------------------------------------------------------
def test_short_press_switches_on_to_the_configured_level_then_off(b):
    setup(b, ao_level=0.0)
    assert tap(b) == [6.0]
    b.device_states["ao_xS51_01"] = 6.0
    assert tap(b) == [0.0]


def test_a_press_shorter_than_the_hold_time_never_dims(b):
    setup(b, ao_level=5.0, dimmer_hold_ms=1000)
    assert hold(b, 0.9) == [0.0]                                      # 0.9 s < 1.0 s: just a short press (toggle off)


# ---- press and hold = dim ---------------------------------------------------------------------------------------------
def test_holding_dims_up_after_the_hold_time_and_stops_on_release(b):
    setup(b, ao_level=5.0, dimmer_hold_ms=1000)
    vals = hold(b, 2.5)                                               # dimming runs for 1.5 s at 2.5 V/s
    assert vals == sorted(vals) and vals[0] > 5.0 and 8.7 <= vals[-1] <= 9.1       # 1.5 s at 2.5 V/s, first step immediately
    assert 0.0 not in vals                                            # release after dimming must not toggle the lamp off
    assert b.dimmer_states[b.local_logic.rules[0].id]["previous_level"] == pytest.approx(vals[-1], abs=0.01)
    run(b, b.ft.advance_to(b.ft.t + 5))
    assert drain(b.mqtt_to_websocket_queue) == []                     # nothing keeps dimming after the release


def test_the_next_hold_goes_the_other_way(b):
    setup(b, ao_level=5.0)
    up = hold(b, 2.0)
    b.device_states["ao_xS51_01"] = up[-1]
    down = hold(b, 2.0)
    assert up == sorted(up) and down == sorted(down, reverse=True) and down[-1] < up[-1]


def test_at_full_brightness_a_hold_dims_down_first(b):
    setup(b, ao_level=10.0)
    vals = hold(b, 2.0)
    assert vals and vals[0] < 10.0 and vals == sorted(vals, reverse=True)


def test_an_off_lamp_comes_on_at_the_minimum_and_brightens(b):
    setup(b, ao_level=0.0, dimmer_min=1.5)
    vals = hold(b, 2.0)
    assert vals[0] == 1.5 and vals == sorted(vals) and vals[-1] > 1.5


def test_dimming_down_stops_at_the_minimum_and_never_switches_off(b):
    setup(b, ao_level=3.0, dimmer_min=1.0)
    b.dimmer_persist[b.local_logic.rules[0].id] = {"last_direction": -1, "previous_level": 3.0}   # this hold goes down
    vals = hold(b, 5.0)                                               # long enough to run into the floor
    assert vals and min(vals) == 1.0 and 0.0 not in vals and vals[-1] == 1.0


def test_hold_time_and_speed_are_per_rule_settings(b):
    setup(b, ao_level=2.0, dimmer_hold_ms=500, dimmer_speed=5.0)
    vals = hold(b, 1.5)                                               # 1.0 s of dimming at 5 V/s
    assert 6.9 <= vals[-1] <= 7.6                                     # first step is immediate: 11 steps of 0.5 V


# ---- the button wiring ---------------------------------------------------------------------------------------------------
def test_the_trigger_operator_does_not_matter_for_push_to_dim(b):
    for op, val in (("eq", 1), ("any", None), ("eq", 0), ("gt", 5)):
        setup(b, ao_level=5.0, trigger_operator=op, trigger_value=val)
        got = b.local_logic.evaluate({"dev": "di", "circuit": BTN, "value": 1}) + b.local_logic.evaluate({"dev": "di", "circuit": BTN, "value": 0})
        assert len(got) == 2, (op, val)                                # both press and release reach the dimmer


def test_without_hold_only_the_press_acts(b):
    setup(b, ao_level=0.0, dimmer_hold=False)
    button(b, 1)
    assert [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)] == [6.0]
    run(b, b.ft.advance_to(5))
    button(b, 0)
    assert drain(b.mqtt_to_websocket_queue) == []


def test_a_failing_condition_blocks_the_press_but_never_the_release(b):
    cond = b.mod.RuleCondition(dev="di", circuit="1_02", operator="eq", value=1)
    setup(b, ao_level=5.0, conditions=[cond])
    b.device_states["di_1_02"] = 0
    assert hold(b, 2.0) == []                                          # condition false: the press is ignored
    b.device_states["di_1_02"] = 1
    button(b, 1)
    run(b, b.ft.advance_to(b.ft.t + 2.0))                              # dimming is under way
    assert drain(b.mqtt_to_websocket_queue)
    b.device_states["di_1_02"] = 0                                     # the condition turns false while held
    button(b, 0)
    drain(b.mqtt_to_websocket_queue)
    run(b, b.ft.advance_to(b.ft.t + 3.0))
    assert drain(b.mqtt_to_websocket_queue) == []                      # ...and the release still stopped the dimming


def test_home_assistant_is_kept_in_step_without_flooding_mqtt(b):
    setup(b, ao_level=2.0, dimmer_hold_ms=500)
    hold(b, 2.6)                                                       # 22 dimming steps: the last periodic ack is at step 20,
                                                                       # so only the ack on release can report the real final level
    acks = [p for t, p in drain(b.websocket_to_mqtt_queue) if t.endswith("/ao/xS51_01/state")]
    assert 3 <= len(acks) <= 9                                         # a handful, not 30
    final = json.loads(acks[-1])
    assert final["state"] == "ON" and final["brightness"] == int(b.device_states["ao_xS51_01"] * 100)   # last ack = where it stopped


def test_the_trace_explains_a_push_to_dim(b):
    setup(b, ao_level=5.0)
    hold(b, 2.0)
    ev = b.local_logic.trace_since(0)["events"]
    assert any(e["step"] == "trigger" and "push-to-dim" in e["detail"] for e in ev)
    assert any(e["step"] == "executed" and "dimming stopped at" in e["detail"] for e in ev)


# ---- fade when tapped (on / off, separately) ------------------------------------------------------------------------------
from conftest import settle   # noqa: E402


def writes_after(b, seconds=0.0):
    settle(b, 0.4)
    return [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)]


def test_a_tap_that_switches_on_can_fade_up(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=1000)
    button(b, 1); button(b, 0)
    # BEFORE the fade has run a single step: HA already shows the wanted end state, nothing was written yet
    acks = [json.loads(p) for t, p in drain(b.websocket_to_mqtt_queue) if t.endswith("/ao/xS51_01/state")]
    assert acks and acks[0]["state"] == "ON" and acks[0]["brightness"] == 600
    assert drain(b.mqtt_to_websocket_queue) == []
    vals = writes_after(b)
    assert len(vals) >= 5 and vals == sorted(vals) and 0 < vals[0] < 6.0 and vals[-1] == 6.0     # then gradual, ending on the level


def test_a_tap_that_switches_off_can_fade_down(b, fast_sleep):
    setup(b, ao_level=6.0, dimmer_fade_off_ms=1000)
    button(b, 1); button(b, 0)
    vals = writes_after(b)
    assert len(vals) >= 5 and vals == sorted(vals, reverse=True) and vals[-1] == 0.0 and vals[0] > 0.0


def test_fade_on_and_fade_off_are_independent(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=0, dimmer_fade_off_ms=1000)
    button(b, 1); button(b, 0)
    assert writes_after(b) == [6.0]                                    # on: instant
    b.device_states["ao_xS51_01"] = 6.0
    b.device_states["di_1_01"] = 0
    button(b, 1); button(b, 0)
    off = writes_after(b)
    assert len(off) >= 5 and off[-1] == 0.0                            # off: fades


def test_no_fade_configured_stays_instant(b, fast_sleep):
    setup(b, ao_level=0.0)
    button(b, 1); button(b, 0)
    assert writes_after(b) == [6.0]


def test_a_second_tap_during_a_fade_reverses_it(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=2000, dimmer_fade_off_ms=2000)
    button(b, 1); button(b, 0)                                         # start fading up...
    b.device_states["ao_xS51_01"] = 3.0                                # ...evok reports it is half way
    drain(b.mqtt_to_websocket_queue)
    button(b, 1); button(b, 0)                                         # tap again: that means "off"
    vals = writes_after(b)
    assert vals and vals[-1] == 0.0 and max(vals) < 6.0                # fades down from where it was, never up to 6 V


def test_holding_takes_over_from_a_running_fade(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=3000)
    button(b, 1); button(b, 0)                                         # fade up started
    assert ("ao", "xS51_01") in b.active_ao_transitions
    b.device_states["ao_xS51_01"] = 2.0
    button(b, 1)                                                       # press and hold...
    run(b, b.ft.advance_to(b.ft.t + 1.5))
    assert ("ao", "xS51_01") not in b.active_ao_transitions            # ...the fade is cancelled, the hand dims
    button(b, 0)


def test_fade_times_are_validated(b):
    for kw in (dict(dimmer_fade_on_ms=-1), dict(dimmer_fade_off_ms=60001)):
        setup(b)
        assert "0-60000" in (b.validate_rule(b.mod.LocalLogicRule(**{**dict(
            name="d", trigger_dev="di", trigger_circuit=BTN, action_type="dimmer", action_dev="ao", action_circuit=AO[1]), **kw})) or "")


def test_the_activity_log_says_the_lamp_is_fading(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=1500)
    button(b, 1); button(b, 0)
    ex = [e for e in b.local_logic.trace_since(0)["events"] if e["step"] == "executed"]
    assert "fading over 1500 ms" in ex[-1]["detail"]
    settle(b, 0.3)                                                     # let the fade finish before the test ends


def test_an_instant_tap_cancels_a_running_fade(b, fast_sleep):
    setup(b, ao_level=0.0, dimmer_fade_on_ms=2000, dimmer_fade_off_ms=0)       # up: fades, down: instant
    button(b, 1); button(b, 0)                                                 # fade up starts (not a single step run yet)
    b.device_states["ao_xS51_01"] = 3.0                                        # evok reports it is half way
    b.device_states["di_1_01"] = 0
    button(b, 1); button(b, 0)                                                 # tap = off, instantly
    assert writes_after(b) == [0.0]                                            # the old fade must NOT carry on up to 6 V


def test_holding_takes_over_from_a_fade_that_is_still_running(b, monkeypatch):
    import asyncio
    monkeypatch.setattr(asyncio, "sleep", b.ft.sleep)                          # fade and dimmer share ONE fake clock
    setup(b, ao_level=0.0, dimmer_fade_on_ms=3000, dimmer_hold_ms=300)
    b.dimmer_persist[b.local_logic.rules[0].id] = {"last_direction": -1, "previous_level": 6.0}   # this hold dims DOWN
    button(b, 1); button(b, 0)                                                 # t=0: fade up to 6 V starts (+0.2 V per 0.1 s)
    run(b, b.ft.advance_to(0.7))
    b.device_states["ao_xS51_01"] = 1.4                                        # evok: the lamp is at 1.4 V
    b.device_states["di_1_01"] = 0
    drain(b.mqtt_to_websocket_queue)
    button(b, 1)                                                               # t=0.7: press and hold (hold time 0.3 s)
    run(b, b.ft.advance_to(1.05))                                              # the fade legitimately runs until the hold time is over
    drain(b.mqtt_to_websocket_queue)                                           # ...from t=1.0 the hand is in charge
    run(b, b.ft.advance_to(2.5))
    button(b, 0)
    vals = [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)]
    assert vals and max(vals) <= 1.4 and vals[-1] == 1.0                       # only the hand dims; the fade did not keep climbing
