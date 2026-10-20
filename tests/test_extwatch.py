"""Extension watchdog: restart evok when a serial extension goes silent - and never become a restart loop."""
import asyncio
import json

import pytest
from conftest import drain, run

from unipi_core.extwatch import (DISABLED, DOWN, HARDWARE_SUSPECTED, LIMIT_REACHED, NO_EXTENSION, OK, REBOOT,
                                 RESTART_EVOK, STARTING, WAITING, ExtensionWatchdog, WatchdogSettings, is_extension)

NEURON = {"dev": "modbus_slave", "circuit": "1", "modbus_type": "TCP", "modbus_spec": "127.0.0.1", "last_comm": 0.01}


def xs51(last_comm):
    return {"dev": "modbus_slave", "circuit": "xS51", "modbus_type": "RTU", "modbus_spec": "/dev/ttyNS0", "last_comm": last_comm}


class Clock:
    def __init__(self):
        self.t = 1_000_000.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


@pytest.fixture
def clk():
    return Clock()


def make(clk, tmp_path=None, uptime=10_000.0, **kw):
    f = str(tmp_path / "wd.json") if tmp_path else None
    return ExtensionWatchdog(WatchdogSettings(**kw), state_file=f, clock=clk, uptime=lambda: uptime)


def silent(wd, clk, step=60, last_comm=5000.0):
    """Feed one 'extension silent' observation, then let `step` seconds pass."""
    a = wd.observe([NEURON, xs51(last_comm)])
    clk.advance(step)
    return a


def test_extension_detection_is_serial_modbus_only():
    assert is_extension(xs51(1)) and not is_extension(NEURON)
    assert is_extension({"modbus_spec": "/dev/ttyUSB0"})


def test_a_neuron_without_extension_is_never_touched(clk):
    wd = make(clk)
    clk.advance(10_000)
    for _ in range(50):
        assert wd.observe([dict(NEURON, last_comm=99999)]) is None             # even a silent TCP board is not an extension
        clk.advance(60)
    assert wd.state == NO_EXTENSION and wd.history == []


def test_nothing_happens_during_startup_grace_or_for_a_healthy_extension(clk):
    wd = make(clk)
    assert silent(wd, clk, 10) is None and wd.state == STARTING             # evok's last_comm is meaningless right after start
    clk.advance(300)
    assert wd.observe([xs51(0.2)]) is None and wd.state == OK


def test_short_silence_is_left_alone_long_silence_restarts_evok_once(clk):
    wd = make(clk)
    clk.advance(300)
    assert wd.observe([xs51(120)]) is None                                     # below down_after_s (180)
    assert wd.observe([xs51(181)]) == RESTART_EVOK
    assert wd.state == WAITING
    clk.advance(60)
    assert wd.observe([xs51(9999)]) is None and wd.state == WAITING            # settle time: no second restart on a stale last_comm
    assert len(wd.history) == 1


def test_ladder_then_hardware_suspected_when_reboot_is_off(clk):
    wd = make(clk, reboot_enabled=False)
    clk.advance(300)
    acts = []
    for _ in range(400):                                                       # ~6.7 h of one-minute checks
        a = silent(wd, clk)
        if a:
            acts.append((round(clk.t - 1_000_300 - 60), a))
    kinds = [k for _, k in acts]
    assert kinds[:2] == [RESTART_EVOK, RESTART_EVOK] and REBOOT not in kinds
    assert acts[1][0] - acts[0][0] >= 600                                      # a pause before the second try
    assert wd.state == HARDWARE_SUSPECTED
    assert len(kinds) <= 3                                                     # the 6 h long retry is the only extra one


def test_reboot_only_when_enabled_old_enough_and_once_per_day(clk, tmp_path):
    wd = make(clk, tmp_path, reboot_enabled=True, uptime=60.0)
    clk.advance(300)
    seen = []
    for _ in range(100):
        a = silent(wd, clk); seen.append(a)
    assert REBOOT not in seen and wd.state == HARDWARE_SUSPECTED               # host booted a minute ago: never reboot it again

    clk2 = Clock(); wd = make(clk2, None, reboot_enabled=True, uptime=5000.0)
    clk2.advance(300)
    seen = [silent(wd, clk2) for _ in range(100)]
    assert [a for a in seen if a] == [RESTART_EVOK, RESTART_EVOK, REBOOT]


def test_history_survives_a_restart_so_a_reboot_cannot_repeat(clk, tmp_path):
    wd = make(clk, tmp_path, reboot_enabled=True)
    clk.advance(300)
    seen = [silent(wd, clk) for _ in range(100)]
    assert REBOOT in seen
    clk.advance(120)                                                           # the host came back; a new process, same history
    wd2 = make(clk, tmp_path, reboot_enabled=True, startup_grace_s=0)
    seen2 = [silent(wd2, clk) for _ in range(200)]
    assert REBOOT not in seen2                                                 # max 1 reboot per day, even across restarts
    assert wd2._recent(REBOOT) == 1


def test_daily_cap_stops_a_flapping_extension_from_causing_endless_restarts(clk, tmp_path):
    wd = make(clk, tmp_path, max_actions_per_day=3, ladder=(RESTART_EVOK,), wait_s=(0.0,), long_retry_s=0.0, settle_s=10.0)
    clk.advance(300)
    acts = []
    for _ in range(100):
        a = wd.observe([xs51(5000)]); clk.advance(30)
        if a: acts.append(a)
    assert len(acts) == 3 and wd.state == LIMIT_REACHED
    clk.advance(86400 + 1)                                                     # a day later it may try again
    assert wd.observe([xs51(5000)]) == RESTART_EVOK


def test_recovery_resets_the_ladder_and_remembers_it(clk):
    wd = make(clk)
    clk.advance(300)
    assert silent(wd, clk, 200) == RESTART_EVOK
    assert wd.observe([xs51(0.05)]) is None and wd.state == OK and wd.attempt == 0 and wd.last_recovery_ts
    assert silent(wd, clk, 60) == RESTART_EVOK                                 # next outage starts from step one again
    assert wd.status()["actions_24h"] == 2


def test_a_command_that_cannot_run_is_not_retried_every_minute(clk):
    wd = make(clk)
    clk.advance(300)
    assert silent(wd, clk, 1) == RESTART_EVOK
    wd.action_failed(RESTART_EVOK, "sudo: a password is required")
    seen = [silent(wd, clk) for _ in range(300)]
    assert RESTART_EVOK not in seen and wd.state == HARDWARE_SUSPECTED and wd.status()["blocked"] == [RESTART_EVOK]


def test_any_one_silent_extension_is_enough_and_disabled_means_disabled(clk):
    wd = make(clk)
    clk.advance(300)
    other = dict(xs51(0.1), circuit="xS30")
    assert wd.observe([other, xs51(900)]) == RESTART_EVOK
    off = make(clk, enabled=False)
    clk.advance(300)
    assert off.observe([xs51(99999)]) is None and off.state == DISABLED


# ---- the bridge: executes the step, tells Home Assistant ------------------------------------------------------------------
@pytest.fixture
def b(discovered, mod):
    discovered.mod = mod
    clk = Clock()
    discovered.clk = clk
    discovered.ext_watchdog.clock = clk
    discovered.ext_watchdog.started_at = clk() - 1000
    discovered.ext_watchdog.state_file = None
    discovered.ext_watchdog.history = []
    discovered.cmds = []
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def fake_exec(b, rc=0, err=b""):
    async def _exec(*cmd, **kw):
        b.cmds.append(list(cmd))

        class P:
            returncode = rc

            async def communicate(self):
                return b"", err
        return P()
    return _exec


def step(b, slaves):
    run(b, b._extension_watchdog_step(slaves))


def topics(b):
    return {t: p for t, p in drain(b.websocket_to_mqtt_queue)}


def test_bridge_restarts_evok_for_a_silent_extension_and_publishes_state(b, monkeypatch):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec(b))
    step(b, [NEURON, xs51(0.1)])
    assert b.cmds == [] and topics(b)[f"unipi/{b.device_name}_bridge/extension_watchdog/state"] == "ok"
    step(b, [NEURON, xs51(4000)])
    assert b.cmds == [["sudo", "-n", "systemctl", "restart", "evok"]]
    t = topics(b)
    assert t[f"unipi/{b.device_name}_bridge/extension_watchdog/state"] == "waiting"
    attrs = json.loads(t[f"unipi/{b.device_name}_bridge/extension_watchdog/attributes"])
    assert attrs["last_action"] == "restart_evok" and attrs["attempt"] == 1 and attrs["extensions"] == {"xS51": 4000.0}


def test_unchanged_state_is_not_republished_every_minute(b, monkeypatch):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec(b))
    step(b, [xs51(0.1)]); drain(b.websocket_to_mqtt_queue)
    step(b, [xs51(0.1)])
    assert [t for t in topics(b) if "extension_watchdog" in t] == []


def test_a_failing_restart_command_is_reported_and_not_repeated(b, monkeypatch):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec(b, rc=1, err=b"sudo: a password is required"))
    step(b, [xs51(4000)])
    assert len(b.cmds) == 1 and b.ext_watchdog.status()["blocked"] == ["restart_evok"]
    for _ in range(5):
        b.clk.advance(700)
        step(b, [xs51(4000)])
    assert len(b.cmds) == 1                                                    # never retried in a loop


def test_a_crashing_watchdog_never_stops_the_extension_monitor(b, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("boom")
    monkeypatch.setattr(asyncio, "create_subprocess_exec", boom)
    step(b, [xs51(4000)])                                                     # must not raise
    assert b.ext_watchdog.status()["blocked"] == ["restart_evok"]


def test_shadow_instance_and_disabled_config_never_act(b, monkeypatch, mod):
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec(b))
    for shadow, enabled in ((True, True), (False, False)):
        w = b.ext_watchdog
        w.s.enabled = enabled and not shadow
        step(b, [xs51(9999)])
    assert b.cmds == []


def test_discovery_publishes_the_watchdog_sensor_with_attributes(b):
    b.publish_bridge_discovery()
    cfgs = {t: json.loads(p) for t, p in topics(b).items() if t.endswith("_ext_watchdog/config")}
    assert len(cfgs) == 1
    c = next(iter(cfgs.values()))
    assert c["name"] == "Extension watchdog" and c["state_topic"].endswith("/extension_watchdog/state")
    assert c["json_attributes_topic"].endswith("/extension_watchdog/attributes") and c["unique_id"].endswith("_ext_watchdog")
    assert c["dev"]["identifiers"] == [f"{b.device_name}_bridge"]              # next to the other bridge/extension sensors


def test_watchdog_attributes_are_retained(b):
    from pathlib import Path
    src = (Path(__file__).resolve().parent.parent / "hass-unipi.py").read_text()
    assert '"/extension_watchdog/attributes"' in src                          # in the MQTT worker's retain list


def test_config_defaults_are_safe(mod):
    e = mod.ExtensionsConfig()
    assert e.watchdog is True and e.watchdog_reboot is False                   # reboot is opt-in
    assert e.watchdog_restart_command == ["sudo", "-n", "systemctl", "restart", "evok"]
    with pytest.raises(Exception):
        mod.ExtensionsConfig(watchdog_down_after_s=5)                          # too eager would restart on every glitch


def test_shadow_instance_is_built_with_the_watchdog_off(mod, tmp_path, monkeypatch):
    from test_shadow import make
    sh = make(mod, tmp_path, monkeypatch, "shadow", "s")
    assert sh.ext_watchdog.s.enabled is False                                  # a read-only twin must never restart evok
    live = make(mod, tmp_path, monkeypatch, "live", "l")
    assert live.ext_watchdog.s.enabled is True


def test_the_real_monitor_loop_feeds_the_watchdog_from_evok_rest(b, monkeypatch, fast_sleep):
    """End to end through _monitor_extensions: evok REST says xS51 is silent -> evok gets restarted."""
    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec(b))
    rest = [NEURON, xs51(4000)]

    class Resp:
        status = 200

        async def json(self):
            return rest

        async def __aenter__(self):
            return self

        async def __aexit__(self, *a):
            return False

    class Session:
        def get(self, url, **kw):
            b.should_stop.set()                                                # one cycle only
            return Resp()
    b.session = Session()
    run(b, b._monitor_extensions())
    assert b.cmds == [["sudo", "-n", "systemctl", "restart", "evok"]]
