"""Output sequencer (T12): exact timing, limits, cancellation, safety. Fake clock, no real waiting."""
import asyncio

import pytest
from conftest import REAL_SLEEP

from unipi_core.sequencer import (Limits, OutputSequencer, Pulse, SequenceRejected, Timed,
                                  WebSocketUnavailable, make_pulse, parse_command)

KEY = ("ro", "2_02")


class FakeTime:
    """Deterministic time: sleepers wake only when the test advances the clock, in deadline order."""

    def __init__(self):
        self.t = 0.0
        self._sleepers = []

    def clock(self):
        return self.t

    async def sleep(self, dt):
        fut = asyncio.get_running_loop().create_future()
        self._sleepers.append((self.t + dt, fut))
        await fut

    async def _yield(self, n=20):
        for _ in range(n):
            await REAL_SLEEP(0)

    async def advance_to(self, target):
        await self._yield()
        while True:
            due = sorted((d, i) for i, (d, f) in enumerate(self._sleepers) if d <= target + 1e-9 and not f.done())
            if not due:
                break
            d, i = due[0]
            self.t = max(self.t, d)
            self._sleepers[i][1].set_result(None)
            await self._yield()
        self.t = target
        await self._yield()


class Rig:
    def __init__(self, limits=None, ready=True, latency=0.0):
        self.ft = FakeTime()
        self.limits = limits or Limits()
        self.ready = ready
        self.latency = latency
        self.writes, self.acks, self.attrs = [], [], []
        self.fail_writes = False
        self.seq = OutputSequencer(write=self._write, ack=self._ack, set_attributes=self._attrs,
                                   limits_for=lambda d, c: self.limits, is_ready=lambda: self.ready,
                                   clock=self.ft.clock, sleep=self.ft.sleep)

    async def _write(self, dev, circuit, value):
        if self.fail_writes:
            raise WebSocketUnavailable("down")
        self.writes.append((round(self.ft.t, 3), value))
        self.ft.t += self.latency

    def _ack(self, dev, circuit, on, origin):
        self.acks.append((round(self.ft.t, 3), on))

    def _attrs(self, dev, circuit, a):
        self.attrs.append(a)


def run(coro):
    return asyncio.new_event_loop().run_until_complete(coro)


# ---- pulse timing ---------------------------------------------------------------------------------
def test_three_rings_exact_timing():
    async def go():
        r = Rig()
        await r.seq.start(*KEY, Pulse(3, 100, 250))
        await r.ft.advance_to(2.0)
        assert r.writes == [(0.0, 1), (0.1, 0), (0.35, 1), (0.45, 0), (0.7, 1), (0.8, 0)]
        assert r.acks == [(0.0, True), (0.8, False)]            # ON while it runs, OFF when done
        assert r.attrs[0]["busy"] is True and r.attrs[0]["remaining"] == 3 and "ends_at" in r.attrs[0]
        assert r.attrs[-1] == {"busy": False}
        assert not r.seq.is_running(*KEY)
    run(go())


def test_write_latency_does_not_accumulate_drift():
    async def go():
        r = Rig(latency=0.005)       # every write takes 5 ms
        await r.seq.start(*KEY, Pulse(4, 100, 250))
        await r.ft.advance_to(3.0)
        ons = [t for t, v in r.writes if v == 1]
        for k, t in enumerate(ons):   # absolute deadlines: the k-th ring starts at k*0.35 (+ at most one write latency)
            assert abs(t - k * 0.35) < 0.011
    run(go())


def test_timed_on_then_off_and_inverse():
    async def go():
        r = Rig()
        await r.seq.start(*KEY, Timed(True, 35))
        await r.ft.advance_to(36)
        assert r.writes == [(0.0, 1), (35.0, 0)] and r.acks == [(0.0, True), (35.0, False)]
        r2 = Rig()
        await r2.seq.start(*KEY, Timed(False, 5))               # off for 5 s, then on again
        await r2.ft.advance_to(6)
        assert r2.writes == [(0.0, 0), (5.0, 1)] and r2.acks == [(0.0, False), (5.0, True)]
    run(go())


# ---- command parsing and limits -------------------------------------------------------------------
L = Limits()


@pytest.mark.parametrize("payload,expected", [
    ({"pulse": {"count": 3, "on_ms": 100, "off_ms": 250}}, Pulse(3, 100, 250)),
    ({"pulse": {"count": 2}}, Pulse(2, 100, 250)),                       # circuit defaults
    ({"pulse": {"count": "2"}}, Pulse(2, 100, 250)),                     # legacy-style string numbers
    ({"pulse": {"count": 2.0, "on_ms": "150"}}, Pulse(2, 150, 250)),
    ({"state": "ON", "duration_s": 35}, Timed(True, 35.0)),
    ({"state": "off", "duration_s": "0.5"}, Timed(False, 0.5)),
    ({"state": "ON"}, None), ({"state": "off"}, None),
])
def test_parse_ok(payload, expected):
    assert parse_command(payload, L) == expected


@pytest.mark.parametrize("payload,why", [
    ({"pulse": {"count": 0}}, "count"), ({"pulse": {"count": 11}}, "count"), ({"pulse": {"count": 1.5}}, "whole"),
    ({"pulse": {"count": True}}, "whole"), ({"pulse": {"count": "x"}}, "whole"),
    ({"pulse": {"count": 1, "on_ms": 10}}, "on_ms"), ({"pulse": {"count": 1, "on_ms": 2001}}, "on_ms"),
    ({"pulse": {"count": 1, "off_ms": 49}}, "off_ms"), ({"pulse": {"count": 1, "off_ms": 10001}}, "off_ms"),
    ({"pulse": 3}, "object"), ({"pulse": {"on_ms": 100}}, "object"), ({"pulse": {"count": 1, "bogus": 1}}, "bogus"),
    ({"state": "ON", "duration_s": 0.05}, "duration_s"), ({"state": "ON", "duration_s": 3601}, "duration_s"),
    ({"duration_s": 5}, "needs a 'state'"), ({"state": "maybe", "duration_s": 5}, "ON or OFF"),
    ({"pulse": {"count": 1}, "duration_s": 5, "state": "ON"}, "only one"), ({"brightness": 5}, "brightness"),
    ({"preset": "nope"}, "unknown preset"), ({}, "nothing to do"),
])
def test_parse_rejects_instead_of_clamping(payload, why):
    with pytest.raises(SequenceRejected) as e:
        parse_command(payload, L)
    assert why in str(e.value)


def test_circuit_limits_are_honoured():
    tight = Limits(max_count=2, max_pulse_ms=200, max_on_s=2.0, on_ms=80, off_ms=120)
    assert parse_command({"pulse": {"count": 2}}, tight) == Pulse(2, 80, 120)   # circuit defaults
    for bad in ({"pulse": {"count": 3}}, {"pulse": {"count": 1, "on_ms": 250}}, {"state": "ON", "duration_s": 5}):
        with pytest.raises(SequenceRejected):
            parse_command(bad, tight)


def test_programmatic_specs_are_revalidated():
    async def go():
        r = Rig()
        with pytest.raises(SequenceRejected):
            await r.seq.start(*KEY, Pulse(99, 100, 250))
        assert r.writes == []
    run(go())


# ---- cancellation, readiness, failures ------------------------------------------------------------
def test_new_command_cancels_running_one_through_off():
    async def go():
        r = Rig()
        await r.seq.start(*KEY, Pulse(5, 100, 250))
        await r.ft.advance_to(0.05)                              # coil is energised right now
        await r.seq.start(*KEY, Timed(True, 2))
        await r.ft.advance_to(5.0)
        assert r.writes == [(0.0, 1), (0.05, 0), (0.05, 1), (2.05, 0)]   # OFF *before* the new ON; old task silent
        assert r.seq.is_running(*KEY) is False
    run(go())


def test_cancel_drives_off_and_acks():
    async def go():
        r = Rig()
        await r.seq.start(*KEY, Pulse(5, 100, 250))
        await r.ft.advance_to(0.05)
        assert await r.seq.cancel(*KEY, "test") is True
        assert r.writes[-1] == (0.05, 0) and r.acks[-1] == (0.05, False) and r.attrs[-1] == {"busy": False}
        assert await r.seq.cancel(*KEY) is False                  # nothing left to cancel
    run(go())


def test_rejected_when_websocket_not_open_and_nothing_is_sent():
    async def go():
        r = Rig(ready=False)
        with pytest.raises(SequenceRejected) as e:
            await r.seq.start(*KEY, Pulse(1, 100, 250))
        assert "not open" in str(e.value) and r.writes == [] and r.ft._sleepers == []
    run(go())


def test_websocket_loss_mid_sequence_aborts_and_off_is_retried_later():
    async def go():
        r = Rig()
        await r.seq.start(*KEY, Pulse(3, 100, 250))
        await r.ft.advance_to(0.05)
        r.fail_writes = True                                      # connection drops before the OFF write
        await r.ft.advance_to(1.0)
        assert r.writes == [(0.0, 1)] and not r.seq.is_running(*KEY)
        assert r.acks[-1][1] is False and "aborted" in r.attrs[-1]["last_error"]
        assert KEY in r.seq._needs_failsafe
        r.fail_writes = False                                     # connection is back
        await r.seq.failsafe_all([])
        assert r.writes[-1][1] == 0 and KEY not in r.seq._needs_failsafe
    run(go())


def test_failsafe_all_and_failures_are_remembered():
    async def go():
        r = Rig()
        r.fail_writes = True
        await r.seq.failsafe_all([KEY, ("do", "1_01")])
        assert r.seq._needs_failsafe == {KEY, ("do", "1_01")}
        r.fail_writes = False
        await r.seq.failsafe_all([])
        assert sorted(v for _, v in r.writes) == [0, 0] and not r.seq._needs_failsafe
    run(go())


# ---- watchdog ---------------------------------------------------------------------------------------
def test_watchdog_forces_off_after_max_on_s_only_when_configured():
    async def go():
        r = Rig(limits=Limits(max_on_s=2.0, watchdog=True))
        r.seq.note_state(*KEY, 1)
        await r.ft.advance_to(1.9); await r.seq.watchdog_tick()
        assert r.writes == []
        await r.ft.advance_to(2.5); await r.seq.watchdog_tick()
        assert r.writes == [(2.5, 0)] and "max_on_s" in r.attrs[-1]["last_error"] and r.acks[-1][1] is False
        r.seq.note_state(*KEY, 1); r.seq.note_state(*KEY, 0)       # turned off normally: forgotten
        await r.ft.advance_to(10); await r.seq.watchdog_tick()
        assert len(r.writes) == 1
        plain = Rig(limits=Limits())                                # no explicit max_on_s: no watchdog
        plain.seq.note_state(*KEY, 1)
        await plain.ft.advance_to(9999); await plain.seq.watchdog_tick()
        assert plain.writes == []
    run(go())


def test_cancel_all():
    async def go():
        r = Rig()
        await r.seq.start("ro", "1", Pulse(5, 100, 250)); await r.seq.start("ro", "2", Timed(True, 9))
        await r.ft.advance_to(0.05)
        await r.seq.cancel_all()
        assert sorted((v) for _, v in r.writes)[:2] == [0, 0] and not r.seq.is_running("ro", "1") and not r.seq.is_running("ro", "2")
    run(go())
