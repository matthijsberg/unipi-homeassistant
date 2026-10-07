"""Output sequencer: pulse trains (doorbell) and timed outputs (roof window) with hard limits.

Normative spec: docs/knowledge/context/interface-core.md section 2 and ADR-003. Summary:
  * one asyncio task per output circuit; a new command cancels the running one, and a cancelled
    sequence ALWAYS drives the output OFF before anything else happens (coil / motor protection)
  * commands outside the circuit limits are REJECTED, never clamped
  * no late execution: nothing is scheduled while the evok WebSocket is not open
  * writes go straight to the WebSocket (not through the 30 s hold queue); deadlines are absolute
    so timing does not drift
  * a watchdog forces OFF after max_on_s; failsafe_off circuits are driven OFF on start/reconnect/stop
Time and sleeping are injectable so the whole thing is testable without waiting.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Awaitable, Callable, Iterable

DEFAULT_ON_MS = 100      # legacy HA timing
DEFAULT_OFF_MS = 250
MIN_ON_MS = 20
MIN_OFF_MS = 50
MAX_OFF_MS = 10_000
MIN_DURATION_S = 0.1
DEFAULT_MAX_COUNT = 10
DEFAULT_MAX_PULSE_MS = 2000
DEFAULT_MAX_ON_S = 3600
ALLOWED_KEYS = {"pulse", "duration_s", "state", "preset"}


class SequenceRejected(Exception):
    """The command is invalid or outside the circuit's limits; nothing was switched."""


class WebSocketUnavailable(Exception):
    """The evok WebSocket is not open (or the write failed)."""


@dataclass(frozen=True)
class Limits:
    max_count: int = DEFAULT_MAX_COUNT
    max_pulse_ms: int = DEFAULT_MAX_PULSE_MS
    max_on_s: float = DEFAULT_MAX_ON_S
    on_ms: int = DEFAULT_ON_MS
    off_ms: int = DEFAULT_OFF_MS
    failsafe_off: bool = False
    watchdog: bool = False  # True only when max_on_s was configured explicitly


@dataclass(frozen=True)
class Pulse:
    count: int
    on_ms: int
    off_ms: int


@dataclass(frozen=True)
class Timed:
    on: bool          # level to hold for the duration (True = ON); the inverse follows
    duration_s: float


Spec = Pulse | Timed


def _int(name: str, v: Any) -> int:
    if isinstance(v, bool):
        raise SequenceRejected(f"'{name}' must be a whole number, got {v!r}")
    try:
        f = float(v)
    except (TypeError, ValueError):
        raise SequenceRejected(f"'{name}' must be a whole number, got {v!r}") from None
    if f != int(f):
        raise SequenceRejected(f"'{name}' must be a whole number, got {v!r}")
    return int(f)


def _num(name: str, v: Any) -> float:
    if isinstance(v, bool):
        raise SequenceRejected(f"'{name}' must be a number, got {v!r}")
    try:
        return float(v)
    except (TypeError, ValueError):
        raise SequenceRejected(f"'{name}' must be a number, got {v!r}") from None


def make_pulse(count: Any, on_ms: Any, off_ms: Any, limits: Limits) -> Pulse:
    """Fill defaults from the circuit and validate against its limits (reject, never clamp)."""
    count = _int("count", count)
    on_ms = limits.on_ms if on_ms is None else _int("on_ms", on_ms)
    off_ms = limits.off_ms if off_ms is None else _int("off_ms", off_ms)
    if not 1 <= count <= limits.max_count:
        raise SequenceRejected(f"count {count} outside 1..{limits.max_count}")
    if not MIN_ON_MS <= on_ms <= limits.max_pulse_ms:
        raise SequenceRejected(f"on_ms {on_ms} outside {MIN_ON_MS}..{limits.max_pulse_ms}")
    if not MIN_OFF_MS <= off_ms <= MAX_OFF_MS:
        raise SequenceRejected(f"off_ms {off_ms} outside {MIN_OFF_MS}..{MAX_OFF_MS}")
    return Pulse(count, on_ms, off_ms)


def make_timed(state: Any, duration_s: Any, limits: Limits) -> Timed:
    s = str(state).upper()
    if s not in ("ON", "OFF"):
        raise SequenceRejected(f"'state' must be ON or OFF when 'duration_s' is used, got {state!r}")
    d = _num("duration_s", duration_s)
    if not MIN_DURATION_S <= d <= limits.max_on_s:
        raise SequenceRejected(f"duration_s {d:g} outside {MIN_DURATION_S:g}..{limits.max_on_s:g}")
    return Timed(s == "ON", d)


def parse_command(payload: dict[str, Any], limits: Limits, presets: dict[str, Any] | None = None) -> Spec | None:
    """JSON command -> Pulse/Timed. Returns None for a plain {"state": "ON"|"OFF"} (no sequence)."""
    unknown = set(payload) - ALLOWED_KEYS
    if unknown:
        raise SequenceRejected(f"unsupported key(s) {sorted(unknown)} for a digital output (allowed: {sorted(ALLOWED_KEYS)})")
    kinds = [k for k in ("pulse", "duration_s", "preset") if k in payload]
    if len(kinds) > 1:
        raise SequenceRejected(f"give only one of pulse / duration_s / preset, got {kinds}")
    if "preset" in payload:
        name = payload["preset"]
        preset = (presets or {}).get(name)
        if preset is None:
            raise SequenceRejected(f"unknown preset {name!r}; configured: {sorted(presets or {})}")
        if getattr(preset, "pulse", None) is not None:
            p = preset.pulse
            return make_pulse(p.count, p.on_ms, p.off_ms, limits)
        t = preset.timed
        return make_timed(t.state, t.duration_s, limits)
    if "pulse" in payload:
        p = payload["pulse"]
        if not isinstance(p, dict) or "count" not in p:
            raise SequenceRejected('"pulse" must be an object like {"count": 3, "on_ms": 100, "off_ms": 250}')
        extra = set(p) - {"count", "on_ms", "off_ms"}
        if extra:
            raise SequenceRejected(f"unsupported pulse key(s) {sorted(extra)}")
        return make_pulse(p["count"], p.get("on_ms"), p.get("off_ms"), limits)
    if "duration_s" in payload:
        if "state" not in payload:
            raise SequenceRejected("'duration_s' needs a 'state' (ON or OFF)")
        return make_timed(payload["state"], payload["duration_s"], limits)
    if "state" in payload and str(payload["state"]).upper() in ("ON", "OFF"):
        return None
    raise SequenceRejected(f"nothing to do in {payload!r}")


Key = tuple[str, str]


class OutputSequencer:
    def __init__(
        self,
        *,
        write: Callable[[str, str, int], Awaitable[None]],
        ack: Callable[[str, str, bool, str], None],
        set_attributes: Callable[[str, str, dict[str, Any]], None],
        limits_for: Callable[[str, str], Limits],
        is_ready: Callable[[], bool],
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        logger: logging.Logger | None = None,
    ):
        self.write, self.ack, self.set_attributes = write, ack, set_attributes
        self.limits_for, self.is_ready = limits_for, is_ready
        self.clock, self.sleep = clock, sleep
        self.log = logger or logging.getLogger("OutputSequencer")
        self._tasks: dict[Key, asyncio.Task] = {}
        self._on_since: dict[Key, float] = {}
        self._needs_failsafe: set[Key] = set()

    # ---- public API ----------------------------------------------------------------------------
    def is_running(self, dev: str, circuit: str) -> bool:
        t = self._tasks.get((dev, circuit))
        return t is not None and not t.done()

    async def start(self, dev: str, circuit: str, spec: Spec, origin: str = "mqtt") -> None:
        key = (dev, circuit)
        limits = self.limits_for(dev, circuit)
        if not self.is_ready():
            raise SequenceRejected("evok WebSocket is not open; not scheduling (a late ring is worse than none)")
        if isinstance(spec, Pulse):
            spec = make_pulse(spec.count, spec.on_ms, spec.off_ms, limits)   # re-validate programmatic callers
        else:
            spec = make_timed("ON" if spec.on else "OFF", spec.duration_s, limits)
        await self.cancel(dev, circuit, "replaced by a new command")
        task = asyncio.get_running_loop().create_task(self._run(key, spec, origin), name=f"Sequence-{dev}-{circuit}")
        self._tasks[key] = task
        task.add_done_callback(lambda t, k=key: self._done(k, t))

    async def cancel(self, dev: str, circuit: str, reason: str = "cancelled") -> bool:
        """Cancel a running sequence. The task's own handler drives the output OFF before it ends."""
        t = self._tasks.get((dev, circuit))
        if t is None or t.done():
            return False
        self.log.info(f"Sequence {dev}/{circuit} cancelled: {reason}")
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        return True

    async def cancel_all(self, reason: str = "shutdown") -> None:
        for dev, circuit in list(self._tasks):
            await self.cancel(dev, circuit, reason)

    async def failsafe_all(self, keys: Iterable[Key], reason: str = "fail-safe") -> None:
        """Drive the given outputs OFF (used after start-up, WebSocket reconnect and at shutdown)."""
        for dev, circuit in set(keys) | self._needs_failsafe:
            try:
                await self.write(dev, circuit, 0)
                self._needs_failsafe.discard((dev, circuit))
                self.log.info(f"{reason}: {dev}/{circuit} driven OFF")
            except WebSocketUnavailable as e:
                self._needs_failsafe.add((dev, circuit))
                self.log.warning(f"{reason}: could not drive {dev}/{circuit} OFF ({e})")

    def note_state(self, dev: str, circuit: str, value: Any) -> None:
        """Feed observed output values (from evok) to the max-on watchdog."""
        key = (dev, circuit)
        try:
            on = int(float(value)) == 1
        except (TypeError, ValueError):
            return
        if on:
            self._on_since.setdefault(key, self.clock())
        else:
            self._on_since.pop(key, None)

    async def watchdog_tick(self) -> None:
        now = self.clock()
        for key, since in list(self._on_since.items()):
            limits = self.limits_for(*key)
            if not limits.watchdog or now - since <= limits.max_on_s:
                continue
            self._on_since.pop(key, None)
            msg = f"on for more than max_on_s={limits.max_on_s:g}s; forced OFF"
            self.log.warning(f"Watchdog {key[0]}/{key[1]}: {msg}")
            await self.cancel(*key, reason="watchdog")
            try:
                await self.write(key[0], key[1], 0)
            except WebSocketUnavailable:
                self._needs_failsafe.add(key)
            self.ack(key[0], key[1], False, "sequence")
            self.set_attributes(key[0], key[1], {"busy": False, "last_error": msg})

    # ---- the sequence itself -------------------------------------------------------------------
    def _done(self, key: Key, task: asyncio.Task) -> None:
        if self._tasks.get(key) is task:
            del self._tasks[key]
        if not task.cancelled() and task.exception():
            self.log.error(f"Sequence {key} crashed: {task.exception()!r}")

    async def _until(self, deadline: float) -> None:
        await self.sleep(max(0.0, deadline - self.clock()))

    async def _run(self, key: Key, spec: Spec, origin: str) -> None:
        dev, circuit = key
        try:
            if isinstance(spec, Pulse):
                await self._run_pulse(key, spec, origin)
            else:
                await self._run_timed(key, spec, origin)
        except asyncio.CancelledError:
            await self._abort_to_off(key, "cancelled")
            raise
        except WebSocketUnavailable as e:
            self._needs_failsafe.add(key)
            self.log.error(f"Sequence {dev}/{circuit} aborted: {e}; OFF will be re-sent when the WebSocket is back")
            self.ack(dev, circuit, False, origin)
            self.set_attributes(dev, circuit, {"busy": False, "last_error": f"aborted: {e}"})

    async def _abort_to_off(self, key: Key, why: str) -> None:
        dev, circuit = key
        try:
            await self.write(dev, circuit, 0)
        except WebSocketUnavailable:
            self._needs_failsafe.add(key)
        self.ack(dev, circuit, False, "sequence")
        self.set_attributes(dev, circuit, {"busy": False})

    def _ends_at(self, seconds: float) -> str:
        return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat(timespec="seconds")

    async def _run_pulse(self, key: Key, p: Pulse, origin: str) -> None:
        dev, circuit = key
        t0 = self.clock()
        period = (p.on_ms + p.off_ms) / 1000.0
        total = (p.count - 1) * period + p.on_ms / 1000.0
        for k in range(p.count):
            await self._until(t0 + k * period)
            await self.write(dev, circuit, 1)
            if k == 0:
                self.ack(dev, circuit, True, origin)
                self.set_attributes(dev, circuit, {"busy": True, "remaining": p.count, "ends_at": self._ends_at(total)})
            await self._until(t0 + k * period + p.on_ms / 1000.0)
            await self.write(dev, circuit, 0)
            if k < p.count - 1:
                self.set_attributes(dev, circuit, {"busy": True, "remaining": p.count - k - 1})
        self.ack(dev, circuit, False, origin)
        self.set_attributes(dev, circuit, {"busy": False})

    async def _run_timed(self, key: Key, t: Timed, origin: str) -> None:
        dev, circuit = key
        t0 = self.clock()
        await self.write(dev, circuit, 1 if t.on else 0)
        self.ack(dev, circuit, t.on, origin)
        self.set_attributes(dev, circuit, {"busy": True, "ends_at": self._ends_at(t.duration_s)})
        await self._until(t0 + t.duration_s)
        await self.write(dev, circuit, 0 if t.on else 1)
        self.ack(dev, circuit, not t.on, origin)
        self.set_attributes(dev, circuit, {"busy": False})
