"""Extension watchdog: bring a silent Unipi extension (xS51 ...) back without a person walking to the box.

Why: the RS485 link to an extension sometimes stops answering (seen on the S103/xS51: 1.3 days of dead buttons and
lamps until someone noticed). Measured on the real fault: re-initialising the UART does nothing, **restarting the evok
service brings the extension back within seconds**. So the ladder is: restart evok (cheap), restart evok again
after a pause, and - only if the operator enabled it - reboot the host. Everything is rate limited and the history is
kept on disk, so a broken cable can never turn into a restart/reboot loop. Beyond the ladder the watchdog stops
acting and only reports ("hardware suspected"), because software can no longer fix it.

It only ever acts when evok reports a *serial* (RTU) Modbus slave, i.e. a real extension. A Neuron without
extensions is never touched. This module is pure logic (clock and storage injectable); the bridge executes the
returned action and publishes `status()`.
"""
from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable

RESTART_EVOK = "restart_evok"
REBOOT = "reboot"

# States (also what Home Assistant shows)
OK = "ok"
NO_EXTENSION = "no_extension"
DISABLED = "disabled"
STARTING = "starting"
WAITING = "waiting"                  # an action was just taken, giving the extension time to answer
DOWN = "down"                        # silent, the next step of the ladder is not due yet
HARDWARE_SUSPECTED = "hardware_suspected"
LIMIT_REACHED = "limit_reached"


@dataclass
class WatchdogSettings:
    enabled: bool = True
    down_after_s: float = 180.0          # silent this long before anything is done (short glitches heal by themselves)
    startup_grace_s: float = 180.0       # after the bridge starts, evok's last_comm is meaningless for a while
    settle_s: float = 120.0              # after an action: no judgement until the extension had time to answer
    # Ladder: what to do on attempt 0, 1, 2 ... and how long to wait after the previous action before the next one.
    ladder: tuple = (RESTART_EVOK, RESTART_EVOK, REBOOT)
    wait_s: tuple = (0.0, 600.0, 1800.0)
    long_retry_s: float = 21600.0        # after the ladder: still try an evok restart this often (6 h)
    max_actions_per_day: int = 8
    reboot_enabled: bool = False         # a host reboot is the highest-impact step: opt in
    max_reboots_per_day: int = 1
    min_uptime_for_reboot_s: float = 900.0   # never reboot a host that has only just booted


def is_extension(slave: dict[str, Any]) -> bool:
    """A real extension = a Modbus slave on a serial line. The Neuron's own board is a TCP slave."""
    return str(slave.get("modbus_type", "")).upper() == "RTU" or str(slave.get("modbus_spec", "")).startswith("/dev/tty")


class ExtensionWatchdog:
    def __init__(self, settings: WatchdogSettings, state_file: str | None = None,
                 clock: Callable[[], float] = time.time, uptime: Callable[[], float] | None = None,
                 logger: logging.Logger | None = None):
        self.s = settings
        self.clock = clock
        self.uptime = uptime or _host_uptime
        self.logger = logger or logging.getLogger("ExtensionWatchdog")
        self.state_file = state_file
        self.started_at = clock()
        self.history: list[dict[str, Any]] = self._load()      # [{"ts":..., "kind":...}], newest last, persisted
        self.attempt = 0                                         # steps taken in the current outage
        self.down_since: float | None = None
        self.last_action_at: float | None = None
        self.hold_until = 0.0
        self.blocked: set[str] = set()                           # action kinds whose command failed to run
        self.state = STARTING if settings.enabled else DISABLED
        self.last_recovery_ts: float | None = None
        self.extensions: dict[str, float] = {}

    # -- persistence ------------------------------------------------------------------------------------------------
    def _load(self) -> list[dict[str, Any]]:
        if not self.state_file:
            return []
        try:
            with open(self.state_file) as f:
                data = json.load(f)
            now = self.clock()
            return [a for a in data.get("actions", []) if now - float(a["ts"]) < 7 * 86400]
        except (OSError, ValueError, KeyError, TypeError):
            return []

    def _save(self) -> None:
        if not self.state_file:
            return
        tmp = self.state_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({"actions": self.history}, f)
            os.replace(tmp, self.state_file)
        except OSError as e:
            self.logger.warning(f"Extension watchdog: could not save its history: {e}")

    def _recent(self, kind: str | None = None, window: float = 86400.0) -> int:
        now = self.clock()
        return sum(1 for a in self.history if now - a["ts"] < window and (kind is None or a["kind"] == kind))

    # -- the decision -----------------------------------------------------------------------------------------------
    def observe(self, slaves: list[dict[str, Any]]) -> str | None:
        """Feed the current list of evok `modbus_slave` entries. Returns an action to execute, or None."""
        now = self.clock()
        if not self.s.enabled:
            self.state = DISABLED
            return None
        exts = [s for s in slaves if is_extension(s)]
        self.extensions = {str(s.get("circuit")): float(s.get("last_comm", 0) or 0) for s in exts}
        if not exts:
            self._reset_outage()
            self.state = NO_EXTENSION
            return None
        if now - self.started_at < self.s.startup_grace_s:
            self.state = STARTING
            return None
        if now < self.hold_until:
            self.state = WAITING
            return None

        down = [c for c, lc in self.extensions.items() if lc > self.s.down_after_s]
        if not down:
            if self.attempt:
                self.last_recovery_ts = now
                self.logger.warning(f"Extension watchdog: extension is answering again after {self.attempt} recovery step(s).")
            self._reset_outage()
            self.state = OK
            return None

        if self.down_since is None:
            self.down_since = now
            self.logger.warning(f"Extension watchdog: {', '.join(down)} silent for more than {self.s.down_after_s:g} s.")

        kind = self._next_step(now)
        if kind is None:
            return None
        self.history.append({"ts": now, "kind": kind})
        self.history = [a for a in self.history if now - a["ts"] < 7 * 86400]
        self._save()
        self.attempt += 1
        self.last_action_at = now
        self.hold_until = now + self.s.settle_s
        self.state = WAITING
        self.logger.warning(f"Extension watchdog: step {self.attempt} -> {kind}")
        return kind

    def _next_step(self, now: float) -> str | None:
        if self._recent() >= self.s.max_actions_per_day:
            self.state = LIMIT_REACHED
            return None
        ladder, waits = self.s.ladder, self.s.wait_s
        if self.attempt < len(ladder):
            kind, wait = ladder[self.attempt], waits[min(self.attempt, len(waits) - 1)]
        else:
            kind, wait = RESTART_EVOK, self.s.long_retry_s
            self.state = HARDWARE_SUSPECTED
        if self.last_action_at is not None and now - self.last_action_at < wait:
            if self.state != HARDWARE_SUSPECTED:
                self.state = DOWN
            return None
        if kind == REBOOT and not self._reboot_allowed():
            # not allowed (off, cap, fresh boot): the ladder is exhausted as far as we are allowed to go
            self.attempt = max(self.attempt, len(ladder))
            self.state = HARDWARE_SUSPECTED
            return None
        if kind in self.blocked:
            self.attempt += 1
            self.state = HARDWARE_SUSPECTED
            return None
        return kind

    def _reboot_allowed(self) -> bool:
        return (self.s.reboot_enabled and REBOOT not in self.blocked
                and self._recent(REBOOT) < self.s.max_reboots_per_day
                and self.uptime() >= self.s.min_uptime_for_reboot_s)

    def _reset_outage(self) -> None:
        self.attempt, self.down_since, self.last_action_at = 0, None, None
        self.blocked.clear()

    def action_failed(self, kind: str, why: str) -> None:
        """The bridge could not even run the command (e.g. no permission): do not keep trying it."""
        self.blocked.add(kind)
        self.logger.error(f"Extension watchdog: action '{kind}' could not be run: {why}")
        self.hold_until = 0.0

    # -- what is shown in Home Assistant ------------------------------------------------------------------------------
    def status(self) -> dict[str, Any]:
        last = self.history[-1] if self.history else None
        return {
            "state": self.state,
            "attempt": self.attempt,
            "down_since": self.down_since,
            "last_action": last["kind"] if last else None,
            "last_action_ts": last["ts"] if last else None,
            "actions_24h": self._recent(),
            "reboots_24h": self._recent(REBOOT),
            "last_recovery_ts": self.last_recovery_ts,
            "blocked": sorted(self.blocked),
            "extensions": self.extensions,
        }


def _host_uptime() -> float:
    try:
        with open("/proc/uptime") as f:
            return float(f.read().split()[0])
    except (OSError, ValueError, IndexError):
        return 0.0
