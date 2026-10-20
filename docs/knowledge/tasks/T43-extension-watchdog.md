---
type: Task
id: T43
title: "Extension watchdog: bring a silent xS extension back automatically"
description: "When evok reports a serial extension (xS51 ...) that has stopped answering, the bridge restarts evok (measured to fix it), with a pause ladder, optional host reboot, daily caps and reporting to Home Assistant."
phase: 1
task_status: in_progress
depends_on: [T10]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, robustness, extension, evok]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-20T21:00:00Z }
---

# Objective
The RS485 link to an extension stops answering "every now and then" (S103 + xS51, 2026-10-19 12:33 UTC → 2026-10-20 20:28 UTC ≈ 31 h: buttons and lamps on the extension dead, HA showed optimistic state only). Find the least invasive recovery, then automate it. Applies to **any** Neuron with a serial extension; a Neuron without one is never touched.

# Measurements on the real fault (S103, 2026-10-20)
- Bridge healthy, evok healthy, evok *was* polling (UART counters: ~8 bytes TX every ~25 s = evok's exponential back-off, capped at 120 s) but the extension never answered (RX = echo + noise). So not a hung bridge and not a hung evok process.
- **Rung 1 – re-apply the UART settings** (`tcsetattr` identical values + flush, no outage): **no effect**.
- **Rung 2 – `systemctl restart evok`**: evok REST back after 3 s, **extension answering again after ~8 s**, bridge reconnected by itself, inputs refreshed. Impact: onboard I/O unreachable for ~3–8 s (outputs keep their state).
- Rung 3 – host reboot: **not tried** (not needed; also it would have ended the working session on the box). Kept as an opt-in last step.
- A physical power-cycle of the xS51 was not needed; if evok restarts stop helping the cause is probably power/cable ("hardware suspected").
- Tool gotcha: evok lists two `modbus_slave` entries (Neuron's own TCP board `circuit 1` and the RTU extension); select by `modbus_type == RTU`.

# Design (`unipi_core/extwatch.py`, wired in `_monitor_extensions`)
- **Detection / activation**: acts only if evok reports a `modbus_slave` with `modbus_type RTU` (or a `/dev/tty*` spec). Nothing → state `no_extension`, never acts. Off with `extensions.watchdog: false`; always off in shadow mode.
- **Trigger**: `last_comm` > `watchdog_down_after_s` (180 s, minimum 30). Not during the first 180 s after the bridge starts, nor for 120 s after an action (evok's `last_comm` is meaningless right after a restart).
- **Ladder**: restart evok → (≥10 min later, still down) restart evok again → (≥30 min later) reboot host *only if* `watchdog_reboot: true`, host up ≥15 min, ≤1 reboot per 24 h → then "hardware suspected": one evok restart every 6 h. Global cap `watchdog_max_actions_per_day` (8). History persisted in `extension_watchdog_state.json` next to the rules (survives restarts/reboots, so no loops).
- **Commands**: `sudo -n systemctl restart evok` / `... reboot` (configurable lists). If a command cannot run (no sudo right) it is reported and not retried; example sudoers drop-in: `tools/sudoers-hass-unipi.example` (S103 user already has passwordless sudo).
- **Reporting**: HA sensor "Extension watchdog" on the bridge device (state `ok|no_extension|starting|waiting|down|hardware_suspected|limit_reached|disabled`, attributes: attempt, last action/time, 24 h counters, last recovery, per-extension `last_comm`), retained; WARNING lines in the bridge log. The existing "Extension xS51 Status" problem sensor is unchanged.

# Evidence
- Tests: `tests/test_extwatch.py` (21): no-extension Neuron never touched, grace/settle, ladder timing, reboot opt-in/uptime/cap across restarts, daily cap, recovery reset, failed-command no-retry, multi-extension, bridge execution + publishing, shadow, real `_monitor_extensions` loop. 16 mutation checks, 14 caught at once, 2 survivors (shadow exclusion, monitor not calling the watchdog) → two tests added → caught.
- Not done: live trigger of the ladder on the S103 (needs a real fault; the extension is currently healthy). The first real outage will show up in the "Extension watchdog" sensor and the bridge log.

# Open questions
- Should the reboot step ever be enabled on the S103? Recommendation: leave off; the evok restart fixed the observed fault.
- The xS51 dies "regularly": check its 24 V supply and RS485 termination/cable if the sensor shows `hardware_suspected` or frequent actions.
