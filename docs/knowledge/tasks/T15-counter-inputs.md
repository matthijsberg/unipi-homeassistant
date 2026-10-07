---
type: Task
id: T15
title: "Publish digital-input pulse counters as total_increasing sensors"
description: "Inputs with counter true get a discovered counter sensor fed from the evok counter field, rate-limited, surviving evok counter resets."
phase: 1
task_status: done
depends_on: [T11]
risk: low
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, inputs, counter, water]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes G8 (water meter). HA computes consumption via `utility_meter` (ADR-005).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §3
- [/context/interface-legacy.md](/context/interface-legacy.md) §2 (counter row)

# Preconditions
- T11 merged. evok 3 `di` objects contain `counter` (verified in S103 fixture: yes).
- Check whether evok sends counter changes over WS or only in REST: record in Evidence
  (if only REST, poll the counter circuits every `counter_interval_s` via REST).

# Files in scope
`unipi_core/signals.py` (`CounterPublisher`), `hass-unipi.py` (discovery + WS/REST hook),
`tests/test_counter.py`.

# Backup
`tools/backup.sh pre-T15`.

# Steps
1. Discovery sensor: topic `…/<dev>/<circuit>/counter`, `state_class: total_increasing`,
   `unit_of_measurement` from config (default none), `value_template {{ value_json.value }}`.
2. Publish `{"value": counter}` retained, at most every `counter_interval_s`, only when changed.
3. Emit `input_changed(dev, circuit, subkey="counter", value=…)` for the legacy adapter.
4. Tests: increments, rate limit, reset to lower value is published as-is (HA handles it).

# Acceptance checks
- Tests green; on S103 counter sensor exists for a configured di (any di; counting pulses
  of a wall button is enough).

# Rollback
Remove `counter` from config; `tools/rollback.sh <previous rc>`.

# Feed the elephant
G8 handled; `/log.md`.

# Evidence
- 2026-10-07: `pytest -q` → **234 passed** together with T18 (+ 16 counter tests). Looked at the live box first: evok **does push `counter`** inside its `di` WebSocket messages, and several inputs have running counters (e.g. one at 24235), so the data is real. A REST poll every `counter_interval_s` stays as a safety net.
- Implemented: circuit options `counter`, `counter_interval_s` (1–3600, default 10), `unit` (only valid with `counter`; digital inputs only); discovered `sensor` `…/di_<c>_counter` (`state_class: total_increasing`, `{"value": n}`), retained; publish only on change and at most every interval (the latest value is held and flushed); initial value at discovery; republish forces; reset to a lower value is published as it is; `input_changed(subkey="counter")` for the legacy adapter.
- Removed from the config model on the way: the T14/T16 option names now fail with "not a bridge feature - do it in Home Assistant" (ADR-005).
- Mutation checks: 10 breaks; 2 needed a second look (a malformed mutation → redone; a real survivor — unchanged values re-published after the interval — got its own test); all caught.
- Not done on hardware: counting needs a real pulse source; the live config is untouched (no `counter` configured on the S103). Enable e.g. `circuits."di/<c>" = {"counter": true, "unit": "L"}` to try it.

# Open questions
