---
type: Task
id: T15
title: "Publish digital-input pulse counters as total_increasing sensors"
description: "Inputs with counter true get a discovered counter sensor fed from the evok counter field, rate-limited, surviving evok counter resets."
phase: 1
task_status: todo
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

# Open questions
