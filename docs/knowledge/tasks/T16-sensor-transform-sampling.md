---
type: Task
id: T16
title: "Add sensor transform, sampling and validation per circuit"
description: "ai, temp and 1-wire sub-keys can be scaled (e.g. volts→lux), averaged over a publish interval and range-checked, configured per circuit; defaults keep today's deadband behaviour."
phase: 1
task_status: dropped
depends_on: [T11]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, sensors, lux, temperature]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# DROPPED 2026-10-07
Scaling/averaging/range checks are one-liners in Home Assistant templates and `statistics`; nothing local depends on them. Decision by Matthijs (logic that does not need to survive an HA outage lives in HA, ADR-005). Kept for the record; reopen only if a local rule needs it.

# Objective
Closes G9, G10, G11 (lux ×200, vis ×8000, averaging, −55…125 °C / 0…100 % validation).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §4 (`transform`, `sampling`, `valid_range`)
- [/context/interface-legacy.md](/context/interface-legacy.md) §2

# Preconditions
- T11 merged.

# Files in scope
`unipi_core/signals.py` (`Transform`, `Sampler`, `Validator`), `hass-unipi.py` (sensor
publish path incl. `_publish_1wdevice_values`, discovery unit/device_class/state_class),
`tests/test_signals.py`.

# Backup
`tools/backup.sh pre-T16`.

# Steps
1. Pipeline per value: validate (`valid_range`, `reject_values`) → transform
   (`value*scale+offset`, `round`) → sampler (`mode: mean|last|max`, `publish_interval_s`)
   → publish. Without config: exactly today's behaviour (deadband 0.05 on raw value).
2. Discovery: `unit_of_measurement`, `device_class`, `state_class: measurement` from config.
3. Rejected values: log at DEBUG with a per-circuit counter; WARNING at most once per 10 min.
4. Tests: lux example (0.5 V → 100 lx), mean over interval, out-of-range dropped, 1-wire
   `vis` sub-key transform, no-config path identical to characterization tests.

# Acceptance checks
- Tests green; on S103 configure one `ai` as lux and see values in HA at the chosen interval.

# Rollback
Remove the keys; `tools/rollback.sh <previous rc>`.

# Feed the elephant
G9–G11 handled; `/log.md`.

# Evidence

# Open questions
