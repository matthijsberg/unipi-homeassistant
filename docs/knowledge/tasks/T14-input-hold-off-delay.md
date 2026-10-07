---
type: Task
id: T14
title: "Add retriggerable off-delay (hold) for pulse-type inputs such as PIRs"
description: "Inputs with off_delay_s publish ON on the first active edge and OFF only after N seconds without a new active edge; local rules can choose raw or held state."
phase: 1
task_status: todo
depends_on: [T11]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, inputs, motion]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes G6 (old `device_delay`). Must live in the bridge because local rules and legacy
topics need the held state and the bridge publishes raw OFF edges that would override an
HA-side `off_delay` (ADR-005).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §3
- [/context/interface-legacy.md](/context/interface-legacy.md) §2 (PIR row)

# Preconditions
- T11 merged.

# Files in scope
`unipi_core/signals.py` (`HoldFilter`), `hass-unipi.py` (input path hook),
`hass-unipi.py` `LocalLogicRule` (`trigger_source: raw|held`, default raw),
`tests/test_hold.py`.

# Backup
`tools/backup.sh pre-T14`.

# Steps
1. `HoldFilter(off_delay_s)`: `on_edge(active, now)` → returns state transitions; timer via
   `loop.call_later`, re-armed on every active edge; inactive edges do nothing while holding.
   Uses the **logical** value (after T11 inversion).
2. Publish held state on the normal `/state` topic; emit `input_changed` with
   `held=True/False` so rules/legacy can pick.
3. Republish must publish the *held* state, not the raw REST value.
4. Discovery: `device_class` from config (e.g. `motion`); no HA `off_delay` key.
5. Tests with fake clock: single pulse → ON then OFF after N s; pulses every 10 s with
   N=20 → stays ON; restart while held → after start the REST value decides (document).

# Acceptance checks
- Tests green; on S103 configure one spare di (or skip hardware test if none is spare —
  note it) and verify in HA.

# Rollback
Remove `off_delay_s` from config, or `tools/rollback.sh <previous rc>`.

# Feed the elephant
interface-core §3 implemented; gap G6 handled; `/log.md`.

# Evidence

# Open questions
