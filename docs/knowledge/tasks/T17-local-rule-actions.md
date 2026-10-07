---
type: Task
id: T17
title: "Add pulse and toggle local-rule actions and a configurable dimmer level"
description: "Local rules can ring a bell (pulse via the sequencer), toggle a digital output, and dimmer rules use action_value as default on-level; the Blockly editor supports all three."
phase: 1
task_status: todo
depends_on: [T12]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, local-rules, web-ui]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes G12, G13, G14 and K4/K5, so all old `handle_local` behaviour can be expressed as core
rules (ADR-002 §5) and keeps working without HA/MQTT.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §5
- [/context/interface-legacy.md](/context/interface-legacy.md) §3
- [/analysis/known-issues.md](/analysis/known-issues.md) K4, K5

# Preconditions
- T12 merged (sequencer). Current `local_rules.json` on S103 is `[]` (verify; if not empty,
  back it up and make sure existing rules load unchanged).

# Files in scope
`hass-unipi.py` (`LocalLogicRule`, `LocalLogicEngine.evaluate`, `execute_local_action`,
`handle_dimmer_action`), `web/index.html` (Blockly blocks + save/load mapping),
`tests/test_local_rules.py`.

# Backup
`tools/backup.sh pre-T17` (includes `local_rules.json`).

# Steps
1. Model: `action_type` ∈ `set|dimmer|toggle|pulse`; new optional fields
   `action_pulse: {count,on_ms,off_ms}`, `action_preset: str`. Keep `action_transition`
   (ms, K4) and add validation message stating the unit; Blockly label shows "ms".
2. `toggle`: read `device_states` for the target; ON→OFF, else ON; via `CommandService.set`.
3. `pulse`: via `CommandService.sequence(... origin="rule")`; rejection is logged, never raised.
4. Dimmer: default level = `action_value` (V) if set, else 10.0; persist `previous_level`
   per rule in `local_rules_state.json` (gitignored) so a restart doesn't jump to 10 V.
5. Triggers fire on the **active edge** only for pulse/toggle by default
   (`trigger_value: 1` after logical inversion), matching legacy behaviour for momentary buttons.
6. Blockly: blocks "toggle output", "pulse output (count, on ms, off ms) / preset",
   dimmer block gets "default level (V)". Round-trip test: create in UI → `GET /api/rules`
   JSON → reload → identical blocks.

# Acceptance checks
- Unit tests for all four action types (fake clock).
- On S103: a rule "di X active → pulse led/1_01 count 2" blinks the LED twice.
- Works without MQTT: proven by a unit test with `FakeMqtt.is_connected() == False`.
  **Never stop the real broker** to test this — it is shared by the whole house.
- Existing (empty or not) rules file loads unchanged.

# Rollback
Restore `local_rules.json` from backup; `tools/rollback.sh <previous rc>`.

# Feed the elephant
interface-core §5 implemented; G12–G14, K4, K5 handled; `/log.md`.

# Evidence

# Open questions
