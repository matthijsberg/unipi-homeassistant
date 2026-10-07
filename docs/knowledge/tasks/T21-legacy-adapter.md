---
type: Task
id: T21
title: "Implement legacy_adapter.py behind the legacy.enabled switch"
description: "With legacy.enabled true the bridge serves every legacy topic in the verified contract via core commands/events; with false the module is never imported."
phase: 2
task_status: todo
depends_on: [T12, T14, T15, T16, T17, T20]
risk: high
human_gate: false
target_hosts: [dev-only, s103]
tags: [legacy, adapter]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Implements ADR-002: old HA YAML keeps working on the upgraded L513, while all behaviour runs
in the core. The adapter only translates.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-002-legacy-adapter.md](/decisions/ADR-002-legacy-adapter.md)
- [/context/interface-legacy.md](/context/interface-legacy.md) (must be human-verified — T20)
- [/context/interface-core.md](/context/interface-core.md) §2, §3, §7

# Preconditions
- `/context/interface-legacy.md` has `verified:` from `human:matthijs`. Otherwise STOP.
- T12, T14–T17 merged.

# Files in scope
`legacy_adapter.py` (new, top level), `hass-unipi.py` (≤ 20 lines: config model `legacy`,
conditional import + `adapter.start(bridge)` / `stop()`), `legacy_map.example.json`,
`tests/test_legacy_adapter.py`.

# Backup
`tools/backup.sh pre-T21`.

# Steps
1. Config: `legacy: {enabled: bool=false, map_file: str="legacy_map.json",
   command_root: "unipi1", qos_ack: 0}`. In `UnipiBridge.__init__`:
   `if config.legacy.enabled: from legacy_adapter import LegacyAdapter` — import nowhere else.
2. `legacy_map.json` schema (pydantic in the adapter):
   ```jsonc
   {"commands": [ {"topic": "unipi1/bgg/hal/bel", "target": "ro/<c>",
                   "kind": "relay|ao|duration|pulse", "pulse_defaults": {"on_ms":100,"off_ms":250}} ],
    "states":   [ {"topic": "unipi/bgg/hal/motion", "source": "di/<c>",
                   "format": "onoff|lux|temperature|humidity|counter",
                   "available_topic": true} ]}
   ```
   The legacy JSON's own `dev`/`circuit` fields are **ignored** in favour of the map (old
   circuit names are invalid after evok 3); log a WARNING if they disagree with the map's
   recorded legacy names.
3. Inbound: subscribe `<topic>/set` for each command; parse per interface-legacy §1:
   `repeat` → `CommandService.sequence(Pulse(count=int(repeat), **pulse_defaults[, timing keys
   found in T20]))`; `duration` → `Timed(state, duration)`; `brightness` (0–255) +
   `transition` → `transition(round(b*1000/255), s)`; `state on/off` → `set`; AO `on`
   without brightness → last non-zero level (adapter memory, default 100 %).
   Retained `/set` ignored (same rule as core).
4. Ack: on `output_changed`/sequence events publish to `<topic>` (retain True, qos per config)
   the original command payload with key order preserved and `state`/`brightness` updated
   (brightness back to 0–255); for pulse: first ack after start echoing the command, final ack
   with `repeat` removed and `state:"off"` — exactly as interface-legacy §1.
5. Outbound states from `input_changed` (held state when the circuit has `off_delay_s`):
   `onoff` → `ON`/`OFF` retained; `lux`/`temperature`/`humidity` → JSON non-retained from the
   already transformed/sampled core value; `counter` → `{"counter_delta": d, "counter": c}`
   with `d` = difference to the last value this adapter published (first publish d = 0).
6. Availability: on `availability(True)` publish `online` retained to every
   `<topic>/available` with `available_topic: true`; `offline` on `False` and on stop.
7. Ignore `source="republish"` events except to re-publish retained legacy states.
8. Tests: replay `tests/fixtures/legacy_traffic.jsonl` command lines through the adapter
   with fake core → assert core calls and ack payloads byte-equal to captured acks;
   state publishing for each format; `enabled:false` ⇒ `legacy_adapter` not in `sys.modules`.

# Acceptance checks
- Tests green, including byte-equal ack comparison.
- S103 deploy with `legacy.enabled: false` → health OK and no behaviour change (module not loaded).
- S103 **shadow** instance (T18) with `legacy.enabled: true`, a map pointing legacy
  topics under a **test root** (e.g. `legacytest/…`) to S103 LEDs/inputs: publish legacy
  bel payload → LED blinks are *logged* (shadow) with the right timing; states appear on
  `legacytest/…`.

# Rollback
Set `legacy.enabled: false` (single switch) or `tools/rollback.sh <previous tag>`.

# Feed the elephant
interface-core §7 implemented; `/log.md`. Tag `v2.3.0` after T22 + T23.

# Evidence

# Open questions
