---
type: Task
id: T13
title: "Expose sequencer presets as Home Assistant button entities"
description: "Each preset in circuits.<id>.presets is discovered as an HA button that starts that sequence; buttons are included in republish and removed when the preset is removed."
phase: 1
task_status: todo
depends_on: [T12]
risk: low
human_gate: false
target_hosts: [dev-only, s103]
tags: [discovery, home-assistant, doorbell]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
One-tap "Bel voordeur (3×)" in HA without writing JSON (G1 HA side, ADR-003).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §2, §4

# Preconditions
- T12 merged; S103 LED presets configured from T12 step 7.

# Files in scope
`hass-unipi.py` (discovery + republish), `tests/test_discovery_buttons.py`, `README.md` (usage section).

# Backup
`tools/backup.sh pre-T13`.

# Steps
1. For every preset: discovery topic `<dp>/button/<dn>/<dev>_<circuit>_<preset>/config` with
   `name` = preset `label`, `unique_id` = `<dn>_<dev>_<circuit>_preset_<preset>`,
   `command_topic` = circuit `/set`, `payload_press` = `{"preset":"<preset>"}`, same
   device block + availability as other entities, `icon` `mdi:bell-ring` when the circuit
   name contains "bel" else `mdi:play`.
2. Removal: keep a small JSON file `.discovered_presets` (runtime dir, gitignored) listing
   published preset topics; on start publish an empty retained payload to any topic no
   longer configured (HA deletes the entity).
3. Include buttons in `republish_all`.
4. README: document the three JSON command forms + an HA automation example.

# Acceptance checks
- Tests: button discovery payload snapshot; removed preset → empty retained publish.
- On S103: the LED preset button appears in HA under the S103 device and blinks the LED.

# Rollback
`tools/rollback.sh <previous rc>`; delete stale button entities in HA if any remain.

# Feed the elephant
`/context/interface-core.md` §2 (buttons implemented), `/log.md`.

# Evidence

# Open questions
