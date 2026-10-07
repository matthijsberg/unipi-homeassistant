---
type: Task
id: T19
title: "(Optional) Allow an analog output to be discovered as fan or number instead of light"
description: "circuits.<ao>.ha_component = fan | number publishes the matching HA entity with percentage control, so ventilation no longer needs an HA template."
phase: 1
task_status: todo
depends_on: [T11]
risk: low
human_gate: false
target_hosts: [dev-only, s103]
tags: [optional, discovery, ventilation]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
G19, nice-to-have. Skip if T20 shows the HA ventilation template is fine. Decide with Matthijs.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §4

# Preconditions
- **🔒 HUMAN** confirmed this task is wanted.

# Files in scope
`hass-unipi.py` (discovery + command parsing for `percentage` topics), tests.

# Backup
`tools/backup.sh pre-T19`.

# Steps
1. `fan`: discovery `fan` with `percentage_command_topic`/`percentage_state_topic`
   (0–100 → 0–10 V) and on/off via existing AO paths (ON = last non-zero %).
2. `number`: discovery `number` 0–100 `%`, `mode: slider`.
3. Default stays `light` (no change for S103).

# Acceptance checks
- Tests for both components; one S103 AO switched to `number` temporarily and back.

# Rollback
Remove the key; old `light` entity returns on republish.

# Feed the elephant
G19 handled; `/log.md`.

# Evidence

# Open questions
