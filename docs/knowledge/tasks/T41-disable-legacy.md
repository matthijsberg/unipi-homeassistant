---
type: Task
id: T41
title: "Switch the legacy adapter off and clean up legacy MQTT leftovers"
description: "legacy.enabled is false on the L513 for 14 days without regressions; legacy YAML removed from HA; retained legacy topics cleared from the broker."
phase: 4
task_status: todo
depends_on: [T40]
risk: low
human_gate: true
target_hosts: [l513]
tags: [legacy, cleanup]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Prove nothing depends on the old topics before deleting code (ADR-002).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-legacy.md](/context/interface-legacy.md)

# Preconditions
- T40 done for all areas.

# Files in scope
L513 `config.json` (`legacy.enabled`), HA YAML (human), `cleanup_ghosts.py` or a new
`tools/clear_retained.py` (dry-run default).

# Backup
`tools/backup.sh pre-T41` (L513) + HA full backup.

# Steps
1. Set `legacy.enabled: false`, restart via `tools/deploy.sh <current tag>` (config-only change).
2. 14 days: daily check that no HA automation errors mention unipi entities; doorbell/lights OK.
3. **🔒 HUMAN** delete legacy YAML entities in HA.
4. `tools/clear_retained.py --dry-run unipi1/# unipi/bgg/# unipi/bbg/# unipi/buiten/# unipi/eerste/# unipi/huis/#`
   → list; **🔒 HUMAN** approves → run without dry-run (publishes empty retained payloads).
   Must never touch `unipi/<dn>/…` (new) topics — explicit exclusion + test.

# Acceptance checks
- 14 days clean; broker has no retained legacy topics (re-run capture: empty).

# Rollback
`legacy.enabled: true` + restart (adapter republishes legacy states); restore HA backup.

# Feed the elephant
interface-legacy → `status: deprecated`; `/log.md`.

# Evidence

# Open questions
