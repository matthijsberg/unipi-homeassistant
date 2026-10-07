---
type: Task
id: T40
title: "Move Home Assistant from legacy YAML entities to discovered entities, one area at a time"
description: "Every automation, script and dashboard uses the discovered L513 entities; legacy YAML entities are unused (still present)."
phase: 4
task_status: todo
depends_on: [T33]
risk: medium
human_gate: true
target_hosts: [dev-only]
tags: [home-assistant, migration]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Make the legacy adapter unnecessary. Done per area (hal, bijkeuken, woonkamer, serre,
buiten, eerste, warmtepomp) so each step is small and reversible.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/ha-legacy-entities.md](/context/ha-legacy-entities.md)
- [/context/l513-circuit-map.md](/context/l513-circuit-map.md)
- [/decisions/ADR-005-core-vs-home-assistant.md](/decisions/ADR-005-core-vs-home-assistant.md)

# Preconditions
- T33 accepted.

# Files in scope
HA config (by **🔒 HUMAN** or with HA access granted to the goldfish), new
`/context/ha-entity-migration.md` (old entity_id → new entity_id per area, status).

# Backup
**🔒 HUMAN** HA full backup before each area.

# Steps (per area)
1. Table old → new entity ids. Prefer renaming the *new* entity to the old `entity_id`
   after deleting/renaming the old one, so automations/dashboards/history need fewer edits
   (HA keeps history per entity_id). Decide per entity with Matthijs.
2. Doorbell: replace "repeat" automations with `mqtt.publish` pulse JSON or preset buttons
   (interface-core §2). Water meter: `utility_meter` on the new counter sensor.
3. Ventilation: keep template fan on the new AO light, or use T19.
4. Disable (not delete) the legacy YAML entities of that area; run 48 h.

# Acceptance checks
- Per area: automations trace OK in HA for 48 h; no reference to legacy entities
  (HA → Settings → Entities → "related" for each legacy entity is empty).

# Rollback
Re-enable the legacy entities of that area / restore HA backup.

# Feed the elephant
Migration table, `/log.md`.

# Evidence

# Open questions
