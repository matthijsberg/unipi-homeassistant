---
type: Task
id: T20
title: "Capture the complete legacy Home Assistant contract"
description: "The HA YAML, automations and live command payloads that use the old L513 topics are captured, and interface-legacy.md is complete and human-verified."
phase: 2
task_status: todo
depends_on: [T01]
risk: low
human_gate: true
target_hosts: [dev-only]
tags: [legacy, home-assistant, inventory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
The adapter (T21) and the HA migration (T40) are only as good as this contract. Today it
is reconstructed from code and retained messages; HA's side is unknown.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-legacy.md](/context/interface-legacy.md)

# Preconditions
- T01 done (live L513 config known).

# Files in scope
`/context/interface-legacy.md`, new `/context/ha-legacy-entities.md`,
`tools/mqtt_capture.py` (read-only subscriber), `tests/fixtures/legacy_traffic.jsonl`.

# Backup
**🔒 HUMAN**: HA full backup before exporting anything (habit; nothing is changed).

# Steps
1. **🔒 HUMAN**: export from HA every YAML block / UI-made MQTT entity using `unipi1/` or
   `unipi/<area>/` topics, plus every automation/script/dashboard referencing those
   entities (Settings → Entities, filter by integration MQTT; or the `configuration.yaml`
   `mqtt:` section). Paste (secrets removed) into `/context/ha-legacy-entities.md`.
2. `tools/mqtt_capture.py`: read-only subscriber (credentials from `config.json`, client id
   `capture-<host>`) to `unipi1/#` and the `unipi/<area>/#` roots, writing JSONL
   `{ts, topic, retain, payload}`. Run 24 h **and** during a manual test session:
   **🔒 HUMAN** press each doorbell from HA and from the buttons, toggle each light from HA
   and wall switch, open/close the roof window from HA, change ventilation, toggle each
   smart-grid relay only if safe (ask heat-pump constraints first!).
3. From the capture, answer: exact bel payload (is there a timing key?), ventilation topic
   layout (3_02 vs 3_03), every `/set` topic and its payload shape, which state topics HA
   reads, whether HA uses `/available`.
4. Update `/context/interface-legacy.md`: remove "inferred", add a table "used by HA:
   yes/no" per topic; list topics with **no** consumer (candidates for not porting).
5. Save a trimmed capture as `tests/fixtures/legacy_traffic.jsonl` (no secrets) for T23.

# Acceptance checks
- The 11 retained topics not in the old config (see K12) never change during the 24 h capture (⇒ stale, to be cleared in T41), or are explained;
- Every legacy topic in the capture appears in the contract; every HA entity in the export
  maps to a topic in the contract.
- **🔒 HUMAN** reviews and adds `verified: { by: "human:matthijs", at: … }` to
  `/context/interface-legacy.md` and `/context/ha-legacy-entities.md`.

# Rollback
Nothing changed.

# Feed the elephant
Both concepts + `/log.md`; open questions in interface-legacy closed.

# Evidence

# Open questions
