---
type: Task
id: T18
title: "Add shadow mode for a read-only second instance"
description: "A second bridge instance with mode shadow can run next to the live one on the same Unipi without writing to evok, firing rules, or touching the live MQTT topics."
phase: 1
task_status: done
depends_on: [T10]
risk: low
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, testing, safety]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
R3: test new versions against real evok traffic on a live box with zero side effects.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §6

# Preconditions
- T10 merged.

# Files in scope
`hass-unipi.py` (AppConfig `mode`, `shadow_discovery`; guards), `unipi_core/commands.py`,
`unipi_core/sequencer.py` (if merged), `tools/shadow.service.example`, `tests/test_shadow.py`.

# Backup
none for live (separate instance); `tools/backup.sh pre-T18` anyway before first shadow run.

# Steps
1. `mode: shadow` ⇒ every WS send path (CommandService, sequencer, worker thread, fail-safe)
   logs `SHADOW: would send …` and returns; local rules evaluated but actions only logged;
   MQTT root forced to `config.mqtt.topic` which **must** differ from the live root (refuse to
   start otherwise); discovery disabled unless `shadow_discovery: true` (then use
   `<dp>` prefix + `unique_id` suffix `_shadow`); LWT/status under the shadow root; web
   server port must differ or be disabled; MQTT client id distinct.
2. Separate config file (`config.shadow.json`, gitignored) and a systemd *example* unit
   `hass-unipi-shadow.service` (not enabled automatically).
3. Test: in shadow mode no WS `send` is ever called, for every command path.

# Acceptance checks
- Tests green.
- On S103 **🔒 HUMAN approves** running the shadow instance for 1 h next to live: live
  service unaffected (health OK), shadow publishes under its root, zero WS writes (grep log).

# Rollback
Stop and disable the shadow unit.

# Feed the elephant
interface-core §6 implemented; `/runbooks/deploy.md` "shadow step" uses it; `/log.md`.

# Evidence
- 2026-10-07: **234 passed**; shadow has 11 tests. Core proof: every command path (MQTT ON/OFF, analog fade, pulse, timed, preset, plain OFF, four rule actions, `send_ws`, fail-safe, `set_digital`, `transition`, and the two raw send functions) is driven against a shadow instance → `websocket.sent == []`, nothing queued for evok, and `SHADOW: would …` lines logged.
- **Design change vs the card (reason: found while reading the code)**: instead of *requiring* a different MQTT root, the shadow's device name gets a `_shadow` suffix. The discovery topic and `unique_id` contain the device name but **not** the MQTT root, so a shadow with the same device name would have silently overwritten the live entities' retained discovery. With the suffix every topic, unique_id, identifier and discovery node differs (tested against a live bridge on the same snapshot: 0 shared discovery topics, unique_ids or device identifiers). The shadow also never reads/writes the live `.device_name` cache.
- No web UI in shadow; discovery configs are dropped by the MQTT worker unless `shadow_discovery: true`; names tagged "(shadow)".
- Tools: `tools/hass-unipi-shadow.service.example`, `tools/shadow_compare.py` (live vs shadow state diff), `tools/clear_shadow_topics.py` (dry-run default; its matcher provably cannot hit live topics), runbook `/runbooks/shadow-instance.md`, `config.shadow.example.json`.
- Mutation checks: 9 breaks (each guard removed) → all caught (one result line looked odd and was re-run to read the real outcome).
- **Not done (for the testing session)**: the 1 h real shadow run next to the live bridge (card acceptance, human-approved).

# Open questions
