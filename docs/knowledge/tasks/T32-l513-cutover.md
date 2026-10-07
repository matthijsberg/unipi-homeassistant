---
type: Task
id: T32
title: "Start the new bridge on the L513 with the legacy adapter and run the acceptance walk"
description: "hass-unipi runs on the L513 with converter-generated config and legacy.enabled true; every function in the legacy contract passes a physical acceptance walk; HA YAML works unchanged; new discovered entities exist in parallel."
phase: 3
task_status: todo
depends_on: [T31, T22]
risk: high
human_gate: true
target_hosts: [l513]
tags: [l513, cutover, legacy, maintenance-window]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Restore all house functions on the upgraded L513 within the window, through the new bridge.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/l513-circuit-map.md](/context/l513-circuit-map.md) (verified)
- [/context/interface-legacy.md](/context/interface-legacy.md)
- [/runbooks/l513-evok3-upgrade.md](/runbooks/l513-evok3-upgrade.md)

# Preconditions
- T31 done in this window. `v2.3.0` tag available.

# Files in scope
L513 runtime dir, its `config.json`, `local_rules.json`, `legacy_map.json`, systemd unit.

# Backup
`tools/backup.sh pre-T32` on L513 (copy to S103).

# Steps
1. Install per runbook; `tools/convert_legacy.py` with the real map → review `REPORT.md`
   (**🔒 HUMAN** OK) → merge outputs into L513 `config.json`, `local_rules.json`,
   `legacy_map.json`. `legacy.enabled: true`. Dedicated MQTT user if created (K8).
2. `tools/deploy.sh v2.3.0` on L513 → health OK.
3. Acceptance walk (**🔒 HUMAN** + goldfish; tick each in Evidence):
   - [ ] Front-door button → bell rings 3×; back-door button → 2× (local rule — must not
         depend on HA: verify with the related HA automations disabled; never stop the broker)
   - [ ] HA bel entity (legacy YAML) → rings with HA's repeat; HA shows ON then OFF
   - [ ] New preset buttons (discovered) ring correctly
   - [ ] Each wall switch toggles its light at the old level (5 V / 10 V); HA legacy light follows
   - [ ] HA legacy lights: on/off/brightness/transition 4 s for all 5 dimmers
   - [ ] Roof window: HA legacy `duration 35` opens and stops after 35 s; fail-safe test: stop service mid-run → relay OFF
   - [ ] Ventilation AO from HA
   - [ ] Smart-grid relays 3_01/3_02 from HA (human decides if safe to toggle)
   - [ ] Each PIR: ON on motion, OFF after its delay (legacy topic and new entity)
   - [ ] Each door/window contact & lock: correct polarity in HA (NC)
   - [ ] Leak sensors: short test (wet finger / test button) → ON
   - [ ] Water meter counter increases; legacy `counter_delta` sane
   - [ ] Lux sensors ≈ old values (±10 %), temperatures/humidity ≈ old values
   - [ ] `/available` topics `online`; bridge status/health entities OK
4. Any failed item: fix only config (map/converter output) within the window; a code bug ⇒
   rollback (card swap) and create a task.

# Acceptance checks
- All walk items ticked; health OK 1 h after start.

# Rollback
Card swap (old system resumes with old YAML; retained legacy states may be briefly stale).

# Feed the elephant
Evidence walk; `/log.md`; tag `v2.4.0` (release notes: L513 live) — tag points at the same
code as v2.3.0 + any config-tool fixes.

# Evidence

# Open questions
