---
type: Task
id: T22
title: "Convert the old unipi_mqtt_config.json into core config, rules and legacy map"
description: "tools/convert_legacy.py turns the old per-circuit config plus the evok-3 circuit map into circuits config, local_rules.json entries and legacy_map.json, with a human-readable diff report."
phase: 2
task_status: todo
depends_on: [T21]
risk: medium
human_gate: false
target_hosts: [dev-only]
tags: [legacy, migration, tooling]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Make the L513 configuration reproducible and reviewable instead of hand-typed during a
maintenance window. Implements the mapping column of the gap matrix.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/analysis/feature-gap-matrix.md](/analysis/feature-gap-matrix.md)
- [/context/interface-legacy.md](/context/interface-legacy.md)
- [/context/interface-core.md](/context/interface-core.md) §4, §5

# Preconditions
- T21 merged. Old config = the **live** L513 version captured in T01.
- Circuit map input: until T31 exists use an identity map + `--placeholder` flag; the tool
  must refuse to write non-placeholder output without a real map.

# Files in scope
`tools/convert_legacy.py`, `tools/l513_circuit_map.example.json`, `tests/test_convert_legacy.py`.

# Backup
none (dev-only; outputs go to a new folder).

# Steps
1. Inputs: old config JSON, circuit map `{"input/2_05": "di/2_05", "relay/2_02": "ro/2_02",
   "input/UART_4_4_02": "di/<xS30 name>", …}`, the T20 contract (for command topics, which are
   not in the old config).
2. Mapping rules:
   - `description` → `name`; `device_normal: nc` → `inverted: true`;
   - `device_delay > 0` (non-counter) → `off_delay_s`; motion-like names → `device_class: motion`,
     `Contact`/`Slot` → `door`/`lock` (*suggestion only*, flagged in report);
   - `device_type: counter` → `counter: true`, `counter_interval_s = device_delay`;
   - `dev: ai` + `interval` → `transform.scale 200`, `unit lx`, `device_class illuminance`,
     `sampling.mean`, `publish_interval_s = interval`;
   - `dev: temp` → 1-wire temp sub-key, `valid_range [-55,125]`, sampling (interval ≈ samples:
     evok v2 interval 3 s × (`interval`+1));
   - `dev: humidity` → 1-wire humidity sub-key, `valid_range [0,100]`;
   - `handle_local.type bel` → rule `pulse` (count = `rings`, on 100, off 300 ms) on the
     button's active edge + circuit `ro/<bell>` gets `failsafe_off`, `max_on_s: 2`, presets;
   - `handle_local.type dimmer` → rule `dimmer` with `action_value = level`;
   - `handle_local.type switch` → rule `toggle`;
   - each `state_topic` → legacy_map `states` entry with the right `format`;
   - each command topic from T20 → legacy_map `commands` entry; roof-window circuit gets
     `failsafe_off: true`, `max_on_s: 60`.
3. Output folder `out/legacy-<date>/`: `circuits.json` (to merge into config),
   `local_rules.json`, `legacy_map.json`, `REPORT.md` (every old entry → new entries,
   warnings, suggestions needing a human decision).
4. Tests on the old config copy: every entry produces output or an explicit warning; the
   two bel rules, five dimmer rules and the counter are present.

# Acceptance checks
- Tests green; `REPORT.md` for the placeholder run attached to the PR.

# Rollback
Delete tool; nothing live.

# Feed the elephant
`/log.md`; mention the tool in `/runbooks/deploy.md` (L513 section, after T31).

# Evidence

# Open questions
