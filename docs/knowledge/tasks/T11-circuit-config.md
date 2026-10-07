---
type: Task
id: T11
title: "Add per-circuit configuration (names, device classes, areas, logical inversion)"
description: "config.json accepts an optional circuits object; discovery uses name/device_class/area; inverted inputs publish the logical state; old inputs.<c>.inverted still works."
phase: 1
task_status: in_progress
depends_on: [T10]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, config, discovery]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes G7 and G18 and gives T12–T19 one place to read per-circuit options
(`/context/interface-core.md` §4). Also fixes K3 and K6.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §3, §4
- [/analysis/known-issues.md](/analysis/known-issues.md) K3, K6

# Preconditions
- T10 merged; tests green.
- Record which S103 inputs are inverted today: `jq .inputs /home/unipi/unipi-homeassistant/scripts/config.json` (may be `{}` / absent).

# Files in scope
`unipi_core/circuits.py`, `hass-unipi.py` (AppConfig + discovery + state publish hooks),
`web/index.html` only if the inputs page must keep working (it calls `/api/inputs`),
`config.example.json`, `tests/test_circuits.py`.

# Backup
`tools/backup.sh pre-T11` (S103).

# Steps
1. `circuits.py`: pydantic `CircuitConfig` with all keys from interface-core §4 (all optional;
   unknown keys rejected with a clear error naming the circuit). `CircuitRegistry.get(dev,
   circuit, subkey=None)` with dev aliases (`input`≡`di`, `relay`≡`ro`, `output`≡`do`,
   `analogoutput`≡`ao`) and fallback to legacy `inputs.<circuit>.inverted`.
2. `AppConfig`: add `circuits: dict[str, CircuitConfig] = {}`. Validate at start; a bad
   config must fail start-up loudly (systemd restarts; healthcheck catches it).
3. Discovery: `name` (else today's `"{dev} {circuit}"`), `device_class` (binary_sensor/
   sensor), `suggested_area` from `area` (HA discovery key `sa` / `suggested_area` in the
   device block is per-device; for entities use `name` only and document the limitation —
   verify against current HA MQTT docs and record which key works).
4. Logical inversion: for `inverted` circuits invert the value **once** at the input edge
   (before `device_states`, rules and events) and publish plain `ON`/`OFF` payloads in
   discovery. Keep `raw` in the `input_changed` event.
5. Migration on S103: if any input is inverted today, discovery payloads change from swapped
   to normal and states flip accordingly — the HA entity state stays identical. Add a test
   proving "HA-visible state unchanged" for an inverted input before/after.
6. K6: resolve `local_rules.json` relative to the config file directory.

# Acceptance checks
- Tests: name/device_class in discovery; inverted input → rule sees logical value; legacy
  `inputs` key still honoured; invalid key → start-up error message names the circuit.
- S103 deploy (`v2.2.0-rc2`): every HA entity of the S103 shows the same state as before
  (compare `/api/status` + HA states before/after; list in Evidence).

# Rollback
`tools/rollback.sh v2.2.0-rc1` (config without `circuits` stays valid for both versions).

# Feed the elephant
`/context/interface-core.md` §3/§4 (mark implemented), `/analysis/known-issues.md`
K3/K6 → handled, `/log.md`.

# Evidence
- 2026-10-07: `pytest -q` → **91 passed** (45 original + 17 T10 + 29 T11). One original test changed **on purpose**: `test_inverted_input_swaps_payloads` → `…uses_plain_payloads_and_logical_state` (the old behaviour is exactly what K3 describes).
- Implemented: `unipi_core/circuits.py` (model, canonical keys incl. aliases, per-device validity, `CircuitRegistry`); `AppConfig.circuits`; discovery `name` / `device_class` (binary_sensor, sensor, switch; 1-wire sub-key names); logical inversion at the WS edge, in the initial snapshot, in the discovery initial state and in the web NO/NC handlers; rules path next to the config file.
- **Deviation from the card (research, not guesswork)**: `area` is rejected, not implemented — HA MQTT discovery has `suggested_area` only inside the *device* block (docs checked 2026-10-07); no per-entity area key exists. Also: only options that exist are accepted; planned ones fail with an explicit message (so a setting is never silently ignored). Unknown `device_class` values only log a warning (HA may add classes).
- Proof that HA sees no change: `test_home_assistant_sees_the_same_state_as_before_t11[0|1]` (old swapped-payload mapping vs new plain payloads, both contact positions).
- Mutation checks: 9 deliberate breaks; 8 caught immediately, 1 survived (state cache seeded with physical values at start-up) → `test_state_cache_is_seeded_with_logical_values_at_startup` added and verified to kill it.
- S103 migration note: the live `config.json` has no `inputs`/`circuits`, and `local_rules.json` is `[]`, so nothing is inverted today and no rule changes meaning.

# Open questions
