---
type: Task
id: T11
title: "Add per-circuit configuration (names, device classes, areas, logical inversion)"
description: "config.json accepts an optional circuits object; discovery uses name/device_class/area; inverted inputs publish the logical state; old inputs.<c>.inverted still works."
phase: 1
task_status: todo
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

# Open questions
