---
type: Task
id: T17
title: "Add pulse and toggle local-rule actions and a configurable dimmer level"
description: "Local rules can ring a bell (pulse via the sequencer), toggle a digital output, and dimmer rules use action_value as default on-level; the Blockly editor supports all three."
phase: 1
task_status: in_progress
depends_on: [T12]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, local-rules, web-ui]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes G12, G13, G14 and K4/K5, so all old `handle_local` behaviour can be expressed as core
rules (ADR-002 §5) and keeps working without HA/MQTT.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-core.md](/context/interface-core.md) §5
- [/context/interface-legacy.md](/context/interface-legacy.md) §3
- [/analysis/known-issues.md](/analysis/known-issues.md) K4, K5

# Preconditions
- T12 merged (sequencer). Current `local_rules.json` on S103 is `[]` (verify; if not empty,
  back it up and make sure existing rules load unchanged).

# Files in scope
`hass-unipi.py` (`LocalLogicRule`, `LocalLogicEngine.evaluate`, `execute_local_action`,
`handle_dimmer_action`), `web/index.html` (Blockly blocks + save/load mapping),
`tests/test_local_rules.py`.

# Backup
`tools/backup.sh pre-T17` (includes `local_rules.json`).

# Steps
1. Model: `action_type` ∈ `set|dimmer|toggle|pulse`; new optional fields
   `action_pulse: {count,on_ms,off_ms}`, `action_preset: str`. Keep `action_transition`
   (ms, K4) and add validation message stating the unit; Blockly label shows "ms".
2. `toggle`: read `device_states` for the target; ON→OFF, else ON; via `CommandService.set`.
3. `pulse`: via `CommandService.sequence(... origin="rule")`; rejection is logged, never raised.
4. Dimmer: default level = `action_value` (V) if set, else 10.0; persist `previous_level`
   per rule in `local_rules_state.json` (gitignored) so a restart doesn't jump to 10 V.
5. Triggers fire on the **active edge** only for pulse/toggle by default
   (`trigger_value: 1` after logical inversion), matching legacy behaviour for momentary buttons.
6. Blockly: blocks "toggle output", "pulse output (count, on ms, off ms) / preset",
   dimmer block gets "default level (V)". Round-trip test: create in UI → `GET /api/rules`
   JSON → reload → identical blocks.

# Acceptance checks
- Unit tests for all four action types (fake clock).
- On S103: a rule "di X active → pulse led/1_01 count 2" blinks the LED twice.
- Works without MQTT: proven by a unit test with `FakeMqtt.is_connected() == False`.
  **Never stop the real broker** to test this — it is shared by the whole house.
- Existing (empty or not) rules file loads unchanged.

# Rollback
Restore `local_rules.json` from backup; `tools/rollback.sh <previous rc>`.

# Feed the elephant
interface-core §5 implemented; G12–G14, K4, K5 handled; `/log.md`.

# Evidence
- 2026-10-07: `pytest -q` → **205 passed** (+ 37 T17 rule tests, 14 web-UI static tests). One old test adapted on purpose: after a sequence the state cache already says OFF (output-state tracking), so the "publishing is normal again" check now uses a real change.
- **Scope changed by Matthijs** (ADR-005): T14/T16 dropped to HA; T17 expanded: `when: always|ha_offline`, validation against circuit limits, rules that fail validation are **disabled but kept**, dimmer level persistence (K5), `hold` switch, tracked output state so `toggle` works for outputs evok never pushes.
- Rule actions: `set` (unchanged), `toggle`, `pulse` (inline `action_pulse` or `action_preset`, timed presets too, through the sequencer so limits/cancel/fail-safe apply), `dimmer` (switch-on level in `action_value`, remembered in `local_rules_state.json` next to the rules, `dimmer_hold`).
- `when: ha_offline` acts only if `homeassistant/status` is not `online` **or** the MQTT link is down (4 cases tested, incl. "HA said online but MQTT is down").
- **UI**: found that the editor rebuilds every rule from scratch on save (it would have erased any new field and the rule id). Now the full rule is kept on the block and merged on save; new blocks for pulse/toggle, dimmer level+hold, and a per-rule "Runs always / only when HA is unreachable" selector. **The page could not be executed here (no Node/browser)**: verified by a JavaScript parser + structural tests only → *needs a click-through by Matthijs*.
- Mutation checks: 11 breaks; 10 caught immediately, 1 survivor (disabled rules still evaluated) because my test used a rule that was rejected at runtime anyway → strengthened with a rule that visibly acts when wrongly evaluated → caught.

# Live result (S103, `v2.2.0-rc6`, 2026-10-07)
- Deployed with `deploy.sh` (healthy). The editor page served by the S103 is the new one.
- **Real-config rule loading test** (no physical trigger available): a valid `pulse`+preset rule, a valid `toggle` rule and an invalid pulse (count 50 > the circuit's max 6) were put in the live rules file → log: `Local rule 'T17 test: too many rings (invalid)' is DISABLED: pulse not allowed on led/1_01: count 50 outside 1..6`, `Loaded 3 local logic rules.`; the file still contained all three; the bridge's startup-error indicator went to `1` (by design) so `healthcheck` reported FAIL. Original `[]` restored (backup `~/backups/local_rules.json.before-T17-test`), restart: healthy, 0 errors.
- Consequence recorded: a disabled rule is a *config* error that makes `deploy.sh` treat the release as unhealthy (and roll back, which would not fix a config problem).
- **Still to do (needs Matthijs at the device)**: (1) click through the rule editor: load, add a pulse/toggle/dimmer rule, set "Runs: only when Home Assistant is unreachable", save, reload the page — values must survive; (2) a real button press that fires a pulse/toggle/dimmer rule; (3) HA-outage test of a `when: ha_offline` rule (stop the HA container or MQTT user) — plus the open question about double-handling in HA automations.

# Follow-up 2026-10-09 (first real rule on the S103 did nothing) → `rc8`
Matthijs' rule "Serre Light" (button `xS51_03` → analog output `1_01` fade) never fired. Two causes, both mine (T17 only normalised device names for the *new* pulse action): trigger `input` vs evok-3 `di` (never matched) and action `analogoutput` vs `ao` (evok would have ignored it); also the saved value was `125` (not a voltage). Fixed with tests using the real rule: names are compared and sent in canonical form; validation refuses impossible values with a reason; disabled rules are a WARNING (not an ERROR, so no false "startup error"/failed deploy) and the editor shows the reason as a warning icon on the block.
**Rule activity** (answer to "show me the flow"): the engine keeps a 300-entry trace (trigger seen/matched, conditions failed and which one, disabled, gated by `when`, delayed, shadow, executed with what was sent, rejected/error); `GET /api/rule_trace?since=<n>`; the editor's right panel has a "Rule activity" list (text built with `textContent`) and the rule's block flashes (blue = trigger matched, green = executed, orange = stopped). Server-side, so a short button pulse is never missed (the old panel only sampled state once per second).
Tests: 271 passed (+36); 10 mutation checks all caught. The editor still cannot be executed here (parser + structural tests only).

# Follow-up 2026-10-09 (2): push-to-dim like a Z-Wave dimmer → `rc9`
Matthijs: on/off works; push-and-hold must dim like a Z-Wave dimmer, he could not find how. It existed as the block "Dimmer Control (AO only)" but was hard to find and had traps: only worked if the trigger operator was manually set to ANY CHANGE, hold time fixed at 0.5 s, dimming down went to 0 V (lamp switched off), direction always "up first".
- Now: tap = toggle (to the switch-on level, or where it was); hold longer than `dimmer_hold_ms` (default 800 in the editor) = dim while held, **direction alternates per hold**, at full brightness it goes down, off → comes on at the minimum and brightens; **dimming down stops at `dimmer_min` (lamp stays on)**, only a tap switches off; `dimmer_speed` V/s; stops on release; remembers the level across restarts; HA is kept in step every ~0.5 s plus a final value on release.
- The rule always follows press **and** release whatever the IF operator says (the editor shows "any"); a failing condition blocks the press but never the release.
- **Bug found by the new tests (not released):** a release whose press had been refused by a condition toggled the lamp off. Releases are now ignored unless their press was accepted.
- Editor: block renamed "Push-to-dim light", fields for hold time / speed / lowest level; the "Runs: only when HA is unreachable" selector is removed from the editor (Matthijs: each function lives either in the Unipi or in HA, no automatic fallback). The `when` setting still exists in the rule JSON and is preserved if a rule has it.
- Tests: 293 (+22); 11 mutation checks, 9 caught at once, 1 equivalent mutant (redundant branch, same behaviour), 1 real gap (final ack) → test strengthened → caught.

# Open questions
