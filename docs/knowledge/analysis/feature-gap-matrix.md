---
type: Analysis
title: "Feature gap matrix — old unipi_mqtt.py vs new hass-unipi.py"
description: "Every behaviour of the old script, whether the new bridge has it, and where it must live after migration (core option, Home Assistant, legacy adapter, or dropped)."
tags: [analysis, migration, gaps]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
sources:
  - id: old-script
    resource: file:///home/unipi/scripts/old_unipi_mqtt/unipi_mqtt.py
  - id: new-script
    resource: file:///home/unipi/unipi-homeassistant/scripts/hass-unipi.py
---

Destination legend: **CORE** = generic, per-circuit-configurable feature in the bridge ·
**HA** = Home Assistant config/automation · **ADAPTER** = legacy topic/payload translation
only (disappears in T42) · **DROP** = not carried over.

| # | Old behaviour | New bridge today | Gap | Destination | Task |
|---|---|---|---|---|---|
| G1 | **Doorbell `repeat`**: N rings, fixed 100 ms on / 250 ms off, interruptible, extra OFF on stop | ✗ (`/set` JSON is always treated as AO fade) | Full | **CORE** pulse sequence `{"pulse":{count,on_ms,off_ms}}` with limits + fail-safe; **HA** button per preset; **ADAPTER** maps `{"state":"pulse","repeat":n}` | T12, T13, T21 |
| G2 | **`duration`**: output ON (or OFF) for N s, then inverse (roof window 35 s) | ✗ | Full | **CORE** `{"state":"ON","duration_s":N}` (same sequencer) + `max_on_s` watchdog | T12, T21 |
| G3 | Relay/output ON/OFF from HA | ✓ (`ro`/`do` switch, read-back verified) | none | CORE (exists); ADAPTER maps old JSON | T21 |
| G4 | AO brightness 0–255 + `transition` s, interruptible | ✓ 0–1000 scale, ≤ 60 s | scale only | CORE (exists); ADAPTER converts 255↔1000 | T21 |
| G5 | AO `on` without brightness → error | ON ⇒ 100 % | behaviour differs | CORE keeps 100 %; ADAPTER: `on` w/o brightness ⇒ last non-zero level (better than legacy) | T21 |
| G6 | **PIR hold** `device_delay` (retriggerable off-delay) | ✗ (publishes raw edges) | Full | **CORE** `off_delay_s` (HA `off_delay` is *not* enough: raw OFF edges overwrite it and local rules/legacy need the held state) | T14 |
| G7 | NC/NO `device_normal` | ~ `inputs.<c>.inverted` swaps HA payloads only | logical state missing | **CORE** logical inversion (state + rules) | T11 |
| G8 | **Water-meter counter**: absolute + delta every 10 s | ✗ | Full | **CORE** `counter: true` → `total_increasing` sensor; **HA** utility_meter for deltas; **ADAPTER** computes legacy `counter_delta` | T15, T21 |
| G9 | Lux from AI: mean over ~60 samples × 200 | raw volts, deadband 0.05 | Full | **CORE** `transform` (scale/offset/round/unit/device_class) + `sampling` (mean, publish interval) | T16 |
| G10 | Temp/humidity averaging + range check (−55…125 °C, 0…100 %) | raw, deadband 0.05, no range check | Partial | **CORE** `sampling` + `valid_range` (+ optional `reject_values` e.g. 85.0 DS18B20 reset) | T16 |
| G11 | DS2438 `vis` → lux × 8000 | raw `vis` volts | Partial | **CORE** transform on 1-wire sub-key | T16 |
| G12 | Local **bel**: button → ring relay N× | ✗ rule actions are only set/dimmer | Full | **CORE** rule `action_type: pulse` (+ Blockly block) | T17 |
| G13 | Local **dimmer** toggle to fixed level (5/10 V) | ✓ dimmer rule (toggle + hold-to-dim) but default level fixed 10 V | small | **CORE** dimmer default level from `action_value` | T17 |
| G14 | Local **switch** toggle relay | ✗ | Full | **CORE** rule `action_type: toggle` | T17 |
| G15 | Local action → HA state update | ✓ optimistic `mqtt_ack` | none | CORE; ADAPTER mirrors to legacy state topics via `output_changed` events | T10, T21 |
| G16 | Per-entity `/available` topics | ✓ device-level availability (better) | topic layout | CORE (exists); ADAPTER publishes per legacy topic | T21 |
| G17 | First-run state sync on WS open | ✓ REST snapshot + republish on HA birth/MQTT reconnect (better) | none | CORE (exists) | – |
| G18 | Friendly names (`description`), implied device classes | ✗ names like `di 1_01` | Full | **CORE** `circuits.<id>.name/device_class/area` in discovery | T11 |
| G19 | Ventilation AO as percentage (HA side today) | AO = light | UX | **HA** keep template fan initially; optional **CORE** `ha_component: fan|number` | T19 (optional) |
| G20 | Interrupt running action on new command | ✓ for AO fades | extend | CORE sequencer cancellation (always OFF first) | T12 |
| G21 | Runs on evok v2 | ✗ (no `device_info`, `modes` list shape, `input`/`relay` names) | Full | **DROP** — L513 upgraded to evok 3 (ADR-001) | T30–T32 |
| G22 | `effect`, `raw_mode`, `mqtt_reply_message`, `handle_other` | – | – | **DROP** (never implemented / unused) | – |
| G23 | Old dimmer publishes retained `/set` to itself | – | – | **DROP** (ADAPTER publishes ack on state topic instead) | T21 |
| G24 | Hard-coded credentials, no watchdog, no LWT | new has config/env, LWT, MQTT watchdog | – | CORE (exists) | – |

## New-only robustness items required for safe migration

| # | Item | Why | Task |
|---|---|---|---|
| R1 | Commands must not execute late (30 s hold queue) | a ring or window-open 30 s after the request is wrong; a dropped OFF is dangerous | T12 |
| R2 | Fail-safe OFF + max-on watchdog for coil/motor outputs | bridge crash mid-pulse would leave the bell/motor energised | T12 |
| R3 | Shadow mode | test a second instance on a live box without touching outputs | T18 |
| R4 | Test harness + characterization tests | no automated tests exist today | T04 |
| R5 | Deploy/rollback tooling with health check | live system, must be reversible in minutes | T03 |

## What moves to Home Assistant (and why)

- **Consumption/deltas** (water meter): HA `utility_meter`/statistics handle resets and
  long-term stats better than the bridge.
- **Which ring pattern when** (e.g. different pattern at night, notification to phones):
  HA script calling the core pulse command or a preset button.
- **Fan/percentage presentation** of ventilation until/unless T19 is done.
- **Anything needing data from outside one Unipi** (presence, time of day, alarm state).

Everything that must keep working when HA or MQTT is down (doorbell buttons, wall
switches, PIR hold for local rules, safety limits) stays in the **CORE**.
