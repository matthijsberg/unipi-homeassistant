---
type: Task
id: T12
title: "Build the output sequencer (pulse trains, timed outputs, limits, fail-safe)"
description: "A single MQTT JSON message can ring the bell N times with chosen on/off timing or switch an output on for N seconds; limits, cancellation-to-OFF, no-late-execution, watchdog and fail-safe OFF are enforced and tested."
phase: 1
task_status: done
depends_on: [T11]
risk: high
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, doorbell, safety, sequencer]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Closes the headline gap G1 (doorbell repeat with count + timing in one message) and G2
(duration), plus robustness items R1/R2 and K1/K2/K7. Normative spec:
`/context/interface-core.md` §2 and ADR-003. Do not invent behaviour beyond that spec.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-003-pulse-sequence-command.md](/decisions/ADR-003-pulse-sequence-command.md)
- [/context/interface-core.md](/context/interface-core.md) §2, §4
- [/context/code-map.md](/context/code-map.md) (`on_mqtt_message`, `process_payload`, `websocket_worker_thread`, `_perform_ao_transition_task`)

# Preconditions
- T11 merged (circuit registry available); tests green.

# Files in scope
`unipi_core/sequencer.py`, `unipi_core/commands.py` (add `sequence()`, `cancel()`),
`hass-unipi.py` (routing in `on_mqtt_message` by dev; fail-safe at startup/WS reconnect/
shutdown; attributes topic), `config.example.json`, `tests/test_sequencer.py`,
`tests/test_mqtt_routing.py`.

# Backup
`tools/backup.sh pre-T12` (S103).

# Steps
1. `sequencer.py` — `OutputSequencer(send_ws, ack, emit, registry, clock=time.monotonic)`:
   - `start(dev, circuit, spec, origin)` where spec is `Pulse(count,on_ms,off_ms)` or
     `Timed(state, duration_s)`; validates against circuit limits (defaults:
     `max_count` 10, `max_pulse_ms` 2000, `off_ms` 50–10000, `on_ms` ≥ 20, `max_on_s` 3600)
     → raise `SequenceRejected(reason)`.
   - One `asyncio.Task` per circuit; starting a new one cancels the old; the cancel path
     always sends OFF and awaits it before the new sequence starts.
   - Writes go **directly** via `send_ws` (an async function that sends on the live
     WebSocket and raises if not OPEN) — never via `mqtt_to_websocket_queue`.
   - If WS not OPEN at start → reject. If a send fails mid-sequence → mark circuit
     `needs_failsafe`; on next WS OPEN send OFF.
   - Schedule by absolute deadlines (`t0 + k*(on+off)`), not cumulative sleeps.
   - Ack: state `ON` at start, `OFF` at end/cancel; attributes JSON per spec rule 7; emit
     `output_changed(origin="sequence")`.
2. Watchdog (in the sequencer module, started by the bridge): every 1 s, for circuits with
   `max_on_s` whose `device_states` value is ON for longer than `max_on_s` (track ON-since
   from `output_changed`/`input_changed` events of that output) → send OFF + log WARNING +
   attributes `last_error`.
3. Fail-safe: for circuits with `failsafe_off: true` send OFF (a) after initial discovery
   completes, (b) after every WS reconnect, (c) in `shutdown()` before MQTT disconnect.
4. Routing (K1/K7): in `on_mqtt_message`, decide by **dev from the topic** first:
   AO → existing transition path; digital outputs (`do`,`ro`,`led`, aliases) → JSON with
   `pulse`/`duration_s`/`preset` → sequencer; JSON `{"state":"ON"|"OFF"}` → existing
   ON/OFF path; anything else → error log. Plain `ON`/`OFF` unchanged.
   A plain `ON`/`OFF` for a circuit with a running sequence cancels it (rule 4).
5. Discovery: add `json_attributes_topic` = `…/<dev>/<circuit>/attributes` to switches of
   circuits that have sequencer options configured (other switches unchanged).
6. Tests with a fake clock and fake WS (no real sleeping): exact on/off sequence and
   timestamps for `count=3,on=100,off=250`; reject over-limit; cancel mid-pulse → OFF sent
   before new command; WS down at start → rejected, nothing sent; WS drop mid-sequence →
   OFF on reconnect; watchdog OFF after `max_on_s`; fail-safe OFF on startup/shutdown;
   duration `ON` 35 s → OFF at 35 s; `OFF`+duration → ON at end.
7. Real-hardware test on **S103 front-panel LEDs only** (`led/1_01`…): configure
   `circuits."led/1_01"` with presets; publish
   `{"pulse":{"count":3,"on_ms":100,"off_ms":250}}` to its `/set`; observe 3 blinks; measure
   timing from evok WS echo timestamps in the debug log (target jitter < 30 ms).

# Acceptance checks
- All unit tests green; T04 characterization tests unchanged and green.
- LED test evidence (log excerpt with timestamps) in Evidence.
- Stop the bridge (`systemctl stop`) during a 10 s duration test on an LED → LED is OFF
  after stop (fail-safe on shutdown). `kill -9` during the same test → LED OFF within 15 s of
  restart (fail-safe after WS connect). Document both.
- S103 deploy `v2.2.0-rc3`, 24 h soak, health OK.

# Rollback
`tools/rollback.sh v2.2.0-rc2`. Caution: from T11 on, unknown keys inside `circuits` are
rejected at start-up, and rc2 does not know the sequencer keys (`presets`,
`pulse_defaults`, `max_count`, `failsafe_off`, …). Remove those keys from `config.json`
**before** rolling back, or the rolled-back bridge will refuse to start (the health check
would then roll forward again).

# Feed the elephant
`/context/interface-core.md` §2 mark implemented + measured jitter; known-issues K1/K2/K7
→ handled; `/log.md`.

# Evidence
- 2026-10-07: `pytest -q` → **150 passed** (45 original + 17 T10 + 30 T11 + 58 T12). Changed on purpose: `test_json_command_to_relay_is_not_supported_yet` (pinned K1) → `test_unsupported_json_to_relay_is_rejected_not_sent_to_fade_path`; `test_unsupported_options_fail_loudly` no longer uses `presets` as its "planned" example.
- Code: `unipi_core/sequencer.py` (`OutputSequencer`, `parse_command`, `Limits`, `Pulse`, `Timed`); circuit options `failsafe_off, max_on_s, pulse_defaults, max_count, max_pulse_ms, presets` (digital outputs only, validated at start-up **including that every preset fits the circuit's own limits**); bridge wiring: JSON routing by device type (K1/K7), direct write path `_ws_write_now` (K2), attributes topic, echo suppression while a sequence runs, plain ON/OFF cancels a running sequence, max-on watchdog task, fail-safe OFF after discovery/WS (re)connect and at shutdown, `json_attributes_topic` in discovery for configured outputs.
- Timing is proven with a deterministic fake clock (exact write times for 3×100/250 ms = 0, 0.1, 0.35, 0.45, 0.7, 0.8 s; 5 ms write latency does not accumulate drift). **Real-hardware jitter has not been measured yet** (needs deploy; see below).
- **Mutation checks, 12 safety-oriented breaks** (cancel without OFF, clamp instead of reject, late execution, cumulative drift, watchdog dead, lost fail-safe retry, missing OFF ack, minimum pulse removed, echo suppression removed, fade-path routing, no shutdown fail-safe, plain command not cancelling): 10 caught at once; **2 survivors** were then analysed: the drift mutation was too weak (OFF stayed absolute) and re-done as truly cumulative → caught; the "plain command cancels" test was too weak (the natural end-OFF looked identical to the cancel-OFF) → test now asserts the *time* of the OFF (t=1, not t=4) → caught.
- Design note: a plain `ON`/`OFF` still goes through the bridge's normal queue (30 s hold, K2 only for plain commands); the "no late execution" guarantee applies to sequences, which use the direct path. Plain-command late execution remains (candidate follow-up).

## Hardware results (S103, after the planned reboot, `v2.2.0-rc3` → `rc5`, front-panel LED `led/1_01`)
Reboot first: the box booted cleanly on the updated Raspberry Pi firmware, all services active, no stale libraries, no errors.
- **Config used** (live `config.json`, backup `~/backups/config.json.before-T12-hwtest`): `circuits."led/1_01"` = `{failsafe_off: true, max_on_s: 15, max_count: 6, presets: {blink3: 3 pulses}}`. Left in place for the soak; remove afterwards if undesired.
- **Rejections (5/5)**: count 7 / on_ms 5 / duration 99 / unknown preset / unknown key → each rejected with a precise `last_error` on the attributes topic; LED never switched.
- **Preset `blink3`**: attribute timeline `remaining 3 → 2 → 1 → done` at 350 ms spacing = exactly the 100+250 ms plan; command → first write ≈ 40 ms (MQTT + bridge).
- **Plain OFF during a timed run, timed ON, watchdog (plain ON forced OFF after `max_on_s`)**: behaved correctly end to end; only coarse timing is possible (see K16).
- **Graceful `systemctl stop` mid timed run → LED ended OFF** (shutdown fail-safe; without it the output would stay ON forever because the sequence's timer dies with the process).
- **`kill -9` mid timed run**: rc4: OFF 18.3 s after the kill (10 s systemd `RestartSec` + ~8 s stabilisation/discovery before the fail-safe). Fix in rc5 (fail-safe the moment the WebSocket connects, retried after discovery): **13.5 s**, of which the bridge itself needs 0.09 s after start. Remaining exposure is systemd's `RestartSec=10`; see K18.
- **Bugs found only on hardware**: (1) watchdog was blind for outputs evok does not push over WebSocket (rc4 fix: it now follows the sequencer's own writes and plain-command acks; a ring train cannot trip it); (2) 8 s of avoidable fail-safe delay after a crash (rc5 fix).
- **Limit of what could be measured**: evok neither pushes `led` changes over its WebSocket nor refreshes the REST value in under ~2–5 s, so the *physical* ring timing of this LED cannot be observed from software. Bridge-side timing is exact (above); the physical edge timing needs a relay/`do` with a scope, or a visual check of the LED. **Open for Matthijs.**
- Soak: started 16:16 UTC with rc5; 24 h. Not a final release yet (`v2.2.0` after T13–T18 as planned).

# Open questions
