---
type: Task
id: T10
title: "Introduce EventBus and CommandService (pure refactor, no behaviour change)"
description: "unipi_core/events.py and unipi_core/commands.py exist; hass-unipi.py emits input_changed/output_changed/availability events and routes all outputs through CommandService; all T04 tests still pass unchanged."
phase: 1
task_status: in_progress
depends_on: [T04]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [core, refactor]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
The sequencer (T12), signal processing (T14–T16) and the legacy adapter (T21) all need one
place to *send* output commands and one place to *observe* state changes (ADR-002 §3).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/code-map.md](/context/code-map.md)
- [/decisions/ADR-002-legacy-adapter.md](/decisions/ADR-002-legacy-adapter.md)

# Preconditions
- `pytest -q` green on `main`.

# Files in scope
`unipi_core/__init__.py`, `unipi_core/events.py`, `unipi_core/commands.py`,
`hass-unipi.py` (thin hooks only), `tests/test_events_commands.py`, `tools/deploy.sh`
(include `unipi_core/` — if not already).

# Backup
`tools/backup.sh pre-T10` on S103 before deploying.

# Steps
1. `events.py`: `EventBus` with `subscribe(kind, callback)` and `emit(kind, **data)`;
   callbacks run on the asyncio loop (`loop.call_soon_threadsafe` when emitted from another
   thread); exceptions in a subscriber are logged and never break the emitter.
   Kinds + payloads:
   - `input_changed(dev, circuit, value, raw, ts, source)` — after dedup/deadband, `source="ws"|"rest"|"republish"`; republish events carry `source="republish"` and subscribers may ignore them.
   - `output_changed(dev, circuit, value, ts, origin)` — whenever `mqtt_ack` publishes; `origin="mqtt"|"rule"|"sequence"|"fade"`.
   - `availability(online: bool)`.
2. `commands.py`: `CommandService(bridge)` with async methods `set(dev, circuit, value, origin)`,
   `transition(dev, circuit, target_0_1000, seconds, origin)`. Internally call the *existing*
   code paths (`mqtt_to_websocket_queue`, `process_ao_transition` logic). No new behaviour.
3. Hooks in `hass-unipi.py`: create `self.events`, `self.commands` in `__init__`; emit
   `input_changed` in `process_websocket_message` (after state update, before rules);
   emit `output_changed` in `mqtt_ack`; emit `availability` in `publish_availability`.
4. Tests: subscriber receives events for a di change, an MQTT ON command, an AO fade end;
   a raising subscriber does not stop publishing.

# Acceptance checks
- All T04 characterization tests pass **without modification**.
- New tests pass.
- Deploy to S103 via `tools/deploy.sh` with a pre-release tag `v2.2.0-rc1`; health OK; 24 h
  soak: no new ERROR lines (`grep -c ERROR` on the log vs. the previous day).

# Rollback
`tools/rollback.sh v2.1.0`.

# Feed the elephant
`/context/code-map.md` (new module rows), `/log.md`.

# Evidence
- 2026-10-07: `pytest -q` → **62 passed** (45 existing tests **unmodified** + 17 new in `tests/test_events_commands.py`). Only test-infrastructure change: `conftest.py` puts the repo root on `sys.path` so `hass-unipi.py` can import `unipi_core` (as when run from its directory).
- Code: `unipi_core/events.py` (EventBus: sync dispatch when on the loop thread or loop idle, `call_soon_threadsafe` from foreign threads, subscriber exceptions isolated), `unipi_core/commands.py` (`send_ws`, `set_digital`, `transition`). Hooks in `hass-unipi.py`: all 7 WS-command call sites + 4 `mqtt_ack` origins routed through `CommandService`; `INPUT_CHANGED` (generic + 1-wire sub-keys, source `ws|republish`), `OUTPUT_CHANGED` (from `mqtt_ack`, origin `mqtt|rule|fade`), `AVAILABILITY`.
- Mutation checks (4 deliberate breaks of new code, all caught; one mutation of mine was initially invalid because it narrowed `except` to the very exception the test raises — redone with `KeyError`).
- Deliberate non-change recorded as **K15**: AO fade steps still use a *blocking* `queue.put` on the event-loop thread (`send_ws(block=True)`) exactly like before. If the WS queue is full this can stall the loop; T12's sequencer replaces this path.

# Open questions
