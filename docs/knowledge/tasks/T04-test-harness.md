---
type: Task
id: T04
title: "Create a pytest harness with characterization tests of current behaviour"
description: "tests/ runs offline on any machine, with fakes for MQTT, evok WebSocket and REST, and pins down today's behaviour of discovery, state publishing, commands, AO fades and local rules."
phase: 0
task_status: done
depends_on: [T02]
risk: low
human_gate: false
target_hosts: [dev-only]
tags: [tests]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
No automated tests exist (R4). Every later goldfish must be able to prove "I changed X and
nothing else". Characterization ("golden master") tests capture current behaviour first.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/code-map.md](/context/code-map.md)
- [/context/interface-core.md](/context/interface-core.md) §1

# Preconditions
- `requirements.txt` already lists `pytest`, `pytest-asyncio`, `pytest-mock`.
- `/home/unipi/unipi-homeassistant/bin/python -c "import pytest"` works.

# Files in scope
`tests/**`, `pytest.ini`, `tests/fixtures/**`. **No changes to `hass-unipi.py`** except, if
strictly needed for importability, moving nothing — import the module via
`importlib.util.spec_from_file_location("hass_unipi", "hass-unipi.py")` (file name has a dash).

# Backup
none (dev-only).

# Steps
1. Fixtures: `tests/fixtures/s103_rest_all.json` from
   `GET http://127.0.0.1:8080/rest/all` (read-only); `tests/fixtures/l513_v2_rest_all.json`
   from `http://<L513_IP>:8080/rest/all` (reference only — evok v2). Strip nothing; these
   contain no secrets. Add a synthetic `l513_v3_rest_all.json` **later** in T31.
2. Fakes: `FakeMqtt` (records `publish(topic,payload,qos,retain)`, `subscribe`, has
   `is_connected()`), `FakeWebSocket` (async `send` records, `recv` from a queue, `state`),
   REST via `aiohttp` test server or by monkeypatching `UnipiBridge.get_unipi_data`.
3. Build `UnipiBridge` with a test `AppConfig` (no file I/O for logging: point
   `logging.file_path` to `tmp_path`). Drain `websocket_to_mqtt_queue` directly instead of
   running the worker threads.
4. Characterization tests (assert exact topics/payloads):
   - discovery for each dev type in the S103 fixture (count + 3 full payload snapshots);
   - `process_websocket_message`: di ON/OFF, dedup, ai deadband, ao state, 1wdevice keys;
   - `process_mqtt_message` ON/OFF → WS command JSON + ack;
   - `on_mqtt_message` JSON AO with transition → steps sent, ack at end; retained `/set` ignored;
   - local rules: `set` rule and `dimmer` short press.
5. `pytest -q` must run in < 60 s on the Pi; mark anything slow with `@pytest.mark.slow`.

# Acceptance checks
- `cd ~/src/unipi-homeassistant && /home/unipi/unipi-homeassistant/bin/python -m pytest -q` → all pass.
- Mutation sanity: temporarily change the ON payload in `mqtt_ack` to `"On"` → at least one test fails; revert.

# Rollback
Delete `tests/`.

# Feed the elephant
`/context/code-map.md` add "Tests" row; `/log.md`. After merge with T03: tag `v2.1.0`
(no deploy needed: runtime code unchanged — verify with `git diff v2.0.0 v2.1.0 -- hass-unipi.py` empty).

# Evidence
- 2026-10-07: `/home/unipi/unipi-homeassistant/bin/python -m pytest -q` → **43 passed in ~8 s** (offline: fake MQTT, fake WebSocket, REST patched with the recorded S103 snapshot; runtime-dir files redirected to tmp).
- Coverage of behaviour pinned: discovery (counts per component, device/availability/LWT, switch/light/sensor/1-wire payloads, inverted inputs, subscribed command topics, initial states), WS→MQTT (dedup, 0.05 deadband boundaries, AO JSON, echo suppression, 1-wire, lists, republish), MQTT→WS (ON/OFF + ack, retained ignored, invalid payload, AO instant/fade/off/out-of-range, payload parsing), local rules (engine, conditions, end-to-end, no fire on republish, dimmer toggle, rules file round-trip).
- **Mutation sanity (8 deliberate breaks of `hass-unipi.py`, each restored via `git checkout`)**: ack casing, AO scale, digital dedup, retained `/set`, rules-on-republish, dimmer default level, AO min step → all caught on the first run. The analog deadband change (0.05→0.5) **survived** the first version of the tests; boundary cases (+0.04 / +0.1) were added and it is now caught.
- Two tests pin *known gaps on purpose* and must be updated by the task that fixes them: JSON `/set` to a relay is ignored (K1 → T12), dimmer default level is a fixed 10 V (K5 → T17).
- Fixtures: `tests/fixtures/s103_rest_all.json` (S103 snapshot), `l513_evok2_rest_all.json` (reference only; evok v2 shapes).

# Open questions
