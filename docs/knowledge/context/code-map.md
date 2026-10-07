---
type: Code Map
title: "hass-unipi.py structure at v2.0.0 (live 2026092501)"
description: "Where things live in the 3960-line bridge script, with the hook points the new features attach to."
resource: file:///home/unipi/unipi-homeassistant/scripts/hass-unipi.py
tags: [code, architecture]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
sources:
  - id: live-script
    resource: file:///home/unipi/unipi-homeassistant/scripts/hass-unipi.py
    title: sha256 dd4afeda…3161, 3960 lines
---

Line numbers are for sha256 `dd4afeda…3161`. After T02 the same file is `hass-unipi.py`
at tag `v2.0.0`. **Goldfish: re-grep the symbol, never trust a line number blindly.**

# Runtime model

- One asyncio loop (`UnipiBridge.loop`) + paho MQTT network thread + two worker threads:
  `MQTTWorker` (drains `websocket_to_mqtt_queue` → `mqtt_client.publish`, retains topics
  ending in `/state`, `/config`, `/status`, …) and `WebSocketWorker` (drains
  `mqtt_to_websocket_queue` → `send_to_websocket`, **holds a message up to 30 s** while WS is
  down, then drops it).
- Paho callbacks run in the paho thread and hop to the loop with
  `loop.call_soon_threadsafe`.
- State cache: `self.device_states["<dev>_<circuit>"]` (1-wire: `1wdevice_<c>_<key>`).

# Symbols

| Area | Symbol (≈line) | Notes / hook for |
|---|---|---|
| Config models | `MqttConfig`…`AppConfig` (114–240) | T11 adds `circuits`, `mode`, `legacy`; keep `inputs` |
| Local rules | `RuleCondition`, `LocalLogicRule`, `LocalLogicEngine` (338–508) | T17 new action types |
| Bridge init | `UnipiBridge.__init__` (512) | T10 creates `CommandService`, `EventBus` |
| Startup | `run` (648), `_async_startup` (876), `perform_discovery_and_mqtt_subscribe` (1440) | T12 fail-safe OFF after discovery/WS connect |
| MQTT in | `on_mqtt_message` (1123) | **Routes every JSON `/set` to `process_ao_transition`** → T12 must route by `dev` |
| MQTT payload parse | `process_payload` (3249) | bare JSON int ⇒ `{"brightness": n}` |
| Digital command | `process_mqtt_message` (2691) | ON/OFF → WS + REST read-back verify + ack |
| AO fades | `process_ao_transition` (2810), `_perform_ao_transition_task` (2890), `active_ao_transitions` | model for sequencer cancellation |
| Ack | `mqtt_ack` (2971) | T10: emit `output_changed` event here |
| Evok in | `websocket_handler` (2325) → `process_websocket_message` (2529) | deadband/dedup; local rules evaluated here; T10 emits `input_changed`; T14/T15/T16 hook here |
| Discovery | `publish_discovery_config` (1969), `_publish_1wdevice_discovery` (2164), topic builders (2268, 2304) | T11 names/device_class/area; T13 buttons; T15 counter sensor; T19 component override |
| Republish | `republish_all` (1681) | must include new entities (buttons, counters, attributes) |
| Local actions | `execute_local_action` (3322), `handle_dimmer_action` (3447), `_dimmer_hold_task` (3526) | T17 |
| Availability/health | `publish_availability` (3572), `publish_error` (3592), `_manage_startup_logging` (781) | T03 health check reads `startup_error` |
| Web UI/API | `setup_web_server` (3612) + handlers; UI `web/index.html` (Blockly) | T17 Blockly blocks |
| Entrypoint | `__main__` (3909): `--config`, `--record` | T18 `--mode` override optional |

# Tests

`tests/` (pytest, offline). Run: `/home/unipi/unipi-homeassistant/bin/python -m pytest -q`.
`conftest.py` provides `bridge` (UnipiBridge with FakeMqtt/FakeWebSocket, REST patched with
`tests/fixtures/s103_rest_all.json`), `discovered` (after initial discovery), `fast_sleep`,
`settle()`, `drain()`, `fx()`. Every new task adds tests here; a change to a pinned behaviour
must update the test deliberately and say so in the commit.

# Target module layout (introduced gradually, never big-bang)

```
hass-unipi.py            # stays the entrypoint + UnipiBridge (shrinks over time)
unipi_core/
  events.py              # T10 EventBus (input_changed, output_changed, availability)
  commands.py            # T10 CommandService: set(), transition(), sequence()
  sequencer.py           # T12 pulse/duration engine + watchdog + fail-safe
  circuits.py            # T11 per-circuit config model + lookup
  signals.py             # T14/T15/T16 hold, counter, transform, sampling, validation
legacy_adapter.py        # T21 — deleted in T42
tests/                   # T04 onward
tools/                   # T03 backup/deploy/rollback
```

Rule: new code goes into `unipi_core/`; `hass-unipi.py` only gets thin hooks. This keeps
every task diff small and reviewable by a goldfish.
