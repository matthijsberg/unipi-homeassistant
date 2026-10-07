---
type: Interface Contract
title: "Legacy MQTT contract of the old unipi_mqtt.py (L513)"
description: "Every topic and payload Home Assistant exchanges with the old script today; the legacy adapter (T21) must reproduce this byte-for-byte where HA depends on it."
resource: file:///home/unipi/scripts/old_unipi_mqtt/unipi_mqtt.py
tags: [contract, legacy, mqtt, home-assistant]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
sources:
  - id: old-script
    resource: file:///home/unipi/scripts/old_unipi_mqtt/unipi_mqtt.py
    title: old script, sha256 63f50901…c15c2
  - id: old-config
    resource: file:///home/unipi/scripts/old_unipi_mqtt/unipi_mqtt_config.json
    title: old per-circuit config (copy), sha256 1619c66d…f51d1
  - id: mqtt-retained
    resource: mqtt://<BROKER_IP>:1883/
    title: retained messages observed 2026-10-07
---

> Status: reconstructed from code + retained messages. **Incomplete until T20** captures the
> HA YAML and live (non-retained) command payloads. Line numbers refer to sha256 `63f50901…`.

# 1. Commands: HA → old script

Subscription: `unipi1/#`; a message is handled only if the topic **contains** `set`
(`on_mqtt_message`, L122). Payload must be JSON with at least `dev`, `circuit`, `state`.
Keys are kept in received order (`OrderedDict`) because the ack echoes the payload.

| Keys present (priority order, L175–210) | Behaviour | Ack (topic = command topic minus `/set`, retain=True, qos 0) |
|---|---|---|
| `transition` + `brightness` | Fade AO `circuit` from current to `brightness/25.5` V in 100 steps over `transition` s (thread, interruptible). Brightness 0–255, capped at 255. | During fade: payload echoed with live `brightness`; at end echoed with final/actual `brightness`. |
| `brightness` | Set AO to `brightness/25.5` V immediately. | Echo payload. |
| `effect` | Not implemented (logs error). | none |
| `duration` | Set `dev` (`relay`/`output`, or AO `off`) to `state` for `duration` s, then the inverse (thread, interruptible, 1 s resolution). | Echo payload at start; at end echo with `state` inverted. |
| `repeat` | **Doorbell**: N × (ON 0.10 s, OFF 0.25 s) on `dev/circuit` (thread). Stop signal ⇒ extra OFF (bell coil protection). | First ack after first pulse (payload echoed, e.g. `state:"pulse"`); final ack with `repeat` removed and `state:"off"`. |
| `state` `on`/`off` | Relay/output on/off; AO only `off`. | Echo payload. |

A new command for the same `dev+circuit` stops the running thread first (`StopThread`).

## Observed command topics (retained copies on the broker)

| Topic | Payload (example) | Hardware |
|---|---|---|
| `unipi1/bgg/hal/bel/set` (inferred) → state `unipi1/bgg/hal/bel` | `{"circuit":"2_02","dev":"relay","state":"pulse","repeat":"2"}` → final `{"circuit":"2_02","dev":"relay","state":"off"}` | **Doorbell relay 2_02** |
| `unipi1/bijkeuken/dakraam/set` | `{"state":"on","circuit":"1_01","dev":"output","duration":35}` | Roof-window motor, relay 1_01 |
| `unipi1/bgg/bijkeuken/licht/set` | `{"state":"off","circuit":"2_03","dev":"analogoutput"}` | AO 2_03 dimmer |
| `unipi1/bgg/woonkamer/nis/licht/set` | `{"state":"on","circuit":"2_04","dev":"analogoutput","brightness":128}` | AO 2_04 |
| `unipi1/bgg/woonkamer/erker/licht/set` | `{"state":"off","circuit":"3_01","dev":"analogoutput"}` | AO 3_01 |
| `unipi1/buiten/achterdeur/licht/set` | AO 2_01, `transition` 4 seen in state | AO 2_01 |
| `unipi1/buiten/voordeur/licht/set` | AO 2_02 | AO 2_02 |
| `unipi1/eerste/ventilatie/…` (`percentage`, `percentage/state`, `state`) | `{"state":"on","circuit":"3_02"/"3_03","dev":"analogoutput","brightness":128..174,"transition":0}` | Ventilation AO 3_02 / 3_03 — **topic layout inconsistent, clarify in T20** |
| `unipi1/warmtepomp/smartgrid/relay3_1/set`, `relay3_2/set` | `{"circuit":"3_01"/"3_02","dev":"relay","state":"off"}` | **Heat-pump SG-ready relays** |

⚠ Open question (T20): does HA send ring **timing** (e.g. a latency key) to the bel today, or
only `repeat`? The old script only reads `repeat`; timing is hard-coded (0.10 s / 0.25 s).

# 2. States: old script → HA

All per-input `state_topic`s come from `unipi_mqtt_config.json`.

| Source | Payload | Retain | Logic in old script |
|---|---|---|---|
| Digital input, no delay | `ON` / `OFF` | yes | `device_normal: "no"`: 1⇒ON, 0⇒OFF. `"nc"`: inverted. |
| Digital input with `device_delay` (PIRs) | `ON` then `OFF` | yes | ON on first active edge; each new pulse inside the window restarts the timer; OFF after `device_delay` s without pulses (`off_commands`, runs every 1 s). |
| Input with `handle_local` | see §3 | | |
| Counter input (`device_type: counter`, water meter `UART_4_4_02`) | `{"counter_delta": d, "counter": c}` | no | Every `device_delay` (10) s, if counter increased: publish absolute evok counter + delta since last publish. |
| AI (`dev: ai`, lux) | `{"lux": int(mean(volts)*200)}` | no | mean over `interval`+1 samples (≈60 s). |
| `temp` (DS18B20 / DS2438) | `{"temperature": round(mean,1)}` | no | mean over `interval`+1 samples; reject outside −55…125 °C. |
| `humidity` (DS2438) | `{"humidity": round(mean,1)}` | no | 0…100 % valid. |
| `light` (DS2438 vis) | `{"lux": round(mean(vis)*8000)}` | no | 0…0.25 V valid; negatives → 0. (Not in current config.) |
| Availability | `<state_topic>/available` = `online` (qos 2, retained) on connect; `offline` on clean disconnect | yes | per entity |

# 3. Local handling (works without HA/MQTT)

| `handle_local.type` | Trigger | Action |
|---|---|---|
| `bel` | Input active edge (`2_02` back door, `2_05` front door) | Ring relay `output_circuit` (`2_02`) `rings` times (2 / 3): ON 0.1 s, OFF 0.3 s. Publish `ON` then `OFF` to the **button's** state topic. |
| `dimmer` | Input active edge (momentary push-buttons) | Toggle AO `output_circuit`: if 0 → `level` V (10 or 5), else → 0. Publishes `{"state":…,"circuit":…,"dev":"analogoutput"[,"brightness":level*25.5]}` **retained to `<state_topic>/set`**, which the old script then consumes itself to produce the ack. |
| `switch` | Input active edge | Toggle relay/output. (Not in current config.) |

Mapping in the current config: `3_02` and `UART_4_4_05` → AO `2_03` (bijkeuken, 10 V);
`3_05` → AO `2_01` (achterdeur, 10 V); `UART_4_4_03` → AO `2_04` (nis, 5 V);
`UART_4_4_04` → AO `3_01` (erker, 5 V); `UART_4_4_06` → AO `2_02` (voordeur, 10 V);
`2_02`→bel 2×, `2_05`→bel 3×.

# 4. Known quirks that HA may rely on

- Ack topics are retained, qos 0, and echo the full command payload (with HA's key order).
- Brightness scale is 0–255 everywhere on the legacy side (new core uses 0–1000).
- Dimmer local toggles publish retained messages on `/set` topics (self-consumed loop). The
  adapter must **not** copy this; it publishes the equivalent ack on the state topic instead.
- `ring_bel` posts every ON/OFF twice (bug, harmless). Do not reproduce.
- Retained junk exists (typos like `serre/vllugel_hoek`, case duplicates `lekkage-Keukenkasten`)
  → cleaned in T41.
