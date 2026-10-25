---
type: Concept
title: "Home Assistant side of the legacy L513 / unipi2 contract (as exported by Matthijs, 2026-10-25)"
description: "Every HA MQTT entity that reads or commands the legacy Unipi topics: topics, payload shapes, templates and the defaults HA fills in. Generated from the YAML export; the adapter (T21) must keep all of it working unchanged."
tags: [legacy, contract, home-assistant, mqtt]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-25T17:00:00Z }
sources:
  - id: ha-export
    resource: file:///home/unipi/.claude/uploads/Binnensensorconfiguratie.txt
    title: "HA MQTT YAML (binary_sensor 28, sensor 201, light 10, switch 7, fan 2, climate 2) - not stored in the repository"
---

Scope: only entities whose topics start with `unipi/`, `unipi1/` or `unipi2/`. The export also holds other MQTT systems (`mbmd` 107, `ecowitt` 58, `P1P2` 15, `beerfridge` 8, `ispindel` 7, `buttonplus` 1 topics): not ours, ignore. **Not in the export (still needed): automations, scripts, scenes, dashboards that use these entities.**

# Findings that decide the design
1. **HA does no PIR holding**: none of the 11 motion `binary_sensor`s has `off_delay` or `expire_after`; HA mirrors `ON`/`OFF` from the topic. The hold times (`device_delay` 20-120 s in the old config) are applied by the legacy script. => After cutover somebody must still apply them (adapter, or HA `off_delay`).
2. **Sensor payloads are JSON objects**: lux `{"lux": n}`, temperature `{"temperature": n}`, humidity `{"humidity": n}`, water meter `{"counter_delta": n, "counter": n}`; HA derives litres (`counter_delta * 0.01`) and m3 (`counter * 0.00001`, `total_increasing`). Inputs are plain `ON`/`OFF` with a retained `<topic>/available` = `online`.
3. **Doorbell (HA "Bel" switch)**: command topic `unipi1/bgg/hal/bel/set`, on-payload `{"circuit": "2_02", "dev": "relay", "repeat": "2", "state": "pulse"}`, off-payload `{"circuit": "2_02", "dev": "relay", "state": "off"}`; state topic `unipi1/bgg/hal/bel`. **HA sends only a ring count (`repeat`, a string), never timing** - closes the open question in `interface-legacy.md`. Timing is the script's own.
4. **Roof window ("Bijkeuken Dakraam")** = two timed outputs on one switch: on-payload `{"state": "on", "circuit": "1_02", "dev": "output", "duration": 25}`, off-payload `{"state": "on", "circuit": "1_01", "dev": "output", "duration": 35}` (the "off" command is a 35 s pulse on the OTHER relay), command topic `unipi1/bijkeuken/dakraam/set`, state comes from the contact input `unipi/bbg/bijkeuken/dakraam/contact`.
5. **Lights are `schema: template` JSON lights** with brightness 0-255 and `transition` in **seconds**; HA fills defaults when a service call gives none (table below). The state topic carries `{"state", "brightness", "transition"?}`.
6. **`unipi2` is a full second legacy device**: lights/relays (`UART_2_2_01..03`, `1_01`, relays 1_01-1_04 for irrigation, veranda relays), plus PIR (`unipi2/veranda/motion`), lux, outside temperature/humidity.
7. Entities that already use the NEW bridge: fan "Ventilatie Woonkamer" -> `unipi/Neuron_S103_2258/ao/xS51_04/...` (range 1-1000, preset modes). The xS51 extension therefore matters for something in daily use.
8. Dead/leftover entries: light "Test Relay 2_02" (`unipi1/bgg/meterkast/testrelay`), switch "Achter Buitenlicht Switch" (command and state topic are the same topic, prefix `unipi1/` while the input publishes on `unipi/`), window/door contacts `serre/vleugel_*` and `woonkamer/vleugel_*` which no config of ours produces (retained `OFF` on the broker): **ask who produces them**.

# Lights (template schema)
| HA entity | state topic | command topic | circuit | on: default brightness / transition s | off: transition s |
|---|---|---|---|---|---|
| Achterdeur light | `unipi1/buiten/achterdeur/licht` | `unipi1/buiten/achterdeur/licht/set` | analogoutput 2_01 | 199 / 2 | 4 |
| Voordeur light | `unipi1/buiten/voordeur/licht` | `unipi1/buiten/voordeur/licht/set` | analogoutput 2_02 | 200 / 2 | 4 |
| Bijkeuken light | `unipi1/bgg/bijkeuken/licht` | `unipi1/bgg/bijkeuken/licht/set` | analogoutput 2_03 | 199 / 4 | 4 |
| Woonkamer Nis light | `unipi1/bgg/woonkamer/nis/licht` | `unipi1/bgg/woonkamer/nis/licht/set` | analogoutput 2_04 | 199 / 4 | 4 |
| Woonkamer Erker light | `unipi1/bgg/woonkamer/erker/licht` | `unipi1/bgg/woonkamer/erker/licht/set` | analogoutput 3_01 | 199 / 4 | 4 |
| Test Relay 2_02 | `unipi1/bgg/meterkast/testrelay` | `unipi1/bgg/meterkast/testrelay/set` | output 2_02 | ? / ? | ? |
| Buiten trap light | `unipi2/buiten/achter_trap/licht` | `unipi2/buiten/achter_trap/licht/set` | analogoutput UART_2_2_01 | 199 / 4 | 4 |
| Veranda loop light | `unipi2/veranda/loop_licht` | `unipi2/veranda/loop_licht/set` | analogoutput UART_2_2_03 | 199 / 1 | 5 |
| Veranda light | `unipi2/veranda/licht` | `unipi2/veranda/licht/set` | analogoutput UART_2_2_02 | 199 / 4 | 4 |
| Serre Gordijnen Licht | `unipi2/serre/gordijnen/licht` | `unipi2/serre/gordijnen/licht/set` | analogoutput 1_01 | 199 / 2 | 4 |

# Switches
| HA entity | state topic | command topic | on payload | off payload |
|---|---|---|---|---|
| Bijkeuken Dakraam | `unipi/bbg/bijkeuken/dakraam/contact` | `unipi1/bijkeuken/dakraam/set` | `{"state": "on", "circuit": "1_02", "dev": "output", "duration": 25}` | `{"state": "on", "circuit": "1_01", "dev": "output", "duration": 35}` |
| Bel | `unipi1/bgg/hal/bel` | `unipi1/bgg/hal/bel/set` | `{"circuit": "2_02", "dev": "relay", "repeat": "2", "state": "pulse"}` | `{"circuit": "2_02", "dev": "relay", "state": "off"}` |
| Achter Buitenlicht Switch | `unipi1/bgg/achter/buitenlichtschakelaar` | `unipi1/bgg/achter/buitenlichtschakelaar` | `ON` | `OFF` |
| WCD Kast Vliering - relay1 | `unipi2/veranda/relay1` | `unipi2/veranda/relay1/set` | `{"circuit": "UART_2_2_01", "dev": "relay", "state": "on"}` | `{"circuit": "UART_2_2_01", "dev": "relay", "state": "off"}` |
| WCD Paal Buiten - relay2 | `unipi2/veranda/relay2` | `unipi2/veranda/relay2/set` | `{"circuit": "UART_2_2_02", "dev": "relay", "state": "on"}` | `{"circuit": "UART_2_2_02", "dev": "relay", "state": "off"}` |
| Warmtepomp Smart Grid 1 - S10S | `unipi1/warmtepomp/smartgrid/relay3_1` | `unipi1/warmtepomp/smartgrid/relay3_1/set` | `{"circuit": "3_01", "dev": "relay", "state": "on"}` | `{"circuit": "3_01", "dev": "relay", "state": "off"}` |
| Warmtepomp Smart Grid 2 - S11S | `unipi1/warmtepomp/smartgrid/relay3_2` | `unipi1/warmtepomp/smartgrid/relay3_2/set` | `{"circuit": "3_02", "dev": "relay", "state": "on"}` | `{"circuit": "3_02", "dev": "relay", "state": "off"}` |

# Fans
- {"name": "Ventilatie Woonkamer", "command_topic": "unipi/Neuron_S103_2258/ao/xS51_04/set", "state_topic": "unipi/Neuron_S103_2258/ao/xS51_04/state", "percentage_state_topic": "unipi/Neuron_S103_2258/ao/xS51_04/output", "percentage_command_topic": "unipi/Neuron_S103_2258/ao/xS51_04/output/set", "speed_range_min": 1, "speed_range_max": 1000, "preset_modes": ["auto", "low", "medium", "high"], "preset_mode_command_topic": "unipi/Neuron_S103_2258/ao/xS51_04/output/set", "preset_mode_state_topic": "unipi/Neuron_S103_2258/ao/xS51_04/output"}
- {"name": "Ventilatie Boven", "command_topic": "unipi1/eerste/ventilatie/state/set", "state_topic": "unipi1/eerste/ventilatie/state", "percentage_state_topic": "unipi1/eerste/ventilatie/percentage/state", "percentage_command_topic": "unipi1/eerste/ventilatie/percentage/state/set", "speed_range_min": 1, "speed_range_max": 255, "preset_modes": ["off", "low", "medium", "high"], "preset_mode_command_topic": "unipi1/eerste/ventilatie/preset/set"}

# Binary sensors (state `ON`/`OFF`, availability `<topic>/available` = `online`)
| HA entity | topic | device_class |
|---|---|---|
| Achterdeur Contact | `unipi/bgg/achterdeur/contact` | door |
| Achterdeur Motion | `unipi/bgg/achterdeur/motion` | motion |
| Achterdeur Bel | `unipi/bgg/achterdeur/beldrukker` |  |
| Voordeur Contact | `unipi/bgg/voordeur/contact` | door |
| Voordeur Slot | `unipi/bgg/voordeur/deurslot` | lock |
| Achterdeur Slot | `unipi/bgg/achterdeur/deurslot` | lock |
| Lekkage Ketel Ruimte | `unipi/bgg/bijkeuken/lekkage_ketel_ruimte` | moisture |
| Lekkage Keukenkasten | `unipi/bgg/bijkeuken/lekkage-keukenkasten` | moisture |
| Lekkage Wasbak Wasmachine | `unipi/bgg/bijkeuken/lekkage-wasbak_machine` | moisture |
| Voordeur Motion | `unipi/bgg/voordeur/motion` | motion |
| Voordeur Bel | `unipi/bgg/voordeur/beldrukker` |  |
| Woonkamer Motion | `unipi/bgg/woonkamer/motion` | presence |
| Serre Motion | `unipi/bgg/serre/motion` | presence |
| Veranda Motion | `unipi2/veranda/motion` | presence |
| Hal Motion | `unipi/bgg/hal/motion` | presence |
| Bijkeuken Motion | `unipi/bgg/bijkeuken/motion` | presence |
| Kantoor Motion | `unipi/bgg/kantoor/motion` | presence |
| Overloop Motion | `unipi/eerste/overloop/motion` | presence |
| Bijkeuken Dakraam Contact | `unipi/bbg/bijkeuken/dakraam/contact` | window |
| Tussendeur Contact | `unipi/bbg/bijkeuken/tussendeur/contact` | door |
| Kantoor Raam Contact | `unipi/bbg/kantoor/raam/contact` | window |
| Serre Schuifpuivleugel-Huis Contact | `unipi/bgg/serre/vleugel_huis/contact` | door |
| Serre Schuifpuivleugel-Tuin Contact | `unipi/bgg/serre/vleugel_tuin/contact` | door |
| Serre Schuifpuivleugel-Hoek Contact | `unipi/bgg/serre/vleugel_hoek/contact` | door |
| Woonkamer Schuifpuivleugel Serre Contact | `unipi/bgg/woonkamer/vleugel_serre/contact` | door |
| Woonkamer Schuifpuivleugel Tuin Contact | `unipi/bgg/woonkamer/vleugel_tuin/contact` | door |

# Sensors
| HA entity | topic | value template | unit |
|---|---|---|---|
| Hal Lux | `unipi/bgg/hal/lux` | `{{ value_json.lux }}` | lux |
| Woonkamer Lux | `unipi/bgg/woonkamer/lux` | `{{ value_json.lux }}` | lux |
| Serre Lux | `unipi/bgg/serre/lux` | `{{ value_json.lux }}` | lux |
| Kantoor Lux | `unipi/bgg/kantoor/lux` | `{{ value_json.lux }}` | lux |
| Achterdeur Lux | `unipi/bgg/achterdeur/lux` | `{{ value_json.lux }}` | lux |
| Bijkeuken Lux | `unipi/bgg/bijkeuken/lux` | `{{ value_json.lux }}` | lux |
| Veranda Lux | `unipi2/veranda/lux` | `{{ value_json.lux }}` | lux |
| Buiten Temperatuur | `unipi/buiten/voordeur/temperatuur` | `{{value_json.temperature}}` | °C |
| Buiten Temperatuur Achter | `unipi2/buiten/achter/temperatuur` | `{{value_json.temperature}}` | °C |
| Buiten Luchtvochtigheid Achter | `unipi2/buiten/achter/luchtvochtigheid` | `{{ value_json.humidity }}` | % |
| Bijkeuken Temperatuur | `unipi/bgg/bijkeuken/temperatuur` | `{{value_json.temperature}}` | °C |
| Bijkeuken Luchtvochtigheid | `unipi/bgg/bijkeuken/luchtvochtigheid` | `{{ value_json.humidity }}` | % |
| Warmtepomp Boier Temperatuur | `unipi/bgg/warmtepomp/boiler/temperatuur` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 1 | `unipi/bgg/warmtepomp/sensor1` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 2 | `unipi/bgg/warmtepomp/sensor2` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 3 | `unipi/bgg/warmtepomp/sensor3` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 4 | `unipi/bgg/warmtepomp/sensor4` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 5 | `unipi/bgg/warmtepomp/sensor5` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 6 | `unipi/bgg/warmtepomp/sensor6` | `{{value_json.temperature}}` | °C |
| Warmtepomp Vloerverwarming Sensor 7 | `unipi/bgg/warmtepomp/sensor7` | `{{value_json.temperature}}` | °C |
| Water Ticks | `unipi/huis/watermeter` | `{{ value_json.counter_delta }}` | tick |
| Water Consumption Actual | `unipi/huis/watermeter` | `{{ value_json.counter_delta \| float * 0.01 \| round(2) }}` | L |
| Water Consumption Total | `unipi/huis/watermeter` | `{{ ((value_json.counter \| float) * 0.000010) \| round(3) }}` | m³ |
