---
type: Hardware Inventory
title: "Unipi controllers, software and network"
description: "The two Unipi PLCs, what runs on each, and how they reach MQTT and Home Assistant (as observed 2026-10-07)."
resource: urn:house:unipi
tags: [inventory, hardware, network]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
sources:
  - id: s103-rest
    resource: http://<S103_IP>:8080/rest/all
    title: evok REST snapshot on S103
    last_modified: 2026-10-07T09:15:00Z
  - id: l513-rest
    resource: http://<L513_IP>:8080/rest/all
    title: evok REST snapshot on L513 (read-only GET)
    last_modified: 2026-10-07T09:18:00Z
  - id: mqtt-retained
    resource: mqtt://<BROKER_IP>:1883/#
    title: 8-second read-only subscription to unipi1/# and unipi/<area>/#
    last_modified: 2026-10-07T09:19:00Z
---

# Overview

| | **S103** ("new") | **L513** ("old") |
|---|---|---|
| IP | <S103_IP> | <L513_IP> |
| Hostname | `S103-sn2258` | not needed (no login to the L513 is planned; read-only REST/MQTT only) |
| Hardware | Unipi Neuron S103 (SN 2258) on Raspberry Pi 3B+, + extension **xS51** (Modbus) | Unipi Neuron **L513** (SN 10, 3 boards) + extension **xS30** on UART (`UART_4_4`) |
| OS / evok | Debian bookworm, **evok 3.0.6.1**, evok-unipi-data 1.1.4 | **evok v2** (inferred from data shape: `input`/`relay` dev names, `neuron` dev, no `device_info`) |
| Bridge | `hass-unipi.py` v`2026092501` (systemd `hass-unipi.service`, venv `/home/unipi/unipi-homeassistant`) | Old `unipi_mqtt.py` "02.2021.1" (copy in `/home/unipi/scripts/old_unipi_mqtt/` on S103) |
| Evok WS / REST used | `ws://<S103_IP>:8080/ws`, `http://<S103_IP>:8080/rest/all` | old script: `ws://<L513_IP>/ws` + REST `http://<L513_IP>:8080/rest/` |
| MQTT topic root | `unipi/<family>_<model>_<sn>/…` + HA discovery | `unipi/…` (state) and `unipi1/…` (commands), **no discovery** – HA YAML |
| Local web UI | `:8088` (rule editor, PAM login) | none |

MQTT broker / Home Assistant: `<BROKER_IP>:1883`.
S103 bridge uses MQTT user `<MQTT_USER_S103>` (re-used Shelly credentials — see recommendation
in [/analysis/known-issues.md](/analysis/known-issues.md)); old script uses user `unipi1`.

# L513 evok-v2 circuits (from `l513-rest`)

| dev | count | circuits |
|---|---|---|
| input | 40 | `1_01…`, `2_01…`, `3_01…`, `UART_4_4_01…16` |
| relay | 14 | `1_01–1_04`, `2_01–2_05`, `3_01–3_05` |
| ao | 9 | `1_01`, `2_01–2_04`, `3_01–3_04` (Voltage) |
| ai | 9 | |
| temp | 8 | DS18B20 1-wire sensors (see `/context/interface-legacy.md`) |
| 1wdevice | 1 | `26729616020000C2` DS2438 (temp/humidity/vis) |
| led | 8 | `1_01–1_04`, `UART_4_4_01–04` |
| other | | `neuron` (model L513, sn 10), `extension` (xS30 on `/dev/extcomm/0/0`), `uart` ×4, `wd` ×4, `owbus` |

> ⚠ After the evok-3 upgrade (ADR-001) **dev names and probably circuit names change**
> (e.g. `input`→`di`, `relay`→`ro`, `UART_4_4_xx`→ an `xS30_xx`-style name). The definitive
> old→new map is produced in **T31** and stored in `/context/l513-circuit-map.md`.

# S103 evok-3 circuits (from `s103-rest`)

`di` ×8, `do` ×4, `ro` ×5 (xS51), `ai` ×5, `ao` ×5, `led` ×7, `1wdevice` ×2 (DS2438),
`modbus_slave` ×2, `device_info` ×2 (Neuron S103 sn 2258; Extension xS51).
The 7 front-panel **`led`** circuits are the safe test outputs for pulse/sequence work.

# L513 facts stated by Matthijs (2026-10-07)

- The old script runs as a **root systemd service started at boot**.
- The script and config copies in `/home/unipi/scripts/old_unipi_mqtt/` are the latest.
- The L513 rollback is a **swap SD card** (old card kept untouched) — the L513 itself is
  never backed up over the network. To be physically confirmed before T30.

# Unipi BaseOS (planned for the new L513 card; see ADR-006)

From search results/Evok docs, to be re-verified against the downloaded image (T06/T30): Debian 12
bookworm arm64; Evok **not** included (install from `repo.unipi.technology`); first boot needs DHCP;
SSH on; default user `unipi` with a publicly documented password (change it); SD ≥ 2 GB.
Reference (S103, working): Unipi apt repo + `unipi-os-configurator`, kernel/firmware packages,
`evok` 3.0.6.1, `/etc/evok/config.yaml` for the xS51 on RS485. Exact image filename/sha256: _TBD (T06)_.

# Unknowns (resolve in the named task)

- Exact Unipi OS / evok version on L513 → **T30/T31** (no login before the upgrade); storage medium = removable µSD per Matthijs, confirm physically → **T30**.
- Whether the 11 retained topics not in the config (e.g. `serre/vleugel_tuin`, `bijkeuken/lekkage-koelkast`) are stale — T01 says very likely yes; confirm in **T20**.
- What the HA YAML for the old entities looks like → **T20**.
