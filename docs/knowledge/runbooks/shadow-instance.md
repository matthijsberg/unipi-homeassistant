---
type: Runbook
title: "Run a shadow instance next to the live bridge"
description: "Test a new bridge version against real Unipi traffic without any risk: a read-only twin that never writes to evok, never runs rules and never touches the live Home Assistant entities."
tags: [runbook, shadow, testing]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T09:00:00Z }
---

# What a shadow instance is (T18)
`mode: shadow` (config or `UNIPI_MODE=shadow`): same code, but
- **never writes to evok** (the two places that send on the WebSocket and the command queue are guarded; every attempt is logged as `SHADOW: would …`),
- **never runs local rules** (logged only), **never starts the web UI**,
- publishes under its **own names**: device `<device>_shadow` ⇒ topics `unipi/<device>_shadow/…`, its own LWT/status, own `unique_id`s and discovery node ids, so it cannot overwrite the live bridge's retained discovery,
- creates **no Home Assistant entities** unless `shadow_discovery: true` (then a separate device named "… (shadow)").
It does read evok and publish state, so you can compare it with the live bridge.

# Set up (on the box that runs the live bridge)
```bash
mkdir -p ~/shadow/release && cd ~/src/unipi-homeassistant
git archive <tag-to-test> | tar -x -C ~/shadow/release          # the version under test; the live one is untouched
cp config.shadow.example.json ~/shadow/config.json             # then edit broker/credentials like the live config
chmod 600 ~/shadow/config.json
cd ~/shadow && /home/unipi/unipi-homeassistant/bin/python release/hass-unipi.py --config config.json
```
Or install `tools/hass-unipi-shadow.service.example` as `/etc/systemd/system/hass-unipi-shadow.service` (not enabled automatically).
The shadow's rules/state files live next to *its* config, so they never mix with the live ones.

# Check
- Log shows `MODE: SHADOW`; MQTT has `unipi/<device>_shadow/status` = `online`.
- `tools/shadow_compare.py --seconds 60` → states identical (analog values may differ slightly).
- Send a command to a **shadow** topic (`unipi/<device>_shadow/led/1_01/set` = `ON`): the log says `SHADOW: would write`, the real output does not change.

# Stop and clean up
`sudo systemctl stop hass-unipi-shadow` (or Ctrl-C), then `tools/clear_shadow_topics.py` (lists), `… --yes` (removes the retained
`…_shadow` topics; it cannot match live topics).
