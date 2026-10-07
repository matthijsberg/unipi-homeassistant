---
type: Interface Contract
title: "Core bridge MQTT contract (hass-unipi.py) — current + planned"
description: "Topics, payloads and per-circuit config of the generic bridge, including the new pulse/duration command that replaces the legacy doorbell \"repeat\"."
resource: file:///home/unipi/unipi-homeassistant/scripts/hass-unipi.py
tags: [contract, mqtt, home-assistant, core]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

Notation: `<root>` = `config.mqtt.topic` (default `unipi`), `<dn>` = device name
`<family>_<model>_<sn>` (e.g. `Neuron_S103_2258`), `<dp>` = discovery prefix (`homeassistant`).
Items marked **[NEW Txx]** do not exist yet; the task builds them. Everything else exists in
v2.0.0 (= live `2026092501`).

# 1. Topics (existing)

| Purpose | Topic | Payload |
|---|---|---|
| Availability (LWT) | `<root>/<dn>/status` | `online` / `offline` (retained) |
| Digital state (`di`,`do`,`ro`,`led`) | `<root>/<dn>/<dev>/<circuit>/state` | `ON` / `OFF` (retained) |
| Digital command (`do`,`ro`,`led`) | `<root>/<dn>/<dev>/<circuit>/set` | `ON` / `OFF` |
| AO state | `<root>/<dn>/ao/<circuit>/state` | `{"state":"ON","brightness":0-1000,"color_mode":"brightness"}` |
| AO command (HA JSON light) | `<root>/<dn>/ao/<circuit>/set` | `{"state":"ON","brightness":0-1000,"transition":s}` (transition ≤ 60 s) |
| AI / temp | `…/<dev>/<circuit>/state` (temp: `<root>/<dn>/1-wire/<circuit>/temp/state`) | `{"value": x}` |
| 1-wire multi | `<root>/<dn>/1-wire/<circuit>/<temp|humidity|vdd|vad|vis>` | `{"value": x}` |
| Bridge health | `<root>/<dn>_bridge/{startup_error,startup_details,connectivity_problem,connectivity_details}` and `…/extension/<c>/{problem,last_comm}` | |
| HA birth | `<dp>/status` = `online` ⇒ bridge republishes everything | |

Retained `/set` messages are ignored (on first connect and always), so HA's `retain: true`
switch commands never replay physical actions.

# 2. Output sequences — pulse & duration **[NEW T12/T13]**

One command message, on the **existing** command topic of any digital output
(`do`, `ro`, `led`; also `relay`/`output` aliases):

```jsonc
// Doorbell: 3 rings, 100 ms on, 250 ms between rings
{"pulse": {"count": 3, "on_ms": 100, "off_ms": 250}}

// Roof window: on for 35 s, then off
{"state": "ON", "duration_s": 35}

// Use a named preset from the circuit config (see §4)
{"preset": "ring_front"}
```

Rules (normative):

1. `pulse.count` 1…`max_count` (circuit, default 10); `on_ms` ≥ 20 and ≤ `max_pulse_ms`
   (default 2000); `off_ms` 50…10000. Missing `on_ms`/`off_ms` ⇒ circuit defaults
   (`pulse_defaults`, global default 100/250 = legacy HA timing).
2. `duration_s` 0.1…`max_on_s` (circuit, default 3600). `state` may be `ON` or `OFF`
   (OFF+duration = "off for N s, then on again", legacy parity). 
3. **Out-of-limit values are rejected, not clamped**: nothing switches; a warning is logged
   and `<root>/<dn>/<dev>/<circuit>/attributes` gets `{"last_error": "..."}`.
4. **One sequence per circuit.** A new command (`ON`, `OFF`, pulse, duration) for the same
   circuit cancels the running sequence; cancellation always drives the output to **OFF**
   first (bell-coil / motor protection), then executes the new command.
5. **No late execution.** If the evok WebSocket is not OPEN when a sequence starts, it is
   rejected (a doorbell must not ring 30 s later). If it drops mid-sequence, the bridge
   sends OFF for that circuit as soon as the socket is back.
6. **Fail-safe.** Circuits with `failsafe_off: true` are driven OFF after every bridge
   start/WS reconnect, and on shutdown. Any output that has been ON longer than its
   `max_on_s` is driven OFF by a watchdog, whatever turned it on (HA, rule, manual).
7. **Acks.** `state` → `ON` when the sequence starts, `OFF` when it ends or is cancelled.
   `attributes` → `{"busy": true, "remaining": n, "ends_at": iso}` while running,
   `{"busy": false}` after. 
8. Timing is scheduled on the asyncio loop with `time.monotonic()` and written straight to
   the WebSocket (not via the 30 s hold queue). Target jitter < 30 ms on a Pi 3B+.

HA usage (automation/script):

```yaml
action: mqtt.publish
data:
  topic: unipi/Neuron_L513_10/ro/<bell-circuit>/set
  payload: '{"pulse": {"count": 3, "on_ms": 100, "off_ms": 250}}'
```

…or press a discovered **button** entity per preset (T13), e.g. *"Bel – voordeur (3×)"*.

# 3. Inputs — hold, counter, logical inversion **[NEW T14/T15/T11]**

- `inverted: true` ⇒ the bridge publishes the **logical** state (NC contact closed = `OFF`)
  and local rules see the logical value. Discovery uses plain `ON`/`OFF` payloads.
  (Today `inverted` only swaps HA payloads; migration of S103 is part of T11.)
- `off_delay_s: N` (PIRs) ⇒ `ON` at the first active edge; `OFF` only after N s without a new
  active edge (retriggerable). Raw edges are still available to local rules
  (`trigger_source: raw|held`, default `raw`).
- `counter: true` ⇒ extra discovered sensor `…/<dev>/<circuit>/counter` with
  `state_class: total_increasing`, published at most every `counter_interval_s` (default 10)
  when changed. Deltas/consumption are computed in HA (utility_meter).

# 4. Per-circuit configuration **[NEW T11]**

**[IMPLEMENTED T11 — only these options]** New optional top-level `circuits` object in
`config.json`, keyed `"<dev>/<circuit>"` (aliases `input`/`relay`/`output`/`analogoutput` accepted).
Supported options: `name`, `device_class` (not for `led`/`ao`/`1wdevice`), `inverted` (only `di`).
**Every other option below is rejected at start-up with a message naming the circuit and the
task that will add it** (planned options never fail silently). `area` is *not possible*: Home
Assistant MQTT discovery only has a per-device `suggested_area`; assign areas in HA.
`1wdevice/<addr>/<temp|humidity|vdd|vad|vis>` accepts `name`. Entity identity (`unique_id`) never
depends on `name`. The old `inputs.<circuit>.inverted` keeps working. `local_rules.json` is
resolved next to the config file (fix K6).

```jsonc
"circuits": {
  "di/2_05":  {"name": "Voordeur beldrukker", "device_class": null},
  "di/1_01":  {"name": "Hal PIR", "device_class": "motion", "off_delay_s": 20},
  "di/2_03":  {"name": "Achterdeur contact", "device_class": "door", "inverted": true},
  "di/xS30_02": {"name": "Watermeter", "counter": true, "counter_interval_s": 10},
  "ro/2_02":  {"name": "Bel", "failsafe_off": true, "max_on_s": 2,
               "pulse_defaults": {"on_ms": 100, "off_ms": 250}, "max_count": 6,
               "presets": {"ring_back": {"label": "Bel achterdeur (2×)", "pulse": {"count": 2}},
                           "ring_front": {"label": "Bel voordeur (3×)", "pulse": {"count": 3}}}},
  "ro/1_01":  {"name": "Dakraam", "failsafe_off": true, "max_on_s": 60},
  "ai/1_01":  {"name": "Hal lux", "unit": "lx", "device_class": "illuminance",
               "transform": {"scale": 200, "offset": 0, "round": 0},
               "sampling": {"mode": "mean", "publish_interval_s": 60}},
  "1wdevice/28D1EFA708000052/temp": {"name": "Buiten temperatuur",
               "valid_range": [-55, 125], "sampling": {"mode": "mean", "publish_interval_s": 30}},
  "ao/3_02":  {"name": "Ventilatie", "ha_component": "fan"}
}
```

Circuit names are placeholders until the L513 evok-3 map exists (T31).

# 5. Local rules **[EXTENDED T17]**

`local_rules.json` (`LocalLogicRule`) gains `action_type` values:

| action_type | Fields | Behaviour |
|---|---|---|
| `set` (existing) | `action_value`, `action_transition`, `action_delay` | unchanged |
| `dimmer` (existing) | `action_value` = default on-level in V **[NEW]** (was fixed 10 V) | short press toggle / long press dim |
| `toggle` **[NEW]** | – | flip a digital output |
| `pulse` **[NEW]** | `action_pulse: {count,on_ms,off_ms}` or `action_preset` | runs §2 sequence (same engine, same limits) |

# 6. Modes **[NEW T18]**

`"mode": "live" | "shadow"` (default `live`). Shadow = read-only twin for safe testing next
to a live instance: own `<root>` (e.g. `unipi_shadow`), discovery disabled unless
`shadow_discovery: true`, **no WebSocket writes**, no local rules, web UI on another port.

# 7. Legacy adapter switch **[NEW T21]**

```jsonc
"legacy": {"enabled": false, "map_file": "legacy_map.json"}
```

`enabled: false` (default) ⇒ `legacy_adapter.py` is not even imported. See ADR-002.
