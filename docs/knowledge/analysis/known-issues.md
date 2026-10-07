---
type: Analysis
title: "Known issues and risks found during the 2026-10-07 review"
description: "Defects and risks in the current bridge and environment that the plan must handle or consciously accept."
tags: [analysis, risks, bugs]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

| # | Where | Issue | Impact | Handled in |
|---|---|---|---|---|
| K1 | `on_mqtt_message` | Every JSON payload on any `/set` is routed to `process_ao_transition`; non-AO devs are rejected there. | JSON commands to relays (pulse/duration) impossible. | T12 |
| K2 | `websocket_worker_thread` | Holds a command up to 30 s while WS is down, then drops it silently. | Late rings/window moves; a dropped OFF. | T12 (sequencer bypasses queue; OFF priority) |
| K3 | `publish_discovery_config` | `inputs.inverted` only swapped HA payloads; `device_states` and rules saw the raw value. | Rules on NC contacts were inverted vs HA. | **Fixed in T11** (logical inversion at the input edge; parity test proves HA sees the same state) |
| K4 | `execute_local_action` | Rule `action_transition` is treated as **ms**; MQTT `transition` is **s**. | Confusing; easy misconfiguration. | T17 (document + validate; keep ms for rules, name it `action_transition_ms` alias) |
| K5 | `handle_dimmer_action` | Default on-level fixed 10 V; `previous_level` lost on restart. | Legacy 5 V lights come on at 10 V first time. | T17 |
| K6 | `LocalLogicEngine("local_rules.json")` | Relative path → depends on CWD (systemd sets it; manual runs may not). | Rules silently empty when started elsewhere. | **Fixed in T11** (resolved next to the config file) |
| K7 | `process_payload` | Bare JSON integer becomes `{"brightness": n}` for any device. | Odd for relays. | T12 (route by dev first) |
| K8 | Env | S103 bridge uses MQTT user `<MQTT_USER_S103>`. | Shared credential; can't tell clients apart in broker ACL/logs. | Recommendation: dedicated users `unipi-s103`, `unipi-l513` (T03 note, human) |
| K9 | Env | `gh` token on S103 expired. | Cannot push. | T00 (human) |
| K10 | Repo | GitHub `main` (2025-03-29, 83 KB script) is far behind live (161 KB). Repo is **public**. | History gap; risk of committing secrets/house data. | T02 (secret guard, example config, house config never committed) |
| K11 | Old script | Plain-text MQTT password hard-coded (`mqtt_pass`). Not present in GitHub history (checked `git log -S`). | Must be redacted before the old script is added to the repo as reference. | T02 |
| K12 | Inventory | Retained topics show 11 inputs not in the old config copy. | Copy may not be what runs on L513. | **Likely stale leftovers** (T01 evidence: all 28 configured inputs exist; the 12 unconfigured L513 inputs all read 0; extras are typo/case variants). Confirm in T20 capture; clean up in T41. |
| K13 | Runtime dir | `scripts/` contains ~370 backups (12 MB) + ad-hoc copies, no VCS. | Hard to know what is live. | T02/T03 (repo + tools; runtime dir becomes deploy target only) |
| K15 | `_perform_ao_transition_task` | AO fade steps use a blocking `queue.put` on the event-loop thread (kept in T10 to stay behaviour-neutral). | If the WS queue (1000) is full (WS down) the loop can stall until the worker drains/drops. | T12 (sequencer, direct non-blocking writes) |
| K16 | `process_mqtt_message` verification | Evok's `led` (maybe other) read-back lags the write >1 s, so the 3-try verification (0.3/0.8/1.3 s) often logs a mismatch WARNING and acks ~1.3 s late. Ack content is still correct. | Noisy warnings, slower ack for LEDs. | T12 (sequencer acks from the write, not from REST polling) / revisit verification timing; A/B vs v2.0.0 still to do. |
| K14 | L513 upgrade | evok 3 renames devs/circuits and needs extension (xS30) + 1-wire configured. | Every HA entity and legacy map depends on it. | T30, T31 |
