---
type: Decision
title: "ADR-003 One sequencer for pulse trains and timed outputs, with hard safety limits"
description: "The doorbell \"repeat\" and the roof-window \"duration\" become one generic output-sequence engine in the core, commanded by a single MQTT JSON message, with per-circuit limits, cancellation-to-OFF, no late execution and fail-safe OFF."
tags: [adr, doorbell, safety, mqtt]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Context
Old: `{"repeat": n}` ⇒ n rings with hard-coded 100/250 ms (HA path) or 100/300 ms (local
path); `{"duration": s}` ⇒ on for s seconds. Implemented as threads, each REST call made twice,
no limits. Matthijs wants count **and** timing in one MQTT message.

# Decision
- Payload on the circuit's normal command topic:
  `{"pulse": {"count": n, "on_ms": a, "off_ms": b}}`, `{"state": "ON", "duration_s": s}`,
  `{"preset": "<name>"}`. Full normative rules in `/context/interface-core.md` §2.
- Units in the name (`_ms`, `_s`) to avoid the seconds/ms confusion found in K4.
- Reject out-of-limit commands (no clamping) — a surprising clamp on a motor or coil is worse
  than a visible error.
- Cancellation always passes through OFF.
- No queuing: if evok WS is down the command is rejected; OFF is re-sent on reconnect.
- Per-circuit `failsafe_off`, `max_on_s` watchdog, `max_count`, `max_pulse_ms`.
- Presets in config become HA **button** entities via discovery (one tap = "ring 3×").
- Same engine serves MQTT commands, local rules (`action_type: pulse`) and the legacy adapter.

# Alternatives rejected
- Flat keys `{"repeat":3,"latency":250}`: ambiguous units, collides with legacy semantics.
- HA-side loops (script toggling ON/OFF): timing jitter over MQTT/Wi-Fi, fails when HA is down,
  and a lost OFF leaves the coil energised.
