---
type: System
title: "Unipi ⇄ MQTT ⇄ Home Assistant — target system"
description: "What we are building: one generic bridge on both Unipis, a removable legacy adapter, and the split of logic between bridge and Home Assistant."
tags: [architecture, overview]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Goal

Replace the old `unipi_mqtt.py` on the **L513** with the new generic bridge
(`hass-unipi.py`, already live on the **S103**) **without losing any function the house
relies on**, while making the bridge more robust, more generally applicable and better
integrated with Home Assistant (discovery, availability, device classes, buttons).

# Target picture

```
                 Home Assistant (<BROKER_IP>)
          discovered entities          legacy YAML entities (temporary)
                 │  ▲                          │  ▲
     unipi/<dn>/…│  │                 unipi1/… │  │ unipi/<area>/…
                 ▼  │                          ▼  │
        ┌──────────────────────── hass-unipi.py (same code on S103 + L513) ───────┐
        │  MQTT core  ◄──── EventBus / CommandService ────►  legacy_adapter.py    │
        │  discovery        (unipi_core/)                    (only if             │
        │  local rules ──►  sequencer · signals · circuits    legacy.enabled)      │
        └───────────────────────────────┬────────────────────────────────────────┘
                                        │ WebSocket + REST
                                   evok 3 (both boxes after T31)
```

# Principles

1. **Adapter translates, core acts.** All behaviour (timing, safety, debouncing, counters,
   scaling, local rules) lives in the generic core and is configured per circuit. The
   legacy adapter only maps old topics/payloads ↔ core events/commands. Switching it off
   removes old *topics*, never *functions*. (ADR-002)
2. **Safety outputs are first-class.** Doorbell coil and window motor get hard limits,
   watchdog and fail-safe OFF in the core. (ADR-003)
3. **Local first.** Wall switches, doorbell buttons and PIR hold work with HA and/or MQTT
   down. HA does what needs house-wide context. (ADR-005)
4. **Always reversible.** Every step has a backup and a tested rollback; the L513 upgrade
   keeps the old SD card untouched. (ADR-004, ADR-001)
5. **One codebase, both boxes, defaults preserve today.** New options default to current
   S103 behaviour.

# Read next

- Gap analysis: [/analysis/feature-gap-matrix.md](/analysis/feature-gap-matrix.md)
- Contracts: [/context/interface-legacy.md](/context/interface-legacy.md),
  [/context/interface-core.md](/context/interface-core.md)
- Plan: [/tasks/index.md](/tasks/index.md)
