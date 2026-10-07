---
type: Decision
title: "ADR-005 What belongs in the bridge core vs. in Home Assistant"
description: "The bridge owns anything that must work without HA or must be safe/timing-critical; HA owns house-wide logic, presentation and statistics."
tags: [adr, architecture, home-assistant]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Decision

| Put it in the **bridge core** when… | Put it in **Home Assistant** when… |
|---|---|
| it must work with HA or MQTT down (wall switches, doorbell button, PIR hold for local rules) | it needs data from outside this Unipi (presence, time, alarm, other devices) |
| it is timing- or safety-critical (pulse timing, coil/motor limits, fail-safe) | it is presentation (dashboards, fan entity templates, notifications) |
| it is a property of the hardware (NC contact, sensor scaling, valid range, counter) | it is statistics/history (consumption, utility_meter, long-term stats) |
| it is generic and reusable by any Unipi user via config | it is a personal preference that changes often (ring pattern per time of day) |

Everything in the core is **opt-in per circuit**; defaults keep the S103 behaviour unchanged.
