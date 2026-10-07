---
type: Decision
title: "ADR-001 Upgrade the L513 to Unipi OS / evok 3 instead of supporting evok v2"
description: "The L513 is reflashed to evok 3 on a new storage medium so one bridge codebase targets evok 3 only; the old medium is kept untouched as the rollback."
tags: [adr, evok, l513]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Context
The L513 runs evok v2. The new bridge assumes evok 3 data (`device_info`, `modes` as dict,
`di/ro/do` names). Options were: evok-v2 adapter in code, or upgrade the box.

# Decision (by Matthijs, 2026-10-07)
Upgrade the L513 to the current Unipi OS + evok 3 (same family as the S103).

# Consequences
- Bridge code stays evok-3 only (simpler, one test matrix). G21 dropped.
- Device and circuit names change ⇒ a verified old→new circuit map (T31) is needed by the
  legacy adapter and by HA.
- Extension xS30 (UART Modbus) and the 1-wire bus must be configured in evok 3.
- **Rollback = put the old µSD card back** (or restore the full image if the L513 boots
  from eMMC). The upgrade is done on a *new* card whenever possible; the old card is labelled
  and stored, never overwritten, until T42 is done.
- The old `unipi_mqtt.py` cannot run on evok 3 (dev names), so after cutover the only way
  back to the old behaviour is the old card. This is why T32 has a hard go/no-go and why the
  legacy adapter exists.
- Requires a maintenance window (doorbell + wall switches down during the swap).
