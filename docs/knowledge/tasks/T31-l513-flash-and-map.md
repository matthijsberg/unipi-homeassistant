---
type: Task
id: T31
title: "Flash the L513 to evok 3 on a new card and build the verified circuit map"
description: "The L513 runs Unipi OS + evok 3 from a new card with all boards, the xS30 extension and 1-wire sensors visible, and every old circuit has a physically verified new name."
phase: 3
task_status: todo
depends_on: [T30]
risk: high
human_gate: true
target_hosts: [l513]
tags: [l513, evok3, upgrade, maintenance-window]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Execute ADR-001 during the window agreed in T30. Output: `/context/l513-circuit-map.md`.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/l513-evok3-upgrade.md](/runbooks/l513-evok3-upgrade.md) (from T30, human-verified)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md) — L513 section

# Preconditions
- T30 GO recorded for *now*. Image backup checksum verified today.

# Files in scope
L513 system (new card only), `/context/l513-circuit-map.md` (new),
`tests/fixtures/l513_v3_rest_all.json`, `tools/l513_circuit_map.json` (gitignored copy in
runtime dir; example in repo).

# Backup
Done in T30; **old card is removed and labelled "L513 evok2 — DO NOT WRITE"**.

# Steps
1. **🔒 HUMAN** physical steps of the runbook (power down, swap card, power up).
2. Configure per runbook; `GET /rest/all` → save fixture; compare counts with
   `/context/hardware-inventory.md` (40 inputs, 14 relays, 9 ao, 9 ai, 8 temp + 1 DS2438,
   8 led). Any missing device ⇒ fix config or **abort → rollback** (time-box: 30 min).
3. Circuit map by **physical verification** (the only trustworthy method):
   inputs — **🔒 HUMAN** presses each button/triggers each PIR/opens each contact while the
   goldfish watches the evok WS log and records `old → new`; outputs — goldfish switches
   each relay/AO *that is safe to switch* (not smart-grid relays without human OK; roof window
   only with human watching) and the human confirms what happened; 1-wire by address (stable).
4. Write `/context/l513-circuit-map.md` (table old dev/circuit → new dev/circuit, how
   verified, by whom) and `tools/l513_circuit_map.json`.
5. **Do not** start the bridge yet (T32). The house now has no doorbell/wall-switch logic —
   if T32 cannot start within the window: **rollback** (card swap).

# Acceptance checks
- Device counts match; every old circuit in the (live) old config has a verified new name.
- **🔒 HUMAN** marks `/context/l513-circuit-map.md` verified.

# Rollback
Card swap per runbook (≤ 10 min). Log reason.

# Feed the elephant
Map concept; inventory (OS, evok version, hostname); `/log.md` with timings.

# Evidence

# Open questions
