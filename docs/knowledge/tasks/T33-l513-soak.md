---
type: Task
id: T33
title: "Soak the L513 for 7 days and decide keep or roll back"
description: "After 7 days of daily checks the cutover is formally accepted (or rolled back) and the old card is kept as cold rollback."
phase: 3
task_status: todo
depends_on: [T32]
risk: medium
human_gate: true
target_hosts: [l513]
tags: [l513, soak]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Catch slow problems (evok counter resets, 1-wire dropouts, memory growth, reconnect issues).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)

# Preconditions
- T32 done.

# Files in scope
none (observation) + `/log.md`.

# Backup
Daily `tools/backup.sh daily-soak` on L513 (cron or by hand), copied to S103.

# Steps
1. Daily: `systemctl status` (restarts?), `grep -c ERROR` in the log, RSS of the process,
   HA history of 3 PIRs + water meter + 2 temperatures (gaps?), doorbell rang when visitors came?
2. Rollback criteria (any ⇒ rollback + task): missed doorbell; light not switchable locally;
   roof window not stopping; > 3 unexplained restarts/day; data gaps > 10 min.
3. Day 7: **🔒 HUMAN** accept → record decision.

# Acceptance checks
- 7 daily entries in Evidence; acceptance recorded.

# Rollback
Card swap.

# Feed the elephant
`/log.md`.

# Evidence

# Open questions
