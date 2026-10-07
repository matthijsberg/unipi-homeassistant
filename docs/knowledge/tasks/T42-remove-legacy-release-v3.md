---
type: Task
id: T42
title: "Remove the legacy adapter and release v3.0.0"
description: "legacy_adapter.py, its config key, map files and converter are removed; v3.0.0 is released and deployed to both Unipis; the old L513 card may be retired after 30 more days."
phase: 4
task_status: todo
depends_on: [T41]
risk: low
human_gate: true
target_hosts: [s103, l513]
tags: [legacy, release, major]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Finish ADR-002: no dead code. Major version because the legacy topic contract is gone.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-004-versioning-and-backups.md](/decisions/ADR-004-versioning-and-backups.md)

# Preconditions
- T41 done (14 days with adapter off).

# Files in scope
`legacy_adapter.py` (delete), `hass-unipi.py` (remove `legacy` config + import, ≤ 20 lines),
`tools/convert_legacy.py` (move to `legacy/` as reference or delete — human choice),
`legacy_map.example.json`, tests for the adapter (delete), README, CHANGELOG.

# Backup
`tools/backup.sh pre-v3.0.0` on both boxes.

# Steps
1. Remove code + tests; a config that still contains `legacy` must start with a WARNING
   ("ignored, removed in v3") — not crash.
2. CHANGELOG + Release notes: breaking = legacy topics gone.
3. Tag `v3.0.0`; deploy S103 → 24 h → L513.
4. **🔒 HUMAN** after 30 more days: retire the old L513 card (keep its image archive).

# Acceptance checks
- Tests green; both boxes healthy 24 h; `grep -ri legacy hass-unipi.py` only the warning.

# Rollback
`tools/rollback.sh v2.4.x` (re-enable adapter if ever needed).

# Feed the elephant
ADR-002 → `status: deprecated` with note "completed"; tasks index all done; `/log.md`.

# Evidence

# Open questions
