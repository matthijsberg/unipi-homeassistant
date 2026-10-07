---
type: Task
id: T23
title: "Validate the legacy path end-to-end before touching the L513"
description: "A dry-run of the full L513 configuration (converter output + adapter) passes contract tests and a 24 h shadow run on the S103 against a test topic root; release v2.3.0."
phase: 2
task_status: todo
depends_on: [T18, T21, T22]
risk: medium
human_gate: true
target_hosts: [dev-only, s103]
tags: [legacy, validation, release]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Find adapter/converter mistakes while the L513 still runs the old system untouched.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/context/interface-legacy.md](/context/interface-legacy.md)
- [/runbooks/deploy.md](/runbooks/deploy.md)

# Preconditions
- T18, T21, T22 merged; tests green.

# Files in scope
`tests/test_legacy_e2e.py`, `tests/fixtures/l513_v3_rest_all.synthetic.json`.

# Backup
`tools/backup.sh pre-T23`.

# Steps
1. Synthetic evok-3 fixture for the L513 (use the S103 fixture shapes with L513 counts and
   the placeholder circuit map) → run discovery + converter output + adapter in-process;
   replay the legacy capture; assert all acks/states.
2. Shadow on S103 for 24 h with converter output re-pointed to S103 circuits/LEDs and a
   test topic root; review logs for ERROR/WARNING.
3. **🔒 HUMAN** review `REPORT.md` and the 24 h log summary; approve.
4. Tag `v2.3.0` + GitHub Release (notes: legacy adapter available, default off).
   Deploy `v2.3.0` to S103 live (`legacy.enabled: false`).

# Acceptance checks
- e2e test green; shadow log has no ERROR; S103 healthy 24 h after `v2.3.0`.

# Rollback
`tools/rollback.sh v2.2.0`.

# Feed the elephant
`/log.md`; tasks index.

# Evidence

# Open questions
