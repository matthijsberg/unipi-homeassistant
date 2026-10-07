---
type: Task
id: T00
title: "Restore GitHub push access on the S103"
description: "gh is authenticated on the S103 and can push to matthijsberg/unipi-homeassistant."
phase: 0
task_status: done
depends_on: []
risk: low
human_gate: true
target_hosts: [s103]
tags: [git, github]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
The `gh` token in `~/.config/gh/hosts.yml` is expired (K9). Without push access nothing in
ADR-004 works.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-004-versioning-and-backups.md](/decisions/ADR-004-versioning-and-backups.md)

# Preconditions
- `gh auth status` → shows "token … is no longer valid".

# Files in scope
none (credential store only)

# Backup
none needed.

# Steps
1. **🔒 HUMAN** run `gh auth login -h github.com` (choose HTTPS + browser/device code).
   Prefer a fine-grained token limited to `unipi-homeassistant` with *Contents: read/write*
   and *Pull requests: read/write*.
2. Optional **🔒 HUMAN**: enable branch protection on `main` (require PR).

# Acceptance checks
- `gh auth status` → "Logged in to github.com".
- `cd ~/src/unipi-homeassistant && git push --dry-run origin plan/legacy-migration` → no auth error.

# Rollback
`gh auth logout -h github.com`.

# Feed the elephant
`/log.md` entry; set `task_status: done`.

# Evidence

# Open questions
