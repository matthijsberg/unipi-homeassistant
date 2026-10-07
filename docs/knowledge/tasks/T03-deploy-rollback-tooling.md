---
type: Task
id: T03
title: "Add backup, deploy, rollback and health-check scripts"
description: "tools/backup.sh, tools/deploy.sh, tools/rollback.sh and tools/healthcheck.py exist, are tested on the S103 by redeploying v2.0.0, and auto-rollback works."
phase: 0
task_status: todo
depends_on: [T02]
risk: medium
human_gate: false
target_hosts: [s103]
tags: [tooling, deploy, backup]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Make "always able to go back" a one-command reality (ADR-004, R5) before any behaviour change.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)
- [/runbooks/deploy.md](/runbooks/deploy.md)

# Preconditions
- Tag `v2.0.0` exists and equals live (T02 acceptance).
- `sudo -n systemctl status hass-unipi` works without password prompt, or **🔒 HUMAN** adds a
  sudoers rule limited to `systemctl {start,stop,restart,status} hass-unipi`.

# Files in scope
`tools/backup.sh`, `tools/deploy.sh`, `tools/rollback.sh`, `tools/healthcheck.py`,
`tools/README.md`, `config.example.json` (add optional `backup` section).

# Backup
Manual backup block from the runbook, label `pre-T03`.

# Steps
1. `backup.sh <label>`: implements the runbook block; reads optional
   `backup.offbox_targets` (list of `user@host:path`) from `config.json` and scp's there;
   prints the archive path; exit ≠ 0 if any off-box copy fails (but keeps the local one).
   Retention per runbook.
2. `healthcheck.py --timeout 90`: uses the venv python + paho; reads `config.json` for broker
   and `.device_name`; passes when all four health items of `/runbooks/deploy.md` hold.
   Exit codes: 0 ok, 1 failed, 2 could not evaluate.
3. `deploy.sh <tag>`: refuse dirty tree; `backup.sh pre-<tag>`; `git archive <tag>` of the
   runtime files (code, `web/`, `requirements.txt`, `unipi_core/` when it exists,
   `legacy_adapter.py` when it exists) into the runtime dir — **never** overwrite
   `config.json`, `local_rules.json`, `legacy_map.json`, `.device_name`; pip install if
   `requirements.txt` changed; restart; `healthcheck.py`; on failure call
   `rollback.sh <backup>` and exit 1. Supports `--host l513` later via ssh (stub + TODO is fine now).
4. `rollback.sh <tag|backup-path>`: tag ⇒ same as deploy without the auto-rollback loop;
   path ⇒ restore that `runtime.tgz`. Always health-checks.
5. Test on S103: `deploy.sh v2.0.0` (no-op content change, real restart) → healthy.
   Then simulate failure: deploy a throwaway local tag whose `hass-unipi.py` has a syntax
   error → must auto-rollback and end healthy. Delete the throwaway tag.

# Acceptance checks
- Both test runs above, with output, in Evidence (downtime per restart noted, expected < 30 s).
- `ls ~/backups` shows `pre-v2.0.0` style folders on S103 and the off-box copy.
- `shellcheck tools/*.sh` clean (install `shellcheck` only if the human agrees; otherwise `bash -n`).

# Rollback
Scripts are additive; delete `tools/`. Live state is restored by the scripts themselves.

# Feed the elephant
Replace "manual equivalent" notes in the runbooks with the script names; `/log.md`.
Tag `v2.1.0` only after T04 is also merged.

# Evidence

# Open questions
