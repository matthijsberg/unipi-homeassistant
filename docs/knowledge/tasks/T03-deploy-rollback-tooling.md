---
type: Task
id: T03
title: "Add backup, deploy, rollback and health-check scripts"
description: "tools/backup.sh, tools/deploy.sh, tools/rollback.sh and tools/healthcheck.py exist, are tested on the S103 by redeploying v2.0.0, and auto-rollback works."
phase: 0
task_status: done
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
- Tools: `tools/backup.sh`, `deploy.sh`, `rollback.sh`, `healthcheck.py`, `tools/README.md`; `tests/test_healthcheck.py` (regression, mutation-verified). `bash -n` clean (shellcheck not installed; not installed without consent). 45 tests pass.
- **Live test 1 (S103)** `deploy.sh v2.0.0`: preflight ok → auto backup `…-pre-v2.0.0` → restart → `healthcheck: OK - healthy (version 2026092501)`; 69 s total (≈60 s of that is waiting for the bridge's fresh `startup_error=0`).
- **Live test 2 — FOUND A REAL BUG.** First run of a release that compiles but crashes at startup: the health check did not detect the crash loop and waited the full 120 s before auto-rollback worked ⇒ ~2 min outage instead of ~25 s. Cause: `systemctl show -p A -p B --value` prints in systemd's *internal* order, not the requested order, so `ActiveState` and `NRestarts` were swapped. Fix: parse `key=value` (`sysd()`); regression test `test_sysd_returns_values_in_requested_order` (fails against the old parsing — mutation-verified).
- My first "syntax error" test release (`this is not python`) was valid Python (`x is not y`), so preflight correctly accepted it — the test input was wrong, not the tool. It still usefully exercised the crash path.
- **Live re-tests after the fix**: genuine syntax error → `PREFLIGHT FAILED … nothing was changed`, no backup, no restart, `.deployed_tag` unchanged. Crash-at-startup release → `healthcheck: FAIL - service crash-looping` within seconds → automatic rollback from the pre-deploy backup → `healthy (version 2026092501)`; runtime hash back to `dd4afedae96d`, `.deployed_tag` = `v2.0.0`.
- Throwaway worktree/branch/tags removed. Backups from the tests remain in `~/backups` (`pre-vtest-*`; they match the retention rule's `pre-v*` pattern only for `pre-v2.0.0`; the `pre-vtest-*` ones are normal rotation).
- **Not done**: off-box copy is not automated yet — no `backup.offbox_targets` configured (no target reachable from this box). Interim: manual hand-over (T01). Add targets to the runtime `config.json` when available.
- Service downtime per restart: bridge back `online` within ~15 s; the 70 s is only the verification wait.

# Open questions
