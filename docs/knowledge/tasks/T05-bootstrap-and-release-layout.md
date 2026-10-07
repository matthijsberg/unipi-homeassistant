---
type: Task
id: T05
title: "Build the BaseOS bootstrap, release layout and site-bundle tooling"
description: "tools/bootstrap.sh, tools/site-bundle.sh and release-aware deploy/rollback exist; the bridge accepts a state dir and --check-config; all default-off for the live S103."
phase: 0
task_status: todo
depends_on: [T03, T04]
risk: medium
human_gate: false
target_hosts: [dev-only, s103]
tags: [deployment, baseos, bootstrap]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T13:30:00Z }
---

# Objective
Make "new SD card with BaseOS → working bridge" one repeatable command (ADR-006).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-006-baseos-deployment.md](/decisions/ADR-006-baseos-deployment.md)
- [/decisions/ADR-004-versioning-and-backups.md](/decisions/ADR-004-versioning-and-backups.md)
- [/runbooks/deploy.md](/runbooks/deploy.md)

# Preconditions
- T03 + T04 merged, `pytest -q` green, `tools/healthcheck.py` OK on the S103.

# Files in scope
`tools/bootstrap.sh`, `tools/site-bundle.sh`, `tools/deploy.sh`, `tools/rollback.sh`,
`systemd/hass-unipi.service` (release-layout unit), `hass-unipi.py` (only: `UNIPI_HA_STATE_DIR`
for the device-name cache, `--check-config`; defaults unchanged), `tests/test_check_config.py`,
`tests/test_site_bundle.py`, `.gitignore`, `tools/pii_scan.py` (forbid `*.site.tgz`, `site-*.tgz`),
`tools/README.md`.

# Backup
`tools/backup.sh pre-T05` on the S103 (the code change is only deployed via deploy.sh).

# Steps
1. Code: `DEVICE_NAME_CACHE_FILE` = `$UNIPI_HA_STATE_DIR/.device_name` when set, else today's
   path; `--check-config` loads/validates the config (incl. `circuits` from T11 when it exists),
   prints a one-line summary, exits 0 / 2. Tests: default path unchanged; env override; check-config
   ok/invalid.
2. `systemd/hass-unipi.service` per ADR-006 layout (kept separate from the repo-root unit that
   the S103 uses).
3. `site-bundle.sh export|import` per ADR-006 (manifest with sha256, mode 600, no overwrite
   without `--force`, backup of replaced files). Tests with a temp tree.
4. `bootstrap.sh` per ADR-006 contract: every step idempotent (check-then-act), `--dry-run`
   prints exactly what would change, all state in `/var/lib/unipi-ha/.bootstrap-state`,
   exit 10 for "reboot required". `--prefix DIR` redirects /opt, /etc, /var and skips apt,
   evok and systemd (for T06 and CI).
5. `deploy.sh` / `rollback.sh`: detect release layout (`$PREFIX/opt/unipi-ha/current` is a
   symlink) → unpack release, venv by requirements hash, atomic symlink flip, restart,
   fresh health check, automatic flip-back; legacy copy-in-place mode stays for the S103.
6. `.gitignore` + `pii_scan.py`: site bundles are forbidden files.

# Acceptance checks
- `pytest -q` green (incl. new tests); existing tests unchanged.
- `tools/bootstrap.sh --prefix /tmp/x --dry-run` then real run twice: second run changes nothing
  (diff of the tree + `.bootstrap-state` identical).
- `site-bundle.sh export` on the S103 → import into `--prefix` tree → `--check-config` passes.
- Deploying a tag, then a second tag, then `rollback.sh` in the temp prefix: symlink flips both
  ways; a deliberately broken tag flips back automatically (fake `systemctl` via
  `UNIPI_SYSTEMCTL`).
- Live S103 unaffected: still `v2.0.0`-equivalent behaviour; `deploy.sh` of the new tag works in
  legacy mode (service healthy).

# Rollback
Revert the PR; S103: `tools/rollback.sh <previous tag>`.

# Feed the elephant
ADR-006 → `status: stable` after Matthijs reviews; `/runbooks/deploy.md` (release-mode section);
`/context/code-map.md` (tooling); `/log.md`.

# Evidence

# Open questions
- Does Unipi's `raspberry-neuron.sh` need a reboot on BaseOS (assumed yes)? Resolved in T06/T31.
