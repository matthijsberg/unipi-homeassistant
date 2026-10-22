---
type: Runbook
title: "Deploy a tagged version to a Unipi"
description: "The only allowed way to change code or config on a live Unipi — backup, deploy from a git tag, health check, automatic rollback."
tags: [runbook, deploy, mandatory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Order of deployment

`dev/tests` → **S103 shadow instance** (T18) → **S103 live** → **L513** (from T32 on).
Never deploy to both live boxes in the same hour; soak S103 ≥ 24 h first.

# Steps (tools exist since T03; verified live on the S103)

```bash
cd ~/src/unipi-homeassistant && git fetch --tags && git status   # clean tree required
tools/deploy.sh v2.2.0            # does: backup → checkout tag into runtime dir →
                                  # pip install -r requirements.txt (if changed) →
                                  # systemctl restart hass-unipi → health check (90 s)
                                  # → auto-rollback on failure
```

Health check = all of:
- `systemctl is-active hass-unipi` = `active` and no restart in the window
- MQTT `unipi/<dn>/status` = `online`
- MQTT `unipi/<dn>_bridge/startup_error` = `0` (published ~60 s after start)
- version in discovery `origin.sw` equals the tag

# Manual fallback (before T03)

Follow `/runbooks/backup-and-rollback.md` → Backup, copy files, `sudo systemctl restart
hass-unipi`, then verify the four health items by hand.

# After deploy

Append to `/log.md`: date, host, tag, health result, backup path.

# Release layout (BaseOS rebuilds, T05) - the S103 still uses the classic mode above
```bash
# fresh BaseOS card, as root, from a checkout of the wanted tag:
tools/bootstrap.sh --site my-unipi.site.tgz --dry-run     # shows exactly what would change
sudo tools/bootstrap.sh --site my-unipi.site.tgz          # exit 10 = reboot, then run the same command again
tools/deploy.sh v2.3.0          # unpack -> venv -> --check-config with the live config -> flip -> restart -> health check -> flip back on failure
tools/rollback.sh previous      # or: rollback.sh <tag>   (a flip, seconds)
tools/site-bundle.sh export     # keep the result PRIVATE (it holds passwords): Mac/NAS, never git
```
Layout: `/opt/unipi-ha/{releases/<tag>,venvs/<hash>,current,previous}`, config `/etc/unipi-ha`, state `/var/lib/unipi-ha`
(`UNIPI_HA_STATE_DIR`). The bridge refuses to start with a config that fails `--check-config` (unit `ExecStartPre`).
Tests/CI: `--prefix DIR` builds the same tree under DIR without apt/evok/systemd; `UNIPI_SYSTEMCTL`, `UNIPI_HEALTHCHECK`, `UNIPI_PREFIX` redirect the service calls.
