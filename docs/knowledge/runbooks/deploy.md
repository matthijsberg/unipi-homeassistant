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

# Steps (after T03)

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
