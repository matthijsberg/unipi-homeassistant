---
type: Runbook
title: "Backup and rollback"
description: "How to take a backup before any change and how to return either Unipi to a known-good state within minutes."
tags: [runbook, backup, rollback, mandatory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Backup (every change, both boxes)

`~/src/unipi-homeassistant/tools/backup.sh <label>` — prints the archive path (tool exists since T03; see `tools/README.md`).

Manual equivalent (use until T03 is done):

```bash
TS=$(date -u +%Y%m%dT%H%M%SZ); L=<label>; D=~/backups/$TS-$L; mkdir -p "$D"
tar -C /home/unipi/unipi-homeassistant -czf "$D/runtime.tgz" --exclude=scripts/backups --exclude='scripts/__pycache__' scripts
cp /etc/systemd/system/hass-unipi.service "$D/" 2>/dev/null
sudo tar -czf "$D/etc-evok.tgz" /etc/evok* 2>/dev/null; sudo chown unipi: "$D/etc-evok.tgz"
/home/unipi/unipi-homeassistant/bin/pip freeze > "$D/pip-freeze.txt"
(cd ~/src/unipi-homeassistant 2>/dev/null && git rev-parse HEAD > "$D/git-head.txt")
sha256sum "$D"/* > "$D/SHA256SUMS"
```

Off-box copy: `scp -r "$D" unipi@<other-unipi>:backups/<this-host>/` (+ NAS when configured).
A backup that exists only on the box being changed **does not count**.

# Rollback — bridge code/config (S103 or L513 after T32)

1. `tools/rollback.sh <tag-or-backup-path>` (T03). It stops `hass-unipi`, restores files,
   restarts, and runs the health check.
2. Manual: `sudo systemctl stop hass-unipi` → untar the chosen `runtime.tgz` over
   `/home/unipi/unipi-homeassistant/` → `sudo systemctl start hass-unipi` →
   check `unipi/<dn>/status` = `online` and `unipi/<dn>_bridge/startup_error` = `0`
   after 60 s.
3. HA side: discovery is retained; after rollback the bridge republishes on start. Entities
   that only exist in the newer version show *unavailable* — harmless.

# L513 backup policy

No network backup of the L513 (decision 2026-10-07): the **old SD card is the backup**. It is
never written to; the upgrade uses a new card. Keep the old card labelled until T42 + 30 days.
Reference copies of the old script/config live in the S103 baseline backup.

# Rollback — L513 before cutover is accepted (T31–T33)

1. Power down the L513 (doorbell + wall switches go down).
2. Replace the new µSD card with the **old, labelled card** (or restore the full image).
3. Power up. Old `unipi_mqtt.py` starts as before; HA YAML entities come back.
4. If the legacy adapter had been publishing, old topics are already in the right format;
   check doorbell, one light switch, one PIR.
5. Write the rollback in `/log.md` with the reason.

# Rollback — Home Assistant changes (T20, T40, T41)

Restore the HA full backup taken in the task's Backup step (Settings → System → Backups),
or revert the specific YAML/automation files from the HA config git if you use one.

# Retention

`~/backups`: keep last 30 + every backup whose label starts with `pre-v` (release) or
`pre-T3` (L513 upgrade). Never delete the old L513 card before T42 is done + 30 days.
