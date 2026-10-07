---
type: Task
id: T01
title: "Take verified baseline backups of both Unipis and confirm what really runs on the L513"
description: "Off-box backups of both boxes exist with checksums, SSH from S103 to L513 works, and the live L513 script/config is captured and compared with the copy in /home/unipi/scripts/old_unipi_mqtt."
phase: 0
task_status: todo
depends_on: []
risk: low
human_gate: true
target_hosts: [s103, l513]
tags: [backup, inventory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
Before any change we need a restorable copy of both systems (ADR-004) and certainty that the
legacy contract is derived from what *actually* runs on the L513 (K12).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)
- [/context/hardware-inventory.md](/context/hardware-inventory.md)

# Preconditions
- `systemctl is-active hass-unipi` on S103 → `active`.
- `ping -c1 <L513_IP>` → reply.

# Files in scope
- `~/backups/**` on both boxes (new)
- `~/.ssh/authorized_keys` on the L513 (append S103 public key) — 🔒 HUMAN
- `/context/hardware-inventory.md`, `/context/interface-legacy.md` (facts only)

# Backup
This task *is* the backup.

# Steps
1. S103: run the manual backup block from the runbook with label `baseline-s103`.
2. **🔒 HUMAN** give S103 SSH access to the L513: append the content of S103
   `~/.ssh/id_ed25519.pub` to the L513 user's `~/.ssh/authorized_keys` (user that runs the
   old script; tell the goldfish which user).
3. On L513 (via SSH from S103), read-only discovery — record outputs in Evidence:
   `hostname; cat /etc/os-release; uname -a; dpkg -l | grep -i evok; lsblk; findmnt /;
   systemctl list-units --type=service | grep -iE 'unipi|mqtt|evok'; ps aux | grep -i [u]nipi;
   ls -la /etc/evok* ; crontab -l`
   Find the running script path from `ps`/systemd unit.
4. L513 backup: same manual block adapted (runtime dir = the old script's dir, its systemd
   unit, `/etc/evok*`, `pip freeze` of the interpreter it uses) with label `baseline-l513`.
5. Copy each box's backup to the other box (`~/backups/<host>/`). **🔒 HUMAN**: tell the
   goldfish if there is a NAS/PC target; if yes copy there too.
6. `diff` the live L513 `unipi_mqtt.py` and `unipi_mqtt_config.json` against
   `/home/unipi/scripts/old_unipi_mqtt/`. If they differ, the **live** files win: copy them
   into the S103 backup folder and list differences in Evidence.
7. **🔒 HUMAN** (prep for T30, not executed now): note whether L513 boots from µSD or eMMC
   (`lsblk` output tells: `mmcblk0` with a removable card vs eMMC).

# Acceptance checks
- `sha256sum -c SHA256SUMS` passes in every backup folder, on both the source and the copy.
- Evidence contains the L513 discovery output and the diff result.
- `/context/hardware-inventory.md` "Unknowns" updated (hostname, OS/evok version, storage, script path).

# Rollback
Nothing changed except an added SSH key; remove that line from L513 `authorized_keys`.

# Feed the elephant
Update inventory unknowns; if live config differs, update `/context/interface-legacy.md`
§2/§3 and its `sources`; `/log.md`.

# Evidence

# Open questions
