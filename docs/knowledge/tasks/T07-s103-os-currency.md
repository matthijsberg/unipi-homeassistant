---
type: Task
id: T07
title: "Bring the S103 operating system up to date and re-verify the bridge"
description: "Pending Debian 12 updates are applied on the S103 in a planned window, the Unipi stack is unchanged and healthy afterwards, and the bridge test-suite and health check pass on the updated platform."
phase: 1
task_status: todo
depends_on: [T03]
risk: medium
human_gate: true
target_hosts: [s103]
tags: [os, updates, platform]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T14:30:00Z }
---

# Objective
Matthijs wants the code proven on the S103 with an up-to-date OS, Python and firmware before the
L513 image is built. Baseline (2026-10-07): Debian 12.15, Python 3.11.2, Unipi kernel
`6.6.31-v8` (unipi-kernel 0.20240627), unipi-firmware6 7.20, evok 3.0.6.1, os-configurator 0.76 —
**all Unipi packages already at the newest version in `repo.unipi.technology`**. Pending: 21
Debian packages (openssl/libssl3, perl, libexpat, libpng, liblzma, ca-certificates, tzdata,
`raspi-firmware` 1.20260915, …).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)
- [/context/hardware-inventory.md](/context/hardware-inventory.md)

# Preconditions
- `apt-get -s upgrade` shows no `unipi-*`, `evok*`, `linux-image*` or kernel packages (otherwise STOP and ask).
- **🔒 HUMAN** agrees a window (house impact: lights/doorbell wiring on the S103 and its HA entities
  are unavailable during the ~2-3 min reboot).
- Optional but recommended **🔒 HUMAN**: full SD image of the S103 (`raspi-firmware` touches boot files).

# Files in scope
none in git (system packages); `/context/hardware-inventory.md`, `/log.md`.

# Backup
`tools/backup.sh pre-T07` + record `dpkg -l` and `apt list --upgradable` before/after.

# Steps
1. Show the exact upgrade list; **🔒 HUMAN** approves (flag `raspi-firmware`: if the human prefers, hold it
   with `apt-mark hold raspi-firmware` and note it).
2. `sudo apt-get -y upgrade` (never `dist-upgrade`/`full-upgrade`, never a release upgrade).
3. Reboot; wait for evok, owserver, unitcp, hass-unipi.
4. Verify: kernel version unchanged; `systemctl --failed` empty; `tools/healthcheck.py --fresh`;
   `GET :8080/rest/all` has `device_info`; xS51 extension `last_comm` small; 1-wire sensors present;
   the unit tests; the LED round trip.
5. Record versions after.

# Acceptance checks
- Same Unipi package versions as before; `apt list --upgradable` empty (or only held packages).
- All services active; health check OK; 24 h without new ERROR lines.

# Rollback
Package-level: `apt-get install pkg=old-version` for the recorded versions; boot-level: restore the SD
image (if taken). Code is unaffected (`tools/rollback.sh`).

# Feed the elephant
Inventory versions; `/log.md`.

# Evidence

# Open questions
- Hold `raspi-firmware`? (Human.)
