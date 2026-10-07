---
type: Task
id: T30
title: "Pre-flight and go/no-go for the L513 evok-3 upgrade"
description: "Everything needed for the L513 upgrade is verified and prepared, and Matthijs has given an explicit GO with a scheduled maintenance window."
phase: 3
task_status: todo
depends_on: [T01, T23]
risk: medium
human_gate: true
target_hosts: [l513]
tags: [l513, evok3, upgrade]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
ADR-001 upgrade is the only step that cannot be undone by software. Prepare so the window is
short and the rollback is a card swap.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-001-upgrade-l513-to-evok3.md](/decisions/ADR-001-upgrade-l513-to-evok3.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)
- [/context/hardware-inventory.md](/context/hardware-inventory.md)

# Preconditions
- T01 recorded L513 storage medium and OS version. T23 done (`v2.3.0` released).

# Files in scope
new `/runbooks/l513-evok3-upgrade.md` (written by this task), `/context/hardware-inventory.md`.

# Backup
**🔒 HUMAN** full image of the L513 storage (`dd`/Win32DiskImager of the µSD, or Unipi's
eMMC backup procedure), stored off-box, checksum recorded. Fresh `baseline-l513` backup (T01 procedure).

# Steps
1. Research (cite sources in the runbook's `sources:`): current Unipi OS image for Neuron
   L513; whether it boots from µSD; evok 3 configuration of (a) the **xS30** extension on
   the L513 UART/RS485 port (Modbus RTU address, speed 19200 per evok-v2 `uart` data),
   (b) the 1-wire bus and sensors, (c) circuit naming for the extension. Check the S103's
   `/etc/evok/` as a working evok-3 example (read-only).
2. Write `/runbooks/l513-evok3-upgrade.md`: flash steps, first-boot settings (hostname,
   IP <L513_IP> static/DHCP reservation, user, SSH key), evok config, bridge install
   (venv, `requirements.txt`, systemd unit, `config.json` with
   `legacy.enabled: true`), verification walk, rollback (card swap). Time each step.
3. Shopping/prep list for **🔒 HUMAN**: new µSD card (industrial grade, same or bigger size),
   label for the old card, laptop with imager, physical access to the cabinet.
4. Go/no-go checklist (all must be YES): image + backups verified off-box; runbook reviewed
   (`verified` by human); `v2.3.0` healthy on S103 ≥ 7 days; HA full backup taken;
   household informed (doorbell, wall-switch lights, roof window, ventilation down ≤ 60 min);
   heat-pump smart-grid relays safe default known (what happens if both are OFF for an hour? — **🔒 HUMAN** confirm);
   weather OK for roof window being unavailable.
5. **🔒 HUMAN** GO + date/time → record in Evidence.

# Acceptance checks
- Runbook exists, human-verified; checklist all YES; GO recorded.

# Rollback
Nothing changed on the L513.

# Feed the elephant
New runbook; `/log.md`; inventory.

# Evidence

# Open questions
