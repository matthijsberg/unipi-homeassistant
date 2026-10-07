---
type: Task
id: T01
title: "Take verified baseline backups and confirm what the L513 runs (no L513 access needed)"
description: "A checksum-verified baseline backup of the S103 exists on the S103 and off-box, the L513 baseline is documented from the downloaded script copies plus read-only REST/MQTT snapshots, and the L513 rollback is the physically swapped SD card."
phase: 0
task_status: in_progress
depends_on: []
risk: low
human_gate: true
target_hosts: [s103]
tags: [backup, inventory]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T11:45:00Z }
---

# Objective
Before any change we need a restorable copy of what we can touch (the S103) and certainty
about what the L513 runs, without logging in to the L513 (decision 2026-10-07: the
L513's backup is its **swap SD card**; its old script runs as a **root systemd service at
boot**; the script/config copies in `scripts/old_unipi_mqtt/` are the latest, downloaded
the day before).

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/runbooks/backup-and-rollback.md](/runbooks/backup-and-rollback.md)
- [/context/hardware-inventory.md](/context/hardware-inventory.md)

# Preconditions
- `systemctl is-active hass-unipi` on the S103 → `active`.
- The L513 evok REST answers a plain GET (read-only) — no SSH, no writes, no login.

# Files in scope
- `~/backups/**` on the S103 (new; mode 700, files 600; contains secrets — never in git)
- `/context/hardware-inventory.md`, `/analysis/known-issues.md` (K12), `/runbooks/backup-and-rollback.md`, `/log.md`

# Backup
This task *is* the backup.

# Steps
1. S103 baseline backup, label `baseline-s103`: runtime dir (without old backups, venv,
   caches), the old-scripts folder, systemd unit, `/etc/evok*`, `pip freeze`, evok/unipi
   package list, git HEAD of the clone, sha256 of the live script, REST snapshots of **both**
   evok instances (read-only GET), and a 12 s retained-MQTT snapshot of `unipi1/#` and
   `unipi/#`. `SHA256SUMS` over everything.
2. L513 baseline from what we already hold (no login): the old script + config copies (in
   the backup), the REST snapshot, the retained MQTT snapshot.
3. K12 analysis: compare retained legacy topics and unconfigured L513 inputs against the
   config (see Evidence).
4. **🔒 HUMAN** off-box copy of the S103 backup (a backup only on the box being changed
   does not count). Options: copy to the PC/NAS, or have the goldfish hand the archive over
   in the app session. Record where it went.
5. **🔒 HUMAN** confirm physically before T30: the L513 boots from a **removable µSD**, and
   which card/size to use for the upgrade; the old card is labelled and kept untouched.

# Acceptance checks
- `sha256sum -c SHA256SUMS` passes in the backup folder (done: 0 failures).
- Off-box copy exists and its checksums match (**pending**).
- Inventory unknowns updated; K12 resolved or turned into a T20 check.

# Rollback
Nothing changed on any Unipi (read-only GETs and one read-only MQTT subscription).

# Feed the elephant
Inventory, K12, backup runbook (L513 rollback = SD swap), `/log.md`.

# Evidence
- 2026-10-07 11:30Z: backup `~/backups/20261007T113014Z-baseline-s103` (≈0.8 MB, 13 files);
  `sha256sum -c` → 0 failures; `/etc/evok*` captured via passwordless sudo.
- Retained MQTT snapshot: 181 topics (178 retained).
- Old config: 45 entries, 44 state topics, every one has a retained `<topic>/available`.
- L513 REST (evok v2): 40 inputs; all 28 configured inputs exist; the 12 **unconfigured**
  inputs are all on the xS30 extension (`UART_4_4_08–11`, `17–24`) and **all read 0**
  (a wired normally-closed contact reads 1, so these look unused).
- 11 retained legacy topics are not in the current config: `lekkage-koelkast`, case
  duplicate `lekkage-Keukenkasten`, `serre/vleugel_{hoek,huis,tuin}` (+ typo `vllugel_hoek`),
  `woonkamer/vleugel_{serre,tuin}/contact`. An older config copy on the S103 also lacks them.
  **Conclusion (high confidence, not proof): stale leftovers from earlier config versions.**
  T20's capture must confirm they never change.

# Open questions
- Where should off-box copies of backups go (PC/NAS path, or hand-over in the app)?
- Confirm L513 boots from a removable µSD (human, before T30).
