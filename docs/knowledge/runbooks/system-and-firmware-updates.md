---
type: Runbook
title: "OS, Raspberry Pi firmware and Unipi firmware: what is updated how, and what Home Assistant shows"
description: "Three layers with different risk. The bootstrap can bring the OS and Pi firmware up to date and optionally install Debian security updates; the Unipi controller firmware is flashed only by a deliberate command."
tags: [runbook, firmware, updates, baseos]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-25T16:00:00Z }
---

# Layers (verified on the S103 and a fresh M203, 2026-10-25)
| Layer | Package | How it is updated | Our tooling |
|---|---|---|---|
| OS, libraries | Debian packages (S103 bookworm / M203 trixie) | `apt-get full-upgrade` | `bootstrap.sh --upgrade-system` (opt-in) |
| Kernel | `unipi-kernel*` (Unipi repo) | apt + **reboot** | same; exit 10 = reboot, then run again |
| Raspberry Pi firmware (GPU/bootloader) | `raspi-firmware` | apt + **reboot** | same |
| evok | `evok` (Unipi repo) | apt | installed by the bootstrap, not upgraded unless you decide |
| **Unipi controller firmware** | package `unipi-firmware6` (images under `/usr/lib/firmware/unipi`) puts the new images on disk; **the boards keep running their old firmware until `fwspi` flashes them** | manual, see below | reported in HA, never automatic |
| Debian security updates | `unattended-upgrades` | automatic if switched on | `bootstrap.sh --auto-security-updates` (opt-in) |

# Package version is not the running firmware (what the M203 taught us)
On the fresh M203 apt had `unipi-firmware6 7.20` installed while the two controller boards actually ran **7.6** (the image's own firmware). After `apt` brought the package to 7.22, the package said, verbatim: *"Firmware update available. Current firmware in your PLC (7.6) should be replaced by the newly installed one (7.22). This operation must be invoked manually: `sudo /opt/unipi/tools/fwspi --auto -v` ... Reboot the PLC unit after the update. WARNING: Do not disconnect the power supply during the update, as it may lead to permanent board damage."* The flash then went 7.6 -> 7.22 on both boards (STM32F051, 30 pages each). Installing the package does **not** flash; the flash is always a separate command.

# How to flash the Unipi controller firmware (a deliberate step, someone at the box)
1. See what is pending: HA bridge device -> *Board firmware (running)*, *Board firmware status* (`update available`), *Firmware updates available*; the attribute `how_to_flash` holds the command.
2. `tools/backup.sh pre-firmware`, `tools/site-bundle.sh export`.
3. Make sure the package is current: `sudo apt-get install --only-upgrade unipi-firmware6` (or `--upgrade-system`).
4. `sudo /opt/unipi/tools/fwspi --auto -v` (per board: "Upgrading firmware X (old was Y)", "7400 OK", "REBOOTING..."). **Never interrupt the power.**
5. `sudo reboot`. Then `tools/healthcheck.py`, check inputs/outputs, note it in `/log.md`.
Docs by Unipi: <https://kb.unipi.technology/en:sw:04-unipi-firmware:05-update-firmware> (automated fetching is blocked; read by hand).

# Automatic Debian security updates (onboarding choice)
`bootstrap.sh --auto-security-updates` installs `unattended-upgrades` and writes `/etc/apt/apt.conf.d/52unipi-auto-security`: package lists daily, security updates only (origin *Debian-Security*, the "general updates" origin is cleared), **never reboots by itself**, and the Unipi / Raspberry Pi repositories (kernel, evok, firmware) are not included - those change when a person decides. Without a flag and without `--yes` the bootstrap asks once on a terminal; `--no-auto-security-updates` (or the default under `--yes`) changes nothing and never switches an existing setup off. Remove the file to disable. Good to know: library updates are installed but a running process keeps the old library until it restarts; a kernel/firmware change only takes effect at the next (manual) reboot. HA shows the state as *Automatic security updates*.

# What Home Assistant shows (T45)
On the "Unipi ... Bridge" device (category *Diagnostic*), refreshed at start-up and every 24 h: Bridge version, Operating system, Kernel, Python, Evok version, Unipi firmware package, Raspberry Pi firmware package, **Board firmware (running)** (read from Modbus holding register 1000 of every board: high byte major, low byte minor; verified 1814 = 7.22 = what `fwspi` reported), **Board firmware status**, **Updates available** (attributes: package list, `package_lists_age_days`), **Firmware updates available** (pending firmware packages + boards behind their package), **Automatic security updates**. "Updates available" only knows what `apt update` has fetched: check `package_lists_age_days`. Extension modules (xS...) on RS485 are not read yet.
