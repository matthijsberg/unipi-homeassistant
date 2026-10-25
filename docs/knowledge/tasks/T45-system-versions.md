---
type: Task
id: T45
title: "Show OS / evok / firmware package versions and pending updates in Home Assistant; optional OS upgrade in the bootstrap"
description: "Diagnostic sensors with the software versions of every Unipi, and bootstrap.sh --upgrade-system that updates the OS and Pi firmware but holds back the Unipi board firmware."
phase: 1
task_status: done
depends_on: [T05]
risk: low
human_gate: false
target_hosts: [dev-only, s103, m203]
tags: [core, ha, firmware, bootstrap]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-25T15:00:00Z }
---

# Objective
Matthijs (2026-10-25): do we update firmware during installation, and can the firmware/OS/script versions be seen in HA?

# What the M203 taught us (2026-10-25, Matthijs flashed it by hand)
My first answer said the M203 had firmware "7.20". That was the **package** (`unipi-firmware6`) apt had installed; the two boards really ran **7.6**. After the package reached 7.22, its postinst said the update "must be invoked manually" (`sudo /opt/unipi/tools/fwspi --auto -v`, reboot, never interrupt the power) and the flash went 7.6 -> 7.22 on both boards. Consequences: (1) installing the package never flashes, so the planned "hold the firmware package back" was unnecessary and was **removed**; (2) the *running* firmware must be read separately - done: Modbus holding register 1000 of each board on unipitcp (port from `/etc/evok/autogen.yaml`; M203 50200, S103 502), high byte major / low byte minor, verified 1814 = 7.22 on both M203 boards = what `fwspi` reported.

# Answers and changes (`rc20`)
- Opt-in `bootstrap.sh --upgrade-system`: `apt-get full-upgrade` (OS, kernel, `raspi-firmware`), exit 10 when a reboot is needed; prints the manual `fwspi` command when a new `unipi-firmware6` package arrived. It never flashes.
- Opt-in `bootstrap.sh --auto-security-updates` (asked once on a terminal when neither flag nor `--yes` is given; `--no-auto-security-updates` leaves things as they are): `unattended-upgrades`, Debian-Security origin only (the general-updates origin is cleared), **no automatic reboot**, Unipi and Raspberry Pi repositories not included.
- `unipi_core/sysinfo.py` + bridge: 12 diagnostic sensors on the bridge device (refreshed at start-up, daily and on republish, collected in a worker thread, read-only): Bridge version, OS, kernel, Python, evok, `unipi-firmware6` / `raspi-firmware` package versions, **Board firmware (running)**, **Board firmware status** (`current` / `update available`), **Updates available** (attributes: package list, apt list age, how to flash), **Firmware updates available** (pending firmware packages + boards behind their package), **Automatic security updates**. Not in shadow mode. Extension modules (RS485) are not read yet.

# Evidence
- `tests/test_sysinfo.py` (15, incl. a fake Modbus slave: only function 3 is ever sent, exception frames are recognised at once, unreachable endpoint leaves the sensor out) and 14 bootstrap tests for the upgrade and auto-update steps (real step functions against fake apt tools: no hold-back, reboot exit 10, failed upgrade stops, security-only config without the Unipi/Pi repositories, never reboots, idempotent, dry run writes nothing, never switched off by "undecided"). 29 mutation checks; 3 survivors found and fixed with stronger tests (collection thread, exception handling speed, evok config block boundary).
- Live: see log (M203, rc20).

# Open questions
- Firmware of RS485 extension modules (xS...): different channel, not read.
- Whether `needrestart` (if installed) restarts `hass-unipi` after a security update of a shared library: a short gap in the local rules is acceptable but worth checking on the first real update.
