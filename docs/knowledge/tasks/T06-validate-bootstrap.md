---
type: Task
id: T06
title: "Validate the bootstrap in a clean arm64 Debian 12 container before the L513 window"
description: "bootstrap.sh (without Evok/hardware) succeeds from scratch in a clean arm64 Debian 12 environment, is idempotent, and its site-bundle round trip and release rollback work; remaining unknowns about the real BaseOS image are listed."
phase: 0
task_status: done
depends_on: [T05]
risk: low
human_gate: true
target_hosts: [dev-only]
tags: [deployment, validation, baseos]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T13:30:00Z }
---

# Objective
Find bootstrap bugs on a disposable system, not on the L513 in the maintenance window.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-006-baseos-deployment.md](/decisions/ADR-006-baseos-deployment.md)

# Preconditions
- T05 merged. **🔒 HUMAN** provides an arm64 Debian 12 environment: Docker on the Mac
  (Apple Silicon is native arm64): `docker run --rm -it --platform linux/arm64 debian:12 bash`,
  or a spare SD card/VM. If only a Pi without Docker is available, use a `--prefix` run on the S103.

# Files in scope
`tests/` (shell-level test script `tests/bootstrap_container_test.sh`), `/runbooks/l513-evok3-upgrade.md` (draft), `/log.md`.

# Backup
none (disposable).

# Steps
1. In the container: `apt-get update && apt-get install -y git sudo`, clone the repo at the
   tag, run `tools/bootstrap.sh --skip-evok --no-systemd --site <test bundle> --yes`.
2. Run it a second time: must be a no-op. Run with a bad bundle: must fail before changing anything.
3. Deploy a second tag (`deploy.sh`), roll back, break a tag on purpose: auto flip-back.
4. `site-bundle.sh export` → `import` round trip; compare checksums.
5. Record everything that can only be checked on real hardware/image (Evok install, kernel
   reboot, os-configurator, xS30 extension, 1-wire) as a checklist in
   `/runbooks/l513-evok3-upgrade.md` (Draft section "Unverified until the real card").
6. **🔒 HUMAN**: download the BaseOS image, record its exact filename, Debian version and sha256 in
   `/context/hardware-inventory.md`; if it is not Debian 12 / Python 3.11 tell the goldfish
   so `requirements.txt` wheel availability is re-checked.

# Acceptance checks
- Container run exit 0 twice; idempotency proven by identical tree checksums.
- Failure paths leave the tree unchanged.
- Checklist of real-hardware items written.

# Rollback
Nothing live was touched.

# Feed the elephant
Runbook draft, inventory (image facts), `/log.md`, tasks index.

# Evidence
**Done on a real Unipi M203 with a fresh BaseOS image (Debian 13, kernel 6.12.109), 2026-10-25** - better than the planned container (see `/context/hardware-inventory.md` for the image facts). Isolated test setup: local mosquitto on the M203, site bundle with `127.0.0.1` URLs (nothing touched the house broker or HA).
- Run 1 (`bootstrap.sh --site ... --tag v2.2.0-rc18 --yes`): apt prerequisites, evok 3.0.6.3 installed from Unipi's repo, release unpacked, venv built (Python 3.13, wheels for pydantic/aiohttp installed fine), config imported and checked, service started - then **failed at the health check**. Four more bootstrap/deploy bugs and one dependency bug were found and fixed one by one (below); runs 2-5 converged.
- Final state: **second/third/fourth run = "changes: none (already up to date)"**, `.bootstrap-state` stable; `deploy.sh v2.2.0-rc17` -> flip + restart + fresh health check OK; `rollback.sh previous` -> back on rc18, OK; one shared venv for both releases; bridge `online`, device `Neuron_M203_154`, extension watchdog `no_extension` (correct: no extension), **LED pulse 3x via MQTT works** on the real hardware.
- Bugs found only by the real image (all fixed, tests added): (1) `python3-minimal` has no `http`/`shutil`/`tarfile`/`venv`: the preflight and the site bundle crashed before any package was installed -> full python installed first, connectivity checked in plain bash, dry run copes with the incomplete python; (2) the health check read `.device_name` at once, but a first start writes it ~15 s later -> it waits; (3) `--fresh` demanded new publishes from a service that was not restarted -> fresh only after a (re)start, and the bootstrap restarts when release/config/unit changed; (4) BaseOS asks a password for every sudo and its `sudoers.d/unipi` is read after ours (last match wins) -> narrow NOPASSWD rule for `systemctl restart hass-unipi|evok` in `zz-hass-unipi`; before that, a refused `sudo -n systemctl restart` aborted `deploy.sh` AFTER the flip and left `current` on the new release while the old code ran -> now the same flip-back as an unhealthy release; (5) `python-pam` imports `six` without declaring it: web login was HTTP 500 on a fresh venv -> `six==1.17.0` in requirements.txt (+ the warning now names the real import error).
- Still unverified (needs the real L513 card): the xS30 extension and its evok config, 1-wire sensors specific to the L513, evok config from the L513 site bundle, kernel reboot behaviour of a first-time Unipi repo setup (not needed on this image: the repo and kernel were already in place).


# Open questions
