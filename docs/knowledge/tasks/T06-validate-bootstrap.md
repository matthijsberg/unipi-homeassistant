---
type: Task
id: T06
title: "Validate the bootstrap in a clean arm64 Debian 12 container before the L513 window"
description: "bootstrap.sh (without Evok/hardware) succeeds from scratch in a clean arm64 Debian 12 environment, is idempotent, and its site-bundle round trip and release rollback work; remaining unknowns about the real BaseOS image are listed."
phase: 0
task_status: todo
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

# Open questions
