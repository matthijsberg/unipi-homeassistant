---
type: Decision
title: "ADR-006 Deploy on Unipi BaseOS with a bootstrap script, release folders and a private site bundle"
description: "A fresh BaseOS SD card becomes a working bridge with one idempotent bootstrap command; code lives in versioned release folders behind a current symlink; machine-specific config is a separate private site bundle."
tags: [adr, deployment, baseos, sd-card, bootstrap]
status: draft
generated: { by: claude-code/claude-sonnet-5-5, at: 2026-10-07T13:30:00Z }
sources:
  - id: kb-opensource
    resource: https://kb.unipi.technology/en:hw:02-neuron:download-image:03-opensource
    title: "Unipi KB - OpenSource OS / BaseOS (direct fetch blocked with HTTP 403; facts taken from search results, to be re-verified against the downloaded image in T30)"
  - id: evok-install
    resource: https://evok.readthedocs.io/en/stable/installation/
    title: "Evok installation (repo script + apt install evok)"
  - id: s103-reference
    resource: file:///etc/os-release
    title: "S103 as working reference: Debian 12 bookworm arm64, Unipi packages from repo.unipi.technology"
---

# Context
The L513 gets a new SD card with **Unipi BaseOS** (decision by Matthijs, built by hand). What
we know (to re-verify on the real image): BaseOS is plain Debian 12 (arm64); it **does not
include Evok** any more (installed from Unipi's apt repo); first boot expects DHCP, SSH is on,
the user is `unipi` with a **publicly documented default password**. The S103 is a working
template (Debian 12 + Unipi kernel/firmware/os-configurator + evok 3.0.6.1 + a small
`/etc/evok/config.yaml` describing its extension).

Requirement (Matthijs): "a very easy way of deploying our code and framework on top of this
baseOS", repeatable for any future SD card rebuild.

# Decision (2026-10-07, Matthijs)
1. **Method: bootstrap script + versioned release** (not a .deb, not a pre-baked image).
   `.deb` packaging may be added later on top without changing the layout.
2. **Layout: release folders + `current` symlink** (new SD card first; the S103 may migrate later).
3. **Site bundle stored as a plain archive on Matthijs' Mac/NAS** (no extra repo, no encryption
   layer). It contains passwords: the human is responsible for where it is kept. Never in git;
   the PII scanner forbids `*.site.tgz`.
4. **Backups (amends ADR-004):** on-device backups (`tools/backup.sh`) + Git/GitHub versioning of
   everything we build are sufficient; automated off-box backup is not required. Config
   survives an SD loss through the site bundle.

# Layout (target; FHS-style, nothing machine-specific inside /opt)
```
/opt/unipi-ha/
  releases/<tag>/            git archive of the tag: hass-unipi.py, web/, unipi_core/, legacy_adapter.py,
                             requirements.txt, tools/, systemd/hass-unipi.service, VERSION
  venvs/<sha8-of-requirements>/   python venv, shared by releases with identical requirements
  current -> releases/<tag>  switched atomically (ln -sfn + mv -T)
/etc/unipi-ha/               config.json  local_rules.json  legacy_map.json     (mode 600, owner unipi)
/var/lib/unipi-ha/           .device_name  local_rules_state.json  .discovered_presets  (+ CWD of the service)
/etc/evok/config.yaml        owned by Evok; captured/restored through the site bundle
```
Service: `User=unipi` (PAM login of the web UI only works for the process' own user),
`WorkingDirectory=/var/lib/unipi-ha`, `ExecStart=/opt/unipi-ha/current/.venv/bin/python
/opt/unipi-ha/current/hass-unipi.py --config /etc/unipi-ha/config.json`.
Code needs two small, default-off changes: state dir via `UNIPI_HA_STATE_DIR` (device-name
cache), and `--check-config` (validate + exit). Both land in T05.

# Bootstrap contract (`tools/bootstrap.sh`, T05)
Idempotent and re-runnable; two phases because Unipi's repo script needs a reboot.
`--dry-run`, `--site FILE`, `--tag TAG` (default: the checked-out tag), `--skip-evok`,
`--prefix DIR` (testing; no systemd, no apt), `--authorized-key FILE`, `--hostname NAME`, `--yes`.
1. Preflight: root, Debian 12 arm64 (warn otherwise), disk ≥ 500 MB, DNS/HTTPS to
   `repo.unipi.technology`, `github.com`, `pypi.org`.
2. Apt prerequisites: `python3-venv python3-pip git ca-certificates`.
3. Unipi repo + kernel/firmware (Unipi's `raspberry-neuron.sh`): the script is downloaded to a
   file, its sha256 printed and the human asked to confirm (unless `--yes`) — never piped into
   a root shell blindly. If the running kernel is not the Unipi kernel afterwards: exit code 10
   "reboot, then run me again".
4. `apt-get install evok`; apply `etc-evok/config.yaml` from the site bundle (old one saved as
   `.bootstrap-orig`); restart evok; wait until `GET :8080/rest/all` contains `device_info`.
5. Release: unpack tag into `releases/<tag>`, venv by requirements hash (offline `--wheelhouse DIR`
   optional), flip `current`.
6. Site: import bundle into `/etc/unipi-ha` + `/var/lib/unipi-ha`; run `--check-config`.
7. systemd unit from the release, `enable --now`, `tools/healthcheck.py --fresh`.
8. Print a summary: versions, what changed, the **change-the-default-password reminder**,
   and how to roll back (`tools/rollback.sh <tag>`).
Optional hardening flags (`--authorized-key`, `--harden-ssh`) are explicit; `--harden-ssh`
refuses to run unless a working key login was verified.

# Site bundle (`tools/site-bundle.sh`, T05)
`export <name> [--out FILE]` from a running box: `config.json`, `local_rules.json`,
`legacy_map.json`, `local_rules_state.json`, `etc-evok/config.yaml`, hostname, and a
`site.json` manifest (role, source version, created, sha256 of every file); mode 600.
`import FILE [--force]` verifies the manifest/checksums, refuses to overwrite without `--force`
(existing files are backed up first). A clean rebuild of any SD card = flash BaseOS → bootstrap
with the last exported bundle.

# Deploy & rollback after bootstrap
`tools/deploy.sh <tag>` and `rollback.sh` detect the layout: in release mode a deploy unpacks the
new release, builds the venv if needed, flips `current`, restarts, runs the fresh health check and
**flips the symlink back automatically** on failure (seconds, no file copying).

# Consequences
- The L513 rebuild (T31) is scripted and repeatable; a lost SD card is a 20-minute job.
- The first install on real hardware happens in the maintenance window; everything except
  Evok/hardware is tested beforehand (T06) in an arm64 Debian 12 container and in a temp
  prefix on the S103.
- Another thing to keep current: the bootstrap must track Unipi's repo/script changes (pinned
  checks + the verification in T06 on every BaseOS bump).
- Open risk: the BaseOS image may differ from the S103's Debian (e.g. newer Debian). Python
  3.11 (bookworm) is assumed; T30 verifies the real image and `requirements.txt` wheels.
