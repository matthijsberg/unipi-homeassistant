---
type: Task
id: T02
title: "Import the live bridge into GitHub as baseline v2.0.0, with secret guard"
description: "GitHub main contains exactly the live S103 code (2026092501) plus this knowledge bundle, example config and a pre-commit secret guard; tag v2.0.0 exists."
phase: 0
task_status: done
depends_on: [T00, T01]
risk: low
human_gate: false
target_hosts: [dev-only]
tags: [git, baseline]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Objective
GitHub `main` (2025-03-29) is far behind the live script (K10). Establish a baseline that is
byte-identical to what runs, so every later change is a reviewable diff and `v2.0.0` is a
rollback point.

# Read first
- [/conventions/elephant-goldfish.md](/conventions/elephant-goldfish.md)
- [/decisions/ADR-004-versioning-and-backups.md](/decisions/ADR-004-versioning-and-backups.md)
- [/analysis/known-issues.md](/analysis/known-issues.md) (K10, K11, K13)

# Preconditions
- T00 done (`gh auth status` OK). T01 done (baseline backup exists).
- `sha256sum /home/unipi/unipi-homeassistant/scripts/hass-unipi.py` →
  `dd4afeda…3161`. If different: the live file changed since this plan — STOP and
  update `/context/code-map.md` first.
- `~/src/unipi-homeassistant` exists (clone made 2026-10-07, branch `plan/legacy-migration`
  holds this bundle, uncommitted).

# Files in scope
In `~/src/unipi-homeassistant`: `hass-unipi.py`, `web/index.html`, `requirements.txt`,
`hass-unipi.service`, `cleanup_ghosts.py`, `config.example.json`, `.gitignore`,
`tools/git-hooks/pre-commit`, `README.md` (section only), `legacy/old_unipi_mqtt/*`,
`docs/knowledge/**`. **Not** `config.json` (remove it from the index if tracked).

# Backup
`git -C ~/src/unipi-homeassistant stash list` empty or noted; nothing live is touched.

# Steps
0. ~~**🔒 HUMAN decision — the repo is public.**~~ **DECIDED 2026-10-07: placeholders + private overlay (the original options follow for the record).** This bundle contains house details (internal
   IPs, MQTT user names, which inputs are door locks/contacts, room layout). Choose one and
   record it in ADR-004: (a) push the bundle as-is to the public repo; (b) redact
   house-specific values in `/context/*` (IPs → `<l513-ip>`, room names kept or not) before
   pushing; (c) keep `docs/knowledge/` in a separate **private** repo (git submodule or
   sibling clone) and only push code publicly. Default if undecided: **(c)**; do not push the
   bundle publicly until decided.
1. Commit the knowledge bundle on `plan/legacy-migration` (or to the private repo per step 0): `docs: add OKF knowledge bundle and migration plan`. Push, open PR, **🔒 HUMAN** review + merge (this also marks the plan as reviewed — add `verified` to the concepts Matthijs approved).
2. New branch `task/T02-baseline` from `main`.
3. Copy live files from `/home/unipi/unipi-homeassistant/scripts/`: `hass-unipi.py`,
   `web/index.html`, `requirements.txt`, `hass-unipi.service`, `cleanup_ghosts.py`. Do not
   copy backups, `*.bak*`, `connection_test*`, `traffic_log.json`, `.device_name`,
   `config.json`, `local_rules.json`.
4. `git rm --cached config.json` (keep the placeholder content as `config.example.json`,
   updated to the current schema: mqtt, websocket, unipi_http, logging, extensions,
   web_server, inputs — placeholders only).
5. `.gitignore`: add `config.json`, `local_rules.json`, `legacy_map.json`, `.device_name`,
   `traffic_log*.json`, `*.log`, `backups/`, `__pycache__/`, `.pytest_cache/`.
6. Secret guard: **already created and active** (plan-bootstrap commit): `tools/pii_scan.py`, `tools/git-hooks/{pre-commit,pre-push}`, extended `.gitignore`. This step is now only: verify on every clone that `git config core.hooksPath` = `tools/git-hooks`, and that `~/.config/unipi-pii/sources.txt` lists the local config files. (Original spec, for the record: `tools/git-hooks/pre-commit` (bash): reads every string value of keys
   matching `password|token|secret` from the *local* `config.json` files it finds
   (runtime dir + repo dir) and fails if any staged diff contains one; also fails on the
   regex `mqtt_pass\s*=\s*"[^"<]`. Install: `git config core.hooksPath tools/git-hooks`.
   Document in README.
7. Old script as reference: copy `/home/unipi/scripts/old_unipi_mqtt/*` (or the live L513
   versions from T01 if they differ) to `legacy/old_unipi_mqtt/`, **replace the
   `mqtt_pass` value with `"<redacted>"`** and the MQTT user with `"<user>"`. The old config
   JSON contains no secrets but does contain house layout — **🔒 HUMAN** decide: commit it,
   or keep it out (then only keep it in backups). Default: keep it out, commit
   `unipi_mqtt_config.example.json` with 3 representative entries.
8. Commit `T02: import live bridge 2026092501 as baseline`. Push, PR, merge.
9. Tag `v2.0.0` on that merge commit, `git push origin v2.0.0`, create a GitHub Release
   "v2.0.0 — live baseline (2026092501)".

# Acceptance checks
- `git show v2.0.0:hass-unipi.py | sha256sum` = live sha256.
- `git ls-files | grep -E '^config.json$|local_rules.json|traffic_log'` → empty.
- Staging a file containing the live MQTT password makes `git commit` fail (test, then unstage).
- `git log --all -p | grep -c '<the live password>'` → 0 (run locally, do not paste the password into Evidence).

# Rollback
Delete the tag/release and revert the merge commit; nothing live was touched.

# Feed the elephant
`/log.md`; `/context/code-map.md` sources → add `resource` to the tagged file on GitHub.

# Evidence
- 2026-10-07: PR #2 merged, tag `v2.0.0` + GitHub Release. `git show v2.0.0:hass-unipi.py | sha256sum` = `dd4afedae96d…` = live.
- No `config.json`/`local_rules`/`traffic_log` tracked; pre-commit blocked a planted password; both live MQTT passwords absent from `git log --all -p`.
- Deviations: `cleanup_ghosts.py` NOT imported (hard-coded broker credentials; kept in the baseline backup; T41 builds a credential-free tool). Old config JSON not committed (default); example with 3 neutral entries instead. `pii_scan.py` allow-lists `10.255.255.255` (routing probe in the live script) so the baseline stays byte-identical.
- Note: the repo root still has the 2025 `unipi_mqtt.py` and `connection_test.py` (pre-existing, unrelated to live code) — candidates for removal in a later cleanup.

# Open questions
