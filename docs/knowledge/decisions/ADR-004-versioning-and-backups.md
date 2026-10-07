---
type: Decision
title: "ADR-004 Git/GitHub versioning, release tags and always-on backups"
description: "Code lives in the existing public GitHub repo with semver tags per phase; house-specific config never enters git and is backed up locally and off-box before every change."
tags: [adr, git, backup, rollback]
status: draft
generated: { by: claude-code/claude-opus-5-5, at: 2026-10-07T09:30:00Z }
---

# Decision
> **Amended 2026-10-07 by Matthijs:** on-device backups (`tools/backup.sh`) plus Git/GitHub versioning of everything we build are sufficient; automated off-box backup is not required. Config survives an SD loss via the private site bundle ([ADR-006](/decisions/ADR-006-baseos-deployment.md)).

- **Repo:** `github.com/matthijsberg/unipi-homeassistant` (public, chosen 2026-10-07).
  Working clone on each box: `~/src/unipi-homeassistant`. Runtime dir
  `/home/unipi/unipi-homeassistant/scripts` becomes a *deploy target only* (no editing there).
- **Branches:** `main` = what is deployed; `task/Txx-…` per task, merged by PR.
  `plan/…` for knowledge-bundle-only changes.
- **Versions:** semver tags + GitHub Releases. `SCRIPT_VERSION` in the code equals the tag
  (e.g. `2.2.0`); the old date string is kept as build metadata in the release notes.

  | Tag | Meaning |
  |---|---|
  | `v2.0.0` | Exact import of live `2026092501` (baseline, T02) |
  | `v2.1.0` | Tooling + tests, no behaviour change (T03, T04) |
  | `v2.2.0` | Core features G1–G20 behind options (T10–T18) |
  | `v2.3.0` | Legacy adapter (T21–T23) |
  | `v2.4.0` | L513 live on new bridge (T32) |
  | `v3.0.0` | Legacy adapter removed (**major**, T42) |

  Patch tags (`v2.2.1`) for fixes. A major bump = something users of the topics must change.
- **Never in git:** `config.json`, `local_rules.json`, `legacy_map.json` (house data),
  `.device_name`, logs, `traffic_log*.json`. Enforced by `.gitignore` + a pre-commit hook
  that scans staged files for the secret values present in the local `config.json`.
  `config.example.json` and `*.example.json` are committed instead.
- **PII/secret guard (decided 2026-10-07):** `.gitignore` + `tools/pii_scan.py` run from
  `tools/git-hooks/pre-commit` (staged diff) and `pre-push` (all pushed commits); activated per
  clone with `git config core.hooksPath tools/git-hooks`. It blocks forbidden file names,
  tokens/keys, e-mail, IPv4, MAC, password assignments, and literal secrets harvested at scan
  time from the local files listed in `~/.config/unipi-pii/sources.txt` (never committed).
  Knowledge bundle = placeholders; real values in gitignored `docs/knowledge/local/`.
  Commit author identity: Matthijs's own (chosen by Matthijs; see the private overlay). Docs never contain it.
- **Backups (`tools/backup.sh <label>`, T03):** before every deploy/config change, a
  timestamped tarball of the runtime dir (without old backups), `/etc/systemd/system/hass-unipi.service`,
  `/etc/evok*`, venv `pip freeze`, and the git commit id → `~/backups/` and copied off-box
  (the *other* Unipi via scp, plus Matthijs' NAS/PC if configured). Keep last 30 + all
  pre-release ones.
- **Big-bang backups:** full storage image of the L513 before T31 (old card stays as-is);
  HA full backup before any HA change (T20, T40, T41).
