# tools/

| Script | Purpose |
|---|---|
| `backup.sh <label>` | Timestamped, checksum-verified backup of the runtime (code + **secrets**), unit file, `/etc/evok*`, pip freeze. Optional off-box copy via `config.json` → `backup.offbox_targets`. Prints the path. |
| `deploy.sh <tag> [--dry-run] [--no-rollback]` | The only supported way to change code on a live box: preflight (tag exists, compiles) → backup → install code files → restart → health check → **automatic rollback** if unhealthy. Never touches `config.json`, `local_rules.json`, `legacy_map.json`, `.device_name`. |
| `rollback.sh <tag\|backup-dir> [--with-config]` | Redeploy a tag, or restore code from a backup (checksum-verified first). |
| `healthcheck.py [--fresh] [--timeout N]` | systemd active and not crash-looping, MQTT `status=online`, `startup_error=0`, version matches. `--fresh` = require live (non-retained) publishes, i.e. proof from *after* a restart. |
| `pii_scan.py`, `git-hooks/` | Commit/push guard (see root README). |

A deploy takes ~70 s: the bridge reports "no startup errors" ~60 s after start, and the health check waits for that fresh message.

Env overrides: `UNIPI_RUNTIME`, `UNIPI_VENV`, `UNIPI_SERVICE`, `UNIPI_BACKUPS`, `UNIPI_SERVICE_FILE`.
