#!/usr/bin/env bash
# Take a timestamped, checksum-verified backup of the bridge runtime before any change.
#   tools/backup.sh <label>        prints the backup directory path on the last line
#
# Contains secrets (config.json): the folder is mode 700 and must never go into git.
# Optional off-box copy: config.json -> "backup": {"offbox_targets": ["user@host:path", ...]}
# Overridable via env: UNIPI_RUNTIME, UNIPI_VENV, UNIPI_BACKUPS, UNIPI_SERVICE_FILE
set -euo pipefail

label="${1:?usage: backup.sh <label>}"
[[ "$label" =~ ^[A-Za-z0-9._-]+$ ]] || { echo "label may only contain A-Z a-z 0-9 . _ -" >&2; exit 2; }

RUNTIME="${UNIPI_RUNTIME:-/home/unipi/unipi-homeassistant/scripts}"
VENV="${UNIPI_VENV:-/home/unipi/unipi-homeassistant}"
ROOT="${UNIPI_BACKUPS:-$HOME/backups}"
SERVICE_FILE="${UNIPI_SERVICE_FILE:-/etc/systemd/system/hass-unipi.service}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

[ -d "$RUNTIME" ] || { echo "runtime dir not found: $RUNTIME" >&2; exit 2; }
umask 077
ts="$(date -u +%Y%m%dT%H%M%SZ)"
dest="$ROOT/$ts-$label"
mkdir -p "$dest"
chmod 700 "$ROOT" "$dest"

# 1. runtime code + config (not old backups, caches)
tar -C "$(dirname "$RUNTIME")" -czf "$dest/runtime.tgz" \
  --exclude="$(basename "$RUNTIME")/backups" --exclude='__pycache__' --exclude='.pytest_cache' \
  "$(basename "$RUNTIME")"
# 2. service unit, evok config, python env, versions
[ -f "$SERVICE_FILE" ] && cp "$SERVICE_FILE" "$dest/"
if sudo -n true 2>/dev/null; then
  sudo -n tar -czf "$dest/etc-evok.tgz" /etc/evok* 2>/dev/null && sudo -n chown "$(id -u):$(id -g)" "$dest/etc-evok.tgz" || true
fi
[ -x "$VENV/bin/pip" ] && "$VENV/bin/pip" freeze > "$dest/pip-freeze.txt" 2>/dev/null || true
dpkg -l 2>/dev/null | grep -iE 'evok|unipi' > "$dest/dpkg-unipi.txt" || true
git -C "$REPO" rev-parse HEAD > "$dest/git-head.txt" 2>/dev/null || true
[ -f "$RUNTIME/hass-unipi.py" ] && sha256sum "$RUNTIME/hass-unipi.py" > "$dest/live-script.sha256"
[ -f "$RUNTIME/.deployed_tag" ] && cp "$RUNTIME/.deployed_tag" "$dest/"

# 3. checksums, then verify what we just wrote
chmod 600 "$dest"/* 2>/dev/null || true
( cd "$dest" && sha256sum $(ls | grep -v '^SHA256SUMS$') > SHA256SUMS && sha256sum -c --quiet SHA256SUMS )

# 4. optional off-box copies (a backup only on the box being changed does not count)
offbox_failed=0
if [ -f "$RUNTIME/config.json" ]; then
  mapfile -t targets < <(python3 - "$RUNTIME/config.json" <<'EOF'
import json, sys
try:
    for t in json.load(open(sys.argv[1])).get("backup", {}).get("offbox_targets", []):
        print(t)
except Exception:
    pass
EOF
)
  for t in "${targets[@]:-}"; do
    [ -n "$t" ] || continue
    if scp -q -o BatchMode=yes -o ConnectTimeout=10 -r "$dest" "$t/"; then
      echo "off-box copy ok: ${t%%:*}" >&2
    else
      echo "WARNING: off-box copy FAILED for ${t%%:*} (local backup kept)" >&2; offbox_failed=1
    fi
  done
  [ "${#targets[@]}" -gt 0 ] && [ -n "${targets[0]:-}" ] || echo "NOTE: no backup.offbox_targets configured - this backup exists only on this box" >&2
fi

# 5. retention: keep the newest 30 plus every pre-v* / pre-T3* backup
mapfile -t old < <(ls -1d "$ROOT"/*/ 2>/dev/null | sed 's:/$::' | sort | head -n -30)
for d in "${old[@]:-}"; do
  [ -n "$d" ] || continue
  b="$(basename "$d")"
  [[ "$b" =~ ^[0-9]{8}T[0-9]{6}Z-[A-Za-z0-9._-]+$ ]] || continue   # only our own folders, never anything else
  case "$b" in *-pre-v*|*-pre-T3*|*-baseline-*) ;; *) rm -rf -- "$ROOT/$b" ;; esac
done

echo "$dest"
exit "$offbox_failed"
