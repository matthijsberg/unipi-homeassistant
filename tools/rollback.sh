#!/usr/bin/env bash
# Return this Unipi to a known-good version.
#   tools/rollback.sh <tag>             deploy that tag (same as deploy.sh, no auto-rollback loop)
#   tools/rollback.sh <backup-dir>      restore CODE from that backup's runtime.tgz
#   add --with-config to also restore config.json / local_rules.json from the backup
set -euo pipefail
target="${1:?usage: rollback.sh <tag|backup-dir> [--with-config]}"; with_config=0
[ "${2:-}" = "--with-config" ] && with_config=1
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="${UNIPI_RUNTIME:-/home/unipi/unipi-homeassistant/scripts}"
VENV="${UNIPI_VENV:-/home/unipi/unipi-homeassistant}"
SERVICE="${UNIPI_SERVICE:-hass-unipi}"

if [ ! -d "$target" ]; then exec "$REPO/tools/deploy.sh" "$target" --no-rollback; fi

tgz="$target/runtime.tgz"; [ -f "$tgz" ] || { echo "no runtime.tgz in $target" >&2; exit 2; }
( cd "$target" && sha256sum -c --quiet SHA256SUMS ) || { echo "backup checksum mismatch - refusing to restore" >&2; exit 2; }
base="$(basename "$RUNTIME")"
members=("$base/hass-unipi.py" "$base/requirements.txt" "$base/web" "$base/unipi_core" "$base/legacy_adapter.py" "$base/.deployed_tag")
[ "$with_config" = 1 ] && members+=("$base/config.json" "$base/local_rules.json" "$base/legacy_map.json")
# only extract members that exist in the archive
avail="$(tar -tzf "$tgz")"; extract=()
for m in "${members[@]}"; do grep -qE "^${m}(/|$)" <<<"$avail" && extract+=("$m"); done
echo "[rollback] restoring: ${extract[*]}"
sudo -n systemctl stop "$SERVICE"
tar -xzf "$tgz" -C "$(dirname "$RUNTIME")" "${extract[@]}"
# a backup taken before any tooled deploy has no .deployed_tag: don't leave the failed release's marker behind
grep -qE "^${base}/\.deployed_tag$" <<<"$avail" || rm -f -- "$RUNTIME/.deployed_tag"
sudo -n systemctl start "$SERVICE"
"$VENV/bin/python" "$REPO/tools/healthcheck.py" --fresh --timeout 120 --runtime "$RUNTIME" --service "$SERVICE"
