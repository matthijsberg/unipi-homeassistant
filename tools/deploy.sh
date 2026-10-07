#!/usr/bin/env bash
# Deploy a tagged version to this Unipi: backup -> preflight -> install -> restart -> health check
# -> automatic rollback if unhealthy. The only supported way to change code on a live box.
#   tools/deploy.sh <tag> [--no-rollback] [--dry-run]
# Never touches: config.json, local_rules.json, legacy_map.json, .device_name (house data).
# Env overrides: UNIPI_RUNTIME UNIPI_VENV UNIPI_SERVICE UNIPI_BACKUPS
set -euo pipefail

tag="${1:?usage: deploy.sh <tag> [--no-rollback] [--dry-run]}"; shift || true
auto_rollback=1; dry=0
for a in "$@"; do case "$a" in --no-rollback) auto_rollback=0;; --dry-run) dry=1;; *) echo "unknown option $a" >&2; exit 2;; esac; done

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNTIME="${UNIPI_RUNTIME:-/home/unipi/unipi-homeassistant/scripts}"
VENV="${UNIPI_VENV:-/home/unipi/unipi-homeassistant}"
SERVICE="${UNIPI_SERVICE:-hass-unipi}"
CODE_FILES=(hass-unipi.py requirements.txt web unipi_core legacy_adapter.py)   # what a deploy may replace
log() { echo "[deploy $tag] $*"; }
run() { if [ "$dry" = 1 ]; then echo "  (dry-run) $*"; else "$@"; fi; }

# --- preconditions -------------------------------------------------------------------------
cd "$REPO"
git rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null || { echo "tag $tag not found (git fetch --tags?)" >&2; exit 2; }
git diff --quiet HEAD -- . ':!docs' || { echo "working tree has uncommitted changes to tracked files; commit or stash first" >&2; exit 2; }
[ -d "$RUNTIME" ] && [ -x "$VENV/bin/python" ] || { echo "runtime/venv not found ($RUNTIME, $VENV)" >&2; exit 2; }

stage="$(mktemp -d)"; trap 'rm -rf -- "$stage"' EXIT
paths=()
for f in "${CODE_FILES[@]}"; do git cat-file -e "$tag:$f" 2>/dev/null && paths+=("$f"); done   # only paths that exist in this tag
[[ " ${paths[*]} " == *" hass-unipi.py "* ]] || { echo "tag $tag has no hass-unipi.py" >&2; exit 2; }
git archive "$tag" -- "${paths[@]}" | tar -x -C "$stage"
new_version="$(sed -n 's/^SCRIPT_VERSION *= *"\([^"]*\)".*/\1/p' "$stage/hass-unipi.py" | head -1)"
[ -n "$new_version" ] || { echo "cannot read SCRIPT_VERSION from $tag" >&2; exit 2; }
"$VENV/bin/python" -m py_compile "$stage/hass-unipi.py" || { echo "PREFLIGHT FAILED: hass-unipi.py does not compile; nothing was changed" >&2; exit 1; }
[ -d "$stage/unipi_core" ] && "$VENV/bin/python" -m compileall -q "$stage/unipi_core" >/dev/null
[ -f "$stage/legacy_adapter.py" ] && "$VENV/bin/python" -m py_compile "$stage/legacy_adapter.py"
log "preflight ok (script version $new_version)"

# --- backup --------------------------------------------------------------------------------
if [ "$dry" = 1 ]; then log "(dry-run) would take backup pre-$tag"; backup="(dry-run)"
else backup="$("$REPO/tools/backup.sh" "pre-$tag" | tail -1)" || true; [ -f "$backup/SHA256SUMS" ] || { echo "backup failed; aborting before any change" >&2; exit 1; }
fi
log "backup: $backup"

# --- install -------------------------------------------------------------------------------
restarts_before="$(systemctl show "$SERVICE" -pNRestarts --value 2>/dev/null || echo 0)"
if [ -f "$stage/requirements.txt" ] && ! cmp -s "$stage/requirements.txt" "$RUNTIME/requirements.txt" 2>/dev/null; then
  log "requirements.txt changed -> pip install"; run "$VENV/bin/pip" install -q -r "$stage/requirements.txt"
fi
for f in "${CODE_FILES[@]}"; do
  [ -e "$stage/$f" ] || { [ -e "$RUNTIME/$f" ] && [ "$f" != hass-unipi.py ] && [ "$f" != requirements.txt ] && log "note: $f exists in runtime but not in $tag (left in place)"; continue; }
  if [ -d "$stage/$f" ]; then run rm -rf "$RUNTIME/$f.new"; run cp -a "$stage/$f" "$RUNTIME/$f.new"; run rm -rf "$RUNTIME/$f"; run mv "$RUNTIME/$f.new" "$RUNTIME/$f"
  else run cp -a "$stage/$f" "$RUNTIME/$f.new"; run mv -f "$RUNTIME/$f.new" "$RUNTIME/$f"; fi
done
run bash -c "echo '$tag' > '$RUNTIME/.deployed_tag'"

# --- restart + verify ------------------------------------------------------------------------
log "restarting $SERVICE"; run sudo -n systemctl restart "$SERVICE"
if [ "$dry" = 1 ]; then log "(dry-run) done"; exit 0; fi
if "$VENV/bin/python" "$REPO/tools/healthcheck.py" --fresh --timeout 120 --restarts-before "$restarts_before" --expect-version "$new_version" --runtime "$RUNTIME" --service "$SERVICE"; then
  log "SUCCESS"; exit 0
fi
log "HEALTH CHECK FAILED"
if [ "$auto_rollback" = 1 ]; then
  log "rolling back to backup $backup"
  "$REPO/tools/rollback.sh" "$backup" || { log "ROLLBACK ALSO FAILED - manual intervention needed (backup: $backup)"; exit 3; }
  log "rolled back; service restored"
fi
exit 1
