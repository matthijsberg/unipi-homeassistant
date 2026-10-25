#!/usr/bin/env bash
# Release-layout deploy (ADR-006): unpack tag -> venv -> check the live config with the NEW code -> safety copy of the
# site files -> atomic flip of `current` -> restart -> fresh health check -> automatic flip back if unhealthy.
# Called by deploy.sh when $UNIPI_PREFIX/opt/unipi-ha/current is a symlink. Old releases stay on disk (rollback is a flip).
#   tools/deploy-release.sh <tag> [--no-rollback] [--dry-run]
# Env: UNIPI_PREFIX (tests), UNIPI_SYSTEMCTL (default "sudo -n systemctl"), UNIPI_HEALTHCHECK (command), UNIPI_BACKUPS,
#      UNIPI_WHEELHOUSE, UNIPI_SERVICE
set -euo pipefail
tag="${1:?usage: deploy-release.sh <tag> [--no-rollback] [--dry-run]}"; shift || true
auto_rollback=1; dry=0
for a in "$@"; do case "$a" in --no-rollback) auto_rollback=0;; --dry-run) dry=1;; *) echo "unknown option $a" >&2; exit 2;; esac; done

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; REPO="$(cd "$HERE/.." && pwd)"
. "$HERE/lib-release.sh"
PREFIX="${UNIPI_PREFIX:-}"; OPT="$PREFIX/opt/unipi-ha"; ETC="$PREFIX/etc/unipi-ha"; VAR="$PREFIX/var/lib/unipi-ha"
SERVICE="${UNIPI_SERVICE:-hass-unipi}"; BACKUPS="${UNIPI_BACKUPS:-$HOME/backups}"; WHEEL="${UNIPI_WHEELHOUSE:-}"
SYSTEMCTL=(${UNIPI_SYSTEMCTL:-sudo -n systemctl})
HC="${UNIPI_HEALTHCHECK:-$OPT/current/.venv/bin/python $OPT/current/tools/healthcheck.py}"
log() { echo "[deploy $tag] $*"; }

version_of() { sed -n 's/^SCRIPT_VERSION *= *"\([^"]*\)".*/\1/p' "$OPT/releases/$1/hass-unipi.py" | head -1; }
healthy() {   # $1 = expected version
  $HC --fresh --timeout 150 --config "$ETC/config.json" --state-dir "$VAR" --runtime "$OPT/current" --expect-version "$1"
}

prev="$(release_current_tag "$OPT")"
[ -n "$prev" ] || { echo "no current release in $OPT" >&2; exit 2; }
git -C "$REPO" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null || { echo "tag $tag not found (git fetch --tags?)" >&2; exit 2; }
if [ "$dry" = 1 ]; then
  log "(dry-run) current is $prev; would unpack $tag, build/reuse venv, --check-config with the live config, save a site bundle, flip, restart, health-check, flip back on failure"
  exit 0
fi

# --- prepare (nothing live changes until the flip) ---------------------------------------------------
r="$(release_unpack "$REPO" "$tag" "$OPT")" || exit 2
log "release $tag: $r"
v="$(release_venv "$OPT" "$tag" "$WHEEL")" || { echo "PREFLIGHT FAILED: cannot build the python environment; nothing was changed" >&2; exit 1; }
new_version="$(version_of "$tag")"; [ -n "$new_version" ] || { echo "cannot read SCRIPT_VERSION from $tag" >&2; exit 2; }
py="$OPT/releases/$tag/.venv/bin/python"
"$py" -m py_compile "$OPT/releases/$tag/hass-unipi.py" || { echo "PREFLIGHT FAILED: does not compile; nothing was changed" >&2; exit 1; }
check="$(UNIPI_HA_STATE_DIR="$VAR" "$py" "$OPT/releases/$tag/hass-unipi.py" --check-config --config "$ETC/config.json" 2>&1 | tail -1)" || true
case "$check" in OK*) log "preflight ok ($new_version): ${check:0:120}";;
  *) echo "PREFLIGHT FAILED: $tag cannot start with the live configuration: $check; nothing was changed" >&2; exit 1;; esac

# --- safety copy of everything machine-specific (the code of the old release stays on disk) ----------
mkdir -p "$BACKUPS"; chmod 700 "$BACKUPS" 2>/dev/null || true
site="$BACKUPS/$(date -u +%Y%m%dT%H%M%SZ)-pre-$tag.site.tgz"
python3 "$HERE/site_bundle.py" export --prefix "${PREFIX:-/}" --out "$site" >/dev/null && log "site files saved: $site" || log "WARNING: could not save the site files"

# --- switch ------------------------------------------------------------------------------------------
release_flip "$OPT" "$tag" >/dev/null; log "current -> $tag (was $prev)"
log "restarting $SERVICE"; "${SYSTEMCTL[@]}" restart "$SERVICE"
if healthy "$new_version"; then
  release_prune "$OPT" 5; log "SUCCESS"; exit 0
fi
log "HEALTH CHECK FAILED"
if [ "$auto_rollback" = 1 ] && [ "$prev" != "$tag" ]; then
  log "flipping back to $prev"
  release_flip "$OPT" "$prev" >/dev/null; "${SYSTEMCTL[@]}" restart "$SERVICE"
  if healthy "$(version_of "$prev")"; then log "rolled back to $prev; service restored"; else log "ROLLBACK ALSO FAILED - manual intervention needed (previous release: $prev)"; exit 3; fi
fi
exit 1
