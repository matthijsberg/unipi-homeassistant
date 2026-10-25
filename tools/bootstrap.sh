#!/usr/bin/env bash
# New Unipi BaseOS SD card -> working bridge, in one repeatable, idempotent command (ADR-006).
#
#   sudo tools/bootstrap.sh --site my-unipi.site.tgz [--tag v2.2.0] [--dry-run] [--yes]
#
# Options:
#   --site FILE            site bundle (tools/site-bundle.sh export) with this machine's config + evok config
#   --tag TAG              release to install (default: the tag checked out in this repo)
#   --dry-run              print exactly what would change, change nothing
#   --yes                  do not ask for confirmation (still prints the checksum of Unipi's script)
#   --skip-evok            do not touch the Unipi apt repo / evok (evok already installed and configured)
#   --prefix DIR           redirect /opt /etc /var into DIR and skip apt, evok, systemd (tests, CI, T06)
#   --wheelhouse DIR       install python packages offline from DIR
#   --hostname NAME        set the hostname           --authorized-key FILE   add this ssh public key for the user
#   --harden-ssh           password logins off; only with --authorized-key AND --key-login-verified
#   --source-repo DIR      take the release from this git repo (default: the repo this script is in, else a clone)
#   --repo-url URL         clone source when this script is not inside a git checkout
#   --unipi-script-url URL Unipi's repo script (default: https://repo.unipi.technology/debian/raspberry-neuron.sh)
#   --user NAME            service user (default unipi)
# Exit codes: 0 ok | 1 failed | 2 usage/preflight | 3 network | 10 reboot required, then run me again.
# Every step checks the real situation first ("check-then-act"); a second run changes nothing.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=lib-release.sh
. "$HERE/lib-release.sh"

SITE=""; TAG=""; DRY=0; YES=0; SKIP_EVOK=0; PREFIX=""; WHEEL=""; HOSTNAME_NEW=""; AUTH_KEY=""; HARDEN=0; KEY_VERIFIED=0
SRC=""; REPO_URL="https://github.com/matthijsberg/unipi-homeassistant.git"
UNIPI_SCRIPT_URL="https://repo.unipi.technology/debian/raspberry-neuron.sh"; SVC_USER="unipi"
while [ $# -gt 0 ]; do
  case "$1" in
    --site) SITE="$2"; shift;; --tag) TAG="$2"; shift;; --dry-run) DRY=1;; --yes) YES=1;; --skip-evok) SKIP_EVOK=1;;
    --prefix) PREFIX="${2%/}"; shift;; --wheelhouse) WHEEL="$2"; shift;; --hostname) HOSTNAME_NEW="$2"; shift;;
    --authorized-key) AUTH_KEY="$2"; shift;; --harden-ssh) HARDEN=1;; --key-login-verified) KEY_VERIFIED=1;;
    --source-repo) SRC="$2"; shift;; --repo-url) REPO_URL="$2"; shift;; --unipi-script-url) UNIPI_SCRIPT_URL="$2"; shift;;
    --user) SVC_USER="$2"; shift;; -h|--help) sed -n '2,24p' "$0"; exit 0;;
    *) echo "unknown option $1 (see --help)" >&2; exit 2;;
  esac; shift
done

LIVE=1; [ -n "$PREFIX" ] && LIVE=0
OPT="$PREFIX/opt/unipi-ha"; ETC="$PREFIX/etc/unipi-ha"; VAR="$PREFIX/var/lib/unipi-ha"
EVOK_CFG="$PREFIX/etc/evok/config.yaml"; UNIT_DST="$PREFIX/etc/systemd/system/hass-unipi.service"
STATE="$VAR/.bootstrap-state"
CHANGED=(); DONE=(); NEED_RESTART=0; STARTED_NOW=0

say()  { echo "[bootstrap] $*"; }
warn() { echo "[bootstrap] WARNING: $*" >&2; }
die()  { echo "[bootstrap] ERROR: $*" >&2; exit "${2:-1}"; }
# act: run a command, or only announce it in --dry-run
act()  { if [ "$DRY" = 1 ]; then echo "  WOULD: $*"; else "$@"; fi; }
changed() { if [ "$DRY" = 1 ]; then CHANGED+=("would: $1"); say "would: $1"; else CHANGED+=("$1"); say "$1"; fi; }
SYSTEMCTL=(${UNIPI_SYSTEMCTL:-systemctl})
owner() { [ "$LIVE" = 1 ] && [ "$DRY" = 0 ] && chown "$SVC_USER:$SVC_USER" "$@" 2>/dev/null || true; }

# ------------------------------------------------------------------------------------------------- 1. preflight
step_preflight() {
  say "step 1/8 preflight"
  if [ "$LIVE" = 1 ] && [ "$(id -u)" != 0 ] && [ "$DRY" = 0 ]; then die "run as root (sudo), or use --dry-run / --prefix" 2; fi
  if [ -r /etc/os-release ]; then
    . /etc/os-release; { [ "${ID:-}" = debian ] && { [ "${VERSION_ID:-}" = 12 ] || [ "${VERSION_ID:-}" = 13 ]; }; } || warn "expected Debian 12 or 13, found ${PRETTY_NAME:-unknown}"
  fi
  [ "$(uname -m)" = aarch64 ] || warn "expected arm64 (aarch64), found $(uname -m)"
  local d="$PREFIX/"; while [ ! -d "$d" ]; do d="$(dirname "$d")"; done
  local free; free="$(df -Pm "$d" | awk 'NR==2{print $4}')"
  local need="${UNIPI_MIN_FREE_MB:-500}"
  [ "${free:-0}" -ge "$need" ] || die "less than $need MB free on $d (${free} MB)" 2
  if [ "$LIVE" = 1 ] && [ -z "${UNIPI_SKIP_NET_CHECK:-}" ]; then
    # TCP only, in bash: a fresh image may have just python3-minimal (no urllib/http) until step 2 has run
    local hosts="github.com pypi.org"; [ "$SKIP_EVOK" = 1 ] || hosts="repo.unipi.technology $hosts"
    [ -n "$WHEEL" ] && hosts="repo.unipi.technology github.com"
    local h
    for h in $hosts; do
      timeout 10 bash -c "exec 3<>/dev/tcp/$h/443" 2>/dev/null || die "cannot reach $h:443 (DNS/network)" 3
    done
  fi
  # where does the release come from?
  if [ -z "$SRC" ]; then
    if git -C "$HERE/.." rev-parse --git-dir >/dev/null 2>&1; then SRC="$(git -C "$HERE/.." rev-parse --show-toplevel)"
    elif [ "$DRY" = 1 ]; then SRC="(clone of $REPO_URL)"
    else SRC="$(mktemp -d)"; git clone -q "$REPO_URL" "$SRC" || die "cannot clone $REPO_URL" 3; trap 'rm -rf -- "$SRC"' EXIT; fi
  fi
  if [ -z "$TAG" ]; then
    TAG="$(git -C "$SRC" describe --exact-match --tags HEAD 2>/dev/null || true)"
    [ -n "$TAG" ] || die "no --tag given and HEAD of $SRC is not a tagged commit" 2
  fi
  if [ -d "$SRC/.git" ] || git -C "$SRC" rev-parse --git-dir >/dev/null 2>&1; then
    git -C "$SRC" rev-parse -q --verify "refs/tags/$TAG^{commit}" >/dev/null || die "tag $TAG not found in $SRC (git fetch --tags?)" 2
  fi
  [ -z "$SITE" ] || [ -f "$SITE" ] || die "site bundle $SITE not found" 2
  # option combinations that must fail BEFORE anything is changed
  if [ "$HARDEN" = 1 ]; then
    [ -n "$AUTH_KEY" ] && [ "$KEY_VERIFIED" = 1 ] || die "--harden-ssh needs --authorized-key and --key-login-verified (you confirm that a key login works, or you lock yourself out)" 2
  fi
  [ -z "$AUTH_KEY" ] || [ -f "$AUTH_KEY" ] || die "public key file $AUTH_KEY not found" 2
  say "  release $TAG from $SRC; layout under ${PREFIX:-/}"
  DONE+=(preflight)
}

# ------------------------------------------------------------------------------------------------- 2. apt prerequisites
# The Unipi BaseOS ships python3-minimal only (no http, tarfile, venv ...): install the full interpreter first.
python_full() { python3 -c 'import http.client, tarfile, hashlib, json, venv, ensurepip' >/dev/null 2>&1; }

step_apt() {
  say "step 2/8 apt prerequisites"
  [ "$LIVE" = 1 ] || { say "  (prefix mode: skipped)"; return 0; }
  local missing=() p
  for p in python3 python3-venv python3-pip git ca-certificates; do dpkg -s "$p" >/dev/null 2>&1 || missing+=("$p"); done
  if [ "${#missing[@]}" -gt 0 ]; then
    if [ "$DRY" = 1 ]; then echo "  WOULD: apt-get update && apt-get install -y ${missing[*]}"
    else
      act apt-get update -q; act env DEBIAN_FRONTEND=noninteractive apt-get install -y -q "${missing[@]}"
    fi
    changed "installed packages: ${missing[*]}"
  fi
  DONE+=(apt)
}

# Checks that need a complete python: the site bundle must be intact BEFORE anything is changed beyond packages.
step_checks() {
  if [ -n "$SITE" ]; then
    if python_full; then
      python3 "$HERE/site_bundle.py" list "$SITE" >/dev/null || die "site bundle $SITE is damaged" 2
    elif [ "$DRY" = 1 ]; then say "  (dry run: python3 is not complete yet on this image, site bundle not verified)"
    else die "python3 is incomplete after the package step" 1; fi
  fi
}

# ------------------------------------------------------------------------------------------------- 3+4. Unipi repo, evok
step_evok() {
  say "step 3-4/8 Unipi repository, kernel/firmware and evok"
  if [ "$SKIP_EVOK" = 1 ]; then say "  (--skip-evok)"; fi
  if [ "$LIVE" = 1 ] && [ "$SKIP_EVOK" = 0 ]; then
    local cand; cand="$(apt-cache policy evok 2>/dev/null | awk '/Candidate:/{print $2}')"
    if { [ -z "$cand" ] || [ "$cand" = "(none)" ]; } && [ "$DRY" = 0 ]; then       # stale package lists on a fresh image?
      apt-get update -q >/dev/null 2>&1 || true
      cand="$(apt-cache policy evok 2>/dev/null | awk '/Candidate:/{print $2}')"
    fi
    if [ -z "$cand" ] || [ "$cand" = "(none)" ]; then
      local f; f="$(mktemp)"
      python3 - "$UNIPI_SCRIPT_URL" "$f" <<'PY' || die "cannot download $UNIPI_SCRIPT_URL" 3
import sys, urllib.request
open(sys.argv[2], "wb").write(urllib.request.urlopen(sys.argv[1], timeout=30).read())
PY
      say "  Unipi's repo script: $UNIPI_SCRIPT_URL"; say "  sha256 $(sha256sum "$f" | cut -d' ' -f1)  ($(wc -l < "$f") lines) - read it if you want to: $f"
      if [ "$DRY" = 1 ]; then echo "  WOULD: run it as root, then ask for a reboot (exit 10)"; rm -f "$f"
      else
        if [ "$YES" = 0 ]; then read -r -p "  Run this script as root now? [y/N] " a; [ "$a" = y ] || die "aborted by user" 2; fi
        bash "$f"; rm -f "$f"; changed "Unipi apt repository / kernel configured"
        persist_state; say "REBOOT REQUIRED: reboot, then run this command again."; exit 10
      fi
    fi
    if ! dpkg -s evok >/dev/null 2>&1; then act env DEBIAN_FRONTEND=noninteractive apt-get install -y -q evok; changed "installed evok"; fi
  fi
  # evok's own config (describes the extensions) comes from the site bundle
  if [ -n "$SITE" ] && [ "$DRY" = 1 ] && ! python_full; then
    say "  (dry run: cannot inspect the site bundle for an evok config yet)"
  elif [ -n "$SITE" ] && python3 "$HERE/site_bundle.py" list "$SITE" | grep -q 'etc-evok/config.yaml'; then
    local tmp; tmp="$(mktemp -d)"
    python3 "$HERE/site_bundle.py" import "$SITE" --prefix "$tmp" --only evok --force >/dev/null
    if ! cmp -s "$tmp/etc/evok/config.yaml" "$EVOK_CFG" 2>/dev/null; then
      if [ "$DRY" = 1 ]; then echo "  WOULD: replace $EVOK_CFG with the one from the site bundle (original kept as .bootstrap-orig)"
      else
        mkdir -p "$(dirname "$EVOK_CFG")"
        [ -f "$EVOK_CFG" ] && [ ! -f "$EVOK_CFG.bootstrap-orig" ] && cp -p "$EVOK_CFG" "$EVOK_CFG.bootstrap-orig"
        install -m 0644 "$tmp/etc/evok/config.yaml" "$EVOK_CFG"
        [ "$LIVE" = 1 ] && "${SYSTEMCTL[@]}" restart evok
        changed "evok config applied from the site bundle"
      fi
    fi
    rm -rf -- "$tmp"
  fi
  if [ "$LIVE" = 1 ] && [ "$DRY" = 0 ]; then
    say "  waiting for evok (REST on :8080)..."
    python3 - <<'PY' || die "evok did not come up with device_info within 90 s (check /etc/evok/config.yaml)" 1
import json, sys, time, urllib.request
end = time.time() + 90
while time.time() < end:
    try:
        d = json.load(urllib.request.urlopen("http://127.0.0.1:8080/rest/all", timeout=5))
        if any(x.get("dev") == "device_info" for x in d):
            sys.exit(0)
    except Exception:
        pass
    time.sleep(2)
sys.exit(1)
PY
  fi
  DONE+=(evok)
}

# ------------------------------------------------------------------------------------------------- 5. directories + release
step_release() {
  say "step 5/8 directories and release $TAG"
  local d
  for d in "$ETC" "$VAR" "$OPT"; do
    if [ ! -d "$d" ]; then act mkdir -p "$d"; changed "created $d"; fi
  done
  if [ "$DRY" = 0 ]; then chmod 750 "$ETC" "$VAR"; owner "$ETC" "$VAR"; fi
  if [ "$DRY" = 1 ]; then
    echo "  WOULD: unpack $TAG to $OPT/releases/$TAG, build/reuse a venv by requirements hash, point $OPT/current at it"; return 0
  fi
  local r; r="$(release_unpack "$SRC" "$TAG" "$OPT")" || die "cannot unpack $TAG" 1
  [ "$r" = created ] && changed "unpacked release $TAG"
  r="$(release_venv "$OPT" "$TAG" "$WHEEL")" || die "cannot create the python environment" 1
  [ "${r%% *}" = created ] && changed "python environment ${r#* } built"
  r="$(release_flip "$OPT" "$TAG")" || die "cannot switch to $TAG" 1
  [ "$r" = flipped ] && { changed "current -> $TAG"; NEED_RESTART=1; }
  [ "$LIVE" = 1 ] && chown -R "$SVC_USER:$SVC_USER" "$OPT" 2>/dev/null || true
  DONE+=(release)
}

# ------------------------------------------------------------------------------------------------- 6. site
step_site() {
  say "step 6/8 site configuration"
  if [ -n "$SITE" ]; then
    local own=() l; [ "$LIVE" = 1 ] && [ "$(id -u)" = 0 ] && own=(--owner "$SVC_USER")
    if [ "$DRY" = 1 ] && ! python_full; then say "  (dry run: would import the site bundle into $ETC and $VAR)"
    elif [ "$DRY" = 1 ]; then python3 "$HERE/site_bundle.py" import "$SITE" --prefix "${PREFIX:-/}" --only unipi --dry-run
    else
      while read -r l; do changed "site: $l"; NEED_RESTART=1; done < <(python3 "$HERE/site_bundle.py" import "$SITE" --prefix "${PREFIX:-/}" --only unipi "${own[@]}" | grep -E '^\s+(create|replace)' || true)
    fi
  elif [ ! -f "$ETC/config.json" ]; then
    warn "no --site bundle and no $ETC/config.json: copy config.example.json there, edit it, then run this again"
    NO_CONFIG=1; return 0
  fi
  if [ "$DRY" = 0 ]; then
    local out
    out="$("$OPT/current/.venv/bin/python" "$OPT/current/hass-unipi.py" --check-config --config "$ETC/config.json" 2>&1 | tail -1)" || true
    case "$out" in OK*) say "  $out";; *) die "the configuration does not pass --check-config: $out" 1;; esac
  fi
  DONE+=(site)
}

# ------------------------------------------------------------------------------------------------- 7. service
step_service() {
  say "step 7/8 systemd service"
  local unit_src="$OPT/current/systemd/hass-unipi.service"
  if [ "$DRY" = 1 ]; then echo "  WOULD: install hass-unipi.service, enable and start it, wait for a fresh healthy report"; return 0; fi
  [ -f "$unit_src" ] || die "release has no systemd/hass-unipi.service" 1
  if ! cmp -s "$unit_src" "$UNIT_DST" 2>/dev/null; then
    mkdir -p "$(dirname "$UNIT_DST")"; install -m 0644 "$unit_src" "$UNIT_DST"; changed "installed $UNIT_DST"; NEED_RESTART=1
    [ "$LIVE" = 1 ] && "${SYSTEMCTL[@]}" daemon-reload
  fi
  if [ "$LIVE" = 0 ]; then say "  (prefix mode: not enabling the service)"; return 0; fi
  if [ "${NO_CONFIG:-0}" = 1 ]; then say "  (no configuration yet: service not started)"; return 0; fi
  if ! "${SYSTEMCTL[@]}" is-enabled hass-unipi >/dev/null 2>&1; then "${SYSTEMCTL[@]}" enable hass-unipi >/dev/null 2>&1; changed "service enabled"; fi
  if ! "${SYSTEMCTL[@]}" is-active hass-unipi >/dev/null 2>&1; then
    "${SYSTEMCTL[@]}" start hass-unipi; changed "service started"; STARTED_NOW=1
  elif [ "$NEED_RESTART" = 1 ]; then
    "${SYSTEMCTL[@]}" restart hass-unipi; changed "service restarted (release/config/unit changed)"; STARTED_NOW=1
  fi
  # A restart produces fresh publishes (--fresh proves it). A service that was already running and untouched does not.
  local hc="${UNIPI_HEALTHCHECK:-$OPT/current/.venv/bin/python $OPT/current/tools/healthcheck.py}" fresh=()
  [ "$STARTED_NOW" = 1 ] && fresh=(--fresh)
  if $hc "${fresh[@]}" --timeout 150 --config "$ETC/config.json" --state-dir "$VAR" --runtime "$OPT/current"; then say "  healthy"
  else die "the service is not healthy (journalctl -u hass-unipi); rollback: tools/rollback.sh previous" 1; fi
  DONE+=(service)
}

# ------------------------------------------------------------------------------------------------- 7b. sudo for the service user
# deploy/rollback and the extension watchdog restart services as the service user (`sudo -n systemctl ...`).
# BaseOS asks for a password on every sudo, so allow exactly these commands without one.
step_sudoers() {
  [ "$LIVE" = 1 ] || return 0
  local f=/etc/sudoers.d/hass-unipi sc; sc="$(command -v systemctl)"
  local want; want="$(printf '%s ALL=(root) NOPASSWD: %s restart hass-unipi, %s restart evok\n' "$SVC_USER" "$sc" "$sc")"
  if [ "$(cat "$f" 2>/dev/null)" = "$want" ]; then return 0; fi
  if [ "$DRY" = 1 ]; then echo "  WOULD: allow $SVC_USER to run '$sc restart hass-unipi|evok' without a password ($f)"; return 0; fi
  local tmp; tmp="$(mktemp)"; printf '%s\n' "$want" > "$tmp"
  visudo -cf "$tmp" >/dev/null || { rm -f "$tmp"; die "generated sudoers rule is invalid; not installing it" 1; }
  install -m 0440 -o root -g root "$tmp" "$f"; rm -f "$tmp"; changed "sudo rule for service restarts installed ($f)"
}

# ------------------------------------------------------------------------------------------------- 8. optional extras
step_extras() {
  say "step 8/8 optional: hostname, ssh key, ssh hardening"
  if [ -n "$HOSTNAME_NEW" ] && [ "$LIVE" = 1 ] && [ "$(hostname)" != "$HOSTNAME_NEW" ]; then act hostnamectl set-hostname "$HOSTNAME_NEW"; changed "hostname -> $HOSTNAME_NEW"; fi
  if [ -n "$AUTH_KEY" ]; then
    [ -f "$AUTH_KEY" ] || die "public key file $AUTH_KEY not found" 2
    local home="$PREFIX/home/$SVC_USER"; [ "$LIVE" = 1 ] && home="$(getent passwd "$SVC_USER" | cut -d: -f6)"
    if ! grep -qxF "$(cat "$AUTH_KEY")" "$home/.ssh/authorized_keys" 2>/dev/null; then
      act mkdir -p "$home/.ssh"; if [ "$DRY" = 0 ]; then cat "$AUTH_KEY" >> "$home/.ssh/authorized_keys"; chmod 700 "$home/.ssh"; chmod 600 "$home/.ssh/authorized_keys"; owner "$home/.ssh" "$home/.ssh/authorized_keys"; fi
      changed "ssh key added for $SVC_USER"
    fi
  fi
  if [ "$HARDEN" = 1 ]; then
    [ -n "$AUTH_KEY" ] && [ "$KEY_VERIFIED" = 1 ] || die "--harden-ssh needs --authorized-key and --key-login-verified (you confirm that a key login works, or you lock yourself out)" 2
    local f="$PREFIX/etc/ssh/sshd_config.d/50-unipi-ha.conf"
    if ! grep -qs '^PasswordAuthentication no' "$f"; then
      act mkdir -p "$(dirname "$f")"; [ "$DRY" = 1 ] || printf 'PasswordAuthentication no\n' > "$f"; [ "$LIVE" = 1 ] && act "${SYSTEMCTL[@]}" reload ssh || true
      changed "ssh password logins disabled"
    fi
  fi
}

# ------------------------------------------------------------------------------------------------- state + summary
persist_state() {
  [ "$DRY" = 1 ] && return 0
  mkdir -p "$VAR"
  local tmp; tmp="$(mktemp)"
  { echo "release=$TAG"; [ "${#DONE[@]}" -eq 0 ] || for s in $(printf '%s\n' "${DONE[@]}" | sort -u); do echo "done.$s=1"; done; } > "$tmp"
  cmp -s "$tmp" "$STATE" 2>/dev/null && rm -f "$tmp" || { mv "$tmp" "$STATE"; chmod 640 "$STATE"; }   # unchanged content = untouched file
}

summary() {
  echo; say "==== summary ===="
  local sv; sv="$(sed -n 's/^SCRIPT_VERSION *= *"\([^"]*\)".*/\1/p' "$OPT/current/hass-unipi.py" 2>/dev/null | head -1 || true)"
  say "release: ${TAG}${sv:+ (bridge $sv)}"
  [ "$LIVE" = 1 ] && say "evok:    $(dpkg-query -W -f='${Version}' evok 2>/dev/null || echo 'not installed')"
  if [ "${#CHANGED[@]}" -eq 0 ]; then say "changes: none (already up to date)"; else say "changes:"; printf '  - %s\n' "${CHANGED[@]}"; fi
  [ "$DRY" = 1 ] && say "(dry run: nothing was changed)"
  if [ "$LIVE" = 1 ] && [ "$DRY" = 0 ]; then
    say "REMINDER: change the default password of user '$SVC_USER' (passwd) if you have not yet."
    say "Roll back to the previous release: tools/rollback.sh previous      Export this box's config: tools/site-bundle.sh export"
  fi
}

step_preflight; step_apt; step_checks; step_evok; step_release; step_site; step_service; step_sudoers; step_extras
persist_state
summary
