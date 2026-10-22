#!/usr/bin/env bash
# Shared by bootstrap.sh, deploy.sh and rollback.sh: the release layout of ADR-006.
#   $OPT/releases/<tag>/        immutable code of one tag (+ .venv -> ../../venvs/<hash>, VERSION)
#   $OPT/venvs/<hash>/          python venv shared by releases with the same requirements + python
#   $OPT/current -> releases/<tag>   (relative, atomic flip)      $OPT/previous  (text: the tag before)
# All functions take the layout root ($OPT) explicitly so tests can run them in a temp tree.
RELEASE_PATHS=(hass-unipi.py requirements.txt web unipi_core legacy_adapter.py tools systemd)

release_current_tag() {   # $1=OPT -> tag or empty
  local l; l="$(readlink "$1/current" 2>/dev/null)" || return 0; basename "$l"
}

release_unpack() {        # $1=git repo, $2=tag, $3=OPT. Idempotent: an existing complete release is kept as is.
  local repo="$1" tag="$2" opt="$3" dest="$3/releases/$2" tmp paths=() f
  [ -f "$dest/VERSION" ] && { echo "unchanged"; return 0; }
  git -C "$repo" rev-parse -q --verify "refs/tags/$tag^{commit}" >/dev/null || { echo "tag $tag not found in $repo" >&2; return 2; }
  for f in "${RELEASE_PATHS[@]}"; do git -C "$repo" cat-file -e "$tag:$f" 2>/dev/null && paths+=("$f"); done
  [[ " ${paths[*]} " == *" hass-unipi.py "* ]] || { echo "tag $tag has no hass-unipi.py" >&2; return 2; }
  mkdir -p "$opt/releases"; tmp="$opt/releases/.$tag.tmp"; rm -rf -- "$tmp"; mkdir -p "$tmp"
  git -C "$repo" archive "$tag" -- "${paths[@]}" | tar -x -C "$tmp"
  echo "$tag" > "$tmp/VERSION"
  rm -rf -- "$dest"; mv -T "$tmp" "$dest"
  echo "created"
}

release_venv() {          # $1=OPT, $2=tag, [$3=wheelhouse]. Prints "created <hash>" or "unchanged <hash>".
  local opt="$1" tag="$2" wheel="${3:-}" rel="$1/releases/$2" h state="created" pipargs=()
  h="$( { sha256sum < "$rel/requirements.txt"; python3 -c 'import sys; print(sys.version_info[:2])'; } | sha256sum | cut -c1-8 )"
  if [ -x "$opt/venvs/$h/bin/python" ] && [ -f "$opt/venvs/$h/.complete" ]; then state="unchanged"
  else
    rm -rf -- "$opt/venvs/$h.tmp"; mkdir -p "$opt/venvs"
    # shellcheck disable=SC2086
    python3 -m venv ${UNIPI_VENV_FLAGS:-} "$opt/venvs/$h.tmp" >&2
    [ -n "$wheel" ] && pipargs=(--no-index --find-links "$wheel")
    if grep -qvE '^\s*(#|$)' "$rel/requirements.txt"; then      # nothing to install for a comment-only file
      "$opt/venvs/$h.tmp/bin/pip" install -q "${pipargs[@]}" -r "$rel/requirements.txt" >&2
    fi
    touch "$opt/venvs/$h.tmp/.complete"
    rm -rf -- "$opt/venvs/$h"; mv -T "$opt/venvs/$h.tmp" "$opt/venvs/$h"
  fi
  ln -sfn "../../venvs/$h" "$rel/.venv"
  echo "$state $h"
}

release_flip() {          # $1=OPT, $2=tag: atomic switch of `current`, remembering the one before in `previous`
  local opt="$1" tag="$2" old
  [ -d "$opt/releases/$tag" ] || { echo "release $tag is not unpacked" >&2; return 2; }
  old="$(release_current_tag "$opt")"
  [ "$old" = "$tag" ] && { echo "unchanged"; return 0; }
  [ -n "$old" ] && echo "$old" > "$opt/previous"
  ln -sfn "releases/$tag" "$opt/current.new" && mv -T "$opt/current.new" "$opt/current"
  echo "flipped"
}

release_prune() {         # $1=OPT, [$2=keep, default 5]: delete old releases, never current/previous; then unused venvs
  local opt="$1" keep="${2:-5}" cur prev d t n=0
  cur="$(release_current_tag "$opt")"; prev="$(cat "$opt/previous" 2>/dev/null || true)"
  for d in $(ls -1dt "$opt"/releases/*/ 2>/dev/null); do
    t="$(basename "$d")"; n=$((n+1))
    [ "$t" = "$cur" ] || [ "$t" = "$prev" ] || [ "$n" -le "$keep" ] || rm -rf -- "$d"
  done
  for d in "$opt"/venvs/*/; do
    [ -d "$d" ] || continue
    ls "$opt"/releases/*/.venv >/dev/null 2>&1 && for l in "$opt"/releases/*/.venv; do [ "$(basename "$(readlink "$l")")" = "$(basename "$d")" ] && continue 2; done
    rm -rf -- "$d"
  done
}
