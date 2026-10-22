#!/usr/bin/env python3
"""Site bundle: everything machine-specific about one Unipi, as one private archive (ADR-006).

  site-bundle.sh export [--out FILE] [--prefix DIR] [--legacy-runtime DIR]
  site-bundle.sh list   FILE
  site-bundle.sh import FILE [--prefix DIR] [--force] [--only unipi|evok] [--owner USER] [--dry-run]

The bundle CONTAINS PASSWORDS (MQTT, web users' rules, ...). It is written with mode 600 and must never go into git
(`*.site.tgz` is git-ignored and refused by pii_scan.py). Keep it on the Mac/NAS.

Contents (see manifest.json inside; every member has a sha256 that import verifies):
  etc-unipi-ha/        config.json  local_rules.json  legacy_map.json            -> <prefix>/etc/unipi-ha
  var-lib-unipi-ha/    .device_name  local_rules_state.json                      -> <prefix>/var/lib/unipi-ha
  etc-evok/config.yaml                                                           -> <prefix>/etc/evok/config.yaml
Export reads the release layout if present, else the classic S103 runtime dir (config files next to the script).
Import never overwrites without --force; with --force the replaced file is kept as <name>.pre-import-<timestamp>.
"""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import pwd
import shutil
import socket
import sys
import tarfile
import time
from pathlib import Path

VERSION = 1
GROUP_UNIPI = {"etc-unipi-ha": "etc/unipi-ha", "var-lib-unipi-ha": "var/lib/unipi-ha"}
GROUP_EVOK = {"etc-evok": "etc/evok"}
# member name -> (bundle dir, secret?)
ETC_FILES = ("config.json", "local_rules.json", "legacy_map.json")
VAR_FILES = (".device_name", "local_rules_state.json")


def sha256(p: Path) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def die(msg: str, code: int = 2):
    print(f"site-bundle: {msg}", file=sys.stderr)
    sys.exit(code)


# --------------------------------------------------------------------------------------------------------- export
def sources(prefix: Path, legacy_runtime: Path) -> tuple[str, dict[str, Path]]:
    """bundle member -> source file. Release layout wins; otherwise the classic runtime dir."""
    found: dict[str, Path] = {}
    etc, var = prefix / "etc/unipi-ha", prefix / "var/lib/unipi-ha"
    if (etc / "config.json").exists():
        layout = "release"
        for n in ETC_FILES:
            if (etc / n).exists():
                found[f"etc-unipi-ha/{n}"] = etc / n
        for n in VAR_FILES:
            if (var / n).exists():
                found[f"var-lib-unipi-ha/{n}"] = var / n
    elif (legacy_runtime / "config.json").exists():
        layout = "classic"
        for n in ETC_FILES:
            if (legacy_runtime / n).exists():
                found[f"etc-unipi-ha/{n}"] = legacy_runtime / n
        for n in VAR_FILES:
            if (legacy_runtime / n).exists():
                found[f"var-lib-unipi-ha/{n}"] = legacy_runtime / n
    else:
        die(f"no config.json found in {etc} or {legacy_runtime}: nothing to export")
    evok = prefix / "etc/evok/config.yaml"
    if evok.exists():
        found["etc-evok/config.yaml"] = evok
    return layout, found


def cmd_export(a) -> None:
    prefix, legacy = Path(a.prefix), Path(a.legacy_runtime)
    layout, found = sources(prefix, legacy)
    out = Path(a.out or f"{socket.gethostname()}-{time.strftime('%Y%m%d')}.site.tgz")
    if out.exists() and not a.force:
        die(f"{out} exists (use --force to replace it)")
    files = [{"path": m, "sha256": sha256(src), "size": src.stat().st_size} for m, src in sorted(found.items())]
    manifest = {"bundle_version": VERSION, "created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "host": socket.gethostname(), "source_layout": layout, "files": files}
    old = os.umask(0o077)
    try:
        with tarfile.open(out, "w:gz") as tf:
            data = json.dumps(manifest, indent=2).encode()
            ti = tarfile.TarInfo("manifest.json"); ti.size = len(data); ti.mode = 0o600; ti.mtime = int(time.time())
            tf.addfile(ti, io.BytesIO(data))
            for m, src in sorted(found.items()):
                ti = tf.gettarinfo(str(src), arcname=m)
                ti.uid = ti.gid = 0; ti.uname = ti.gname = ""; ti.mode = 0o600
                with open(src, "rb") as f:
                    tf.addfile(ti, f)
    finally:
        os.umask(old)
    os.chmod(out, 0o600)
    print(f"site-bundle: wrote {out} ({layout} layout, {len(files)} files) - contains passwords, keep it private")
    for f in files:
        print(f"  {f['path']}")


# --------------------------------------------------------------------------------------------------------- read/verify
def read_bundle(path: Path) -> tuple[dict, dict[str, bytes]]:
    if not path.exists():
        die(f"{path} not found")
    members: dict[str, bytes] = {}
    try:
        with tarfile.open(path, "r:gz") as tf:
            for ti in tf.getmembers():
                n = ti.name
                if not ti.isfile() or n.startswith("/") or ".." in Path(n).parts:
                    die(f"refusing unsafe member in bundle: {n!r}")
                members[n] = tf.extractfile(ti).read()
    except (tarfile.TarError, OSError, EOFError) as e:
        die(f"{path} is not a readable bundle: {e}")
    if "manifest.json" not in members:
        die("bundle has no manifest.json")
    try:
        manifest = json.loads(members.pop("manifest.json"))
    except ValueError:
        die("manifest.json is not valid JSON")
    if manifest.get("bundle_version") != VERSION:
        die(f"unsupported bundle_version {manifest.get('bundle_version')!r}")
    listed = {f["path"]: f["sha256"] for f in manifest.get("files", [])}
    if set(listed) != set(members):
        die(f"manifest and archive disagree: only in manifest {sorted(set(listed) - set(members))}, only in archive {sorted(set(members) - set(listed))}")
    for n, data in members.items():
        if hashlib.sha256(data).hexdigest() != listed[n]:
            die(f"CHECKSUM MISMATCH for {n}: the bundle is damaged or was changed")
    return manifest, members


def cmd_list(a) -> None:
    manifest, members = read_bundle(Path(a.file))
    print(f"bundle from {manifest['host']} created {manifest['created']} ({manifest['source_layout']} layout), all checksums OK")
    for n in sorted(members):
        print(f"  {n}  ({len(members[n])} bytes)")


# --------------------------------------------------------------------------------------------------------- import
def target_for(prefix: Path, member: str) -> Path | None:
    top, _, name = member.partition("/")
    if top in GROUP_UNIPI or top in GROUP_EVOK:
        return prefix / {**GROUP_UNIPI, **GROUP_EVOK}[top] / name
    return None


def owner_ids(name: str | None) -> tuple[int, int] | None:
    try:
        if name:
            pw = pwd.getpwnam(name)
        elif os.geteuid() == 0:
            pw = pwd.getpwnam("unipi")
        else:
            return None
        return pw.pw_uid, pw.pw_gid
    except KeyError:
        if name:
            die(f"user {name!r} does not exist")
        return None


def cmd_import(a) -> None:
    prefix = Path(a.prefix)
    manifest, members = read_bundle(Path(a.file))
    groups = {"unipi": GROUP_UNIPI, "evok": GROUP_EVOK, None: {**GROUP_UNIPI, **GROUP_EVOK}}[a.only]
    todo = [(m, target_for(prefix, m)) for m in sorted(members) if m.partition("/")[0] in groups]
    ids = owner_ids(a.owner)
    clashes = [t for m, t in todo if t.exists() and sha256(t) != hashlib.sha256(members[m]).hexdigest()]
    if clashes and not a.force:
        die("would overwrite existing files (use --force; replaced files are kept as .pre-import-*):\n  " + "\n  ".join(map(str, clashes)))
    stamp = time.strftime("%Y%m%dT%H%M%S")
    for m, t in todo:
        same = t.exists() and sha256(t) == hashlib.sha256(members[m]).hexdigest()
        verb = "unchanged" if same else ("replace" if t.exists() else "create")
        print(f"  {verb:9} {t}")
        if a.dry_run or same:
            continue
        t.parent.mkdir(parents=True, exist_ok=True)
        if t.parent != prefix and ids:
            os.chown(t.parent, *ids)
        if t.exists():
            shutil.copy2(t, t.with_name(t.name + f".pre-import-{stamp}"))
        tmp = t.with_name(t.name + ".tmp-import")
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as f:
            f.write(members[m])
        # evok's own config is world-readable by convention; ours holds passwords
        os.chmod(tmp, 0o644 if m.startswith("etc-evok/") else 0o600)
        if ids and not m.startswith("etc-evok/"):
            os.chown(tmp, *ids)
        os.replace(tmp, t)
    if a.dry_run:
        print("site-bundle: dry run, nothing written")
    else:
        print(f"site-bundle: imported {len(todo)} file(s) from {manifest['host']} ({manifest['created']})")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("export", "list", "import"):
        sp = sub.add_parser(name)
        if name != "export":
            sp.add_argument("file")
        sp.add_argument("--prefix", default=os.environ.get("UNIPI_PREFIX", "/"))
        sp.add_argument("--force", action="store_true")
        if name == "export":
            sp.add_argument("--out")
            sp.add_argument("--legacy-runtime", default=os.environ.get("UNIPI_RUNTIME", "/home/unipi/unipi-homeassistant/scripts"))
        if name == "import":
            sp.add_argument("--only", choices=("unipi", "evok"))
            sp.add_argument("--owner")
            sp.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()
    {"export": cmd_export, "list": cmd_list, "import": cmd_import}[a.cmd](a)


if __name__ == "__main__":
    main()
