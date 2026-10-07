#!/usr/bin/env python3
"""PII / secret scanner used by the git pre-commit and pre-push hooks.

Scans only ADDED lines (staged diff, a commit range, or whole files) for:
  1. forbidden file names (house config, logs, traffic captures, ...)
  2. generic patterns: tokens, private keys, e-mail, IPv4 addresses, MAC addresses,
     password-like assignments
  3. local denylist: literal secret/identifying values harvested at scan time from
     the local config files listed in ~/.config/unipi-pii/sources.txt (one path per
     line). The values are never written anywhere.

Escape hatch for a line that is intentionally fine: end it with a comment marker `pii-ok`
(`# pii-ok`, `// pii-ok`, `<!-- pii-ok -->`); a mere mention in prose does NOT count,
e.g.  `url = "http://192.168.1.1"  # pii-ok`.  Placeholders in <angle-brackets>,
127.0.0.1, 0.0.0.0 and example.* are always allowed.

Usage:
  pii_scan.py --staged              # pre-commit
  pii_scan.py --range A..B          # pre-push (commits being pushed)
  pii_scan.py --paths FILE...       # scan working-tree files (report mode)
Exit code 0 = clean, 1 = findings, 2 = scanner error.
"""
from __future__ import annotations

import argparse
import fnmatch
import json
import os
import re
import subprocess
import sys

FORBIDDEN_FILES = [
    "config.json", "config.*.json", "local_rules.json", "local_rules_state.json",
    "legacy_map.json", ".device_name", ".discovered_presets",
    "traffic_log*", "*.log", "*.bak*", "*.pem", "*.key", "id_*", ".env", ".env.*",
    "hosts.yml",
]
FORBIDDEN_ALLOW = ["config.example.json", "*.example.json", "*.example"]

PATTERNS = [
    ("github-token", re.compile(r"\b(github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})\b")),
    ("private-key", re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")),
    ("aws-key", re.compile(r"\bAKIA[0-9A-Z]{16}\b")),
    ("email", re.compile(r"\b[A-Za-z0-9._%+-]+@(?!example\.)[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")),
    ("ipv4", re.compile(r"\b(?!127\.0\.0\.1\b|0\.0\.0\.0\b|255\.255\.255\.)(?:\d{1,3}\.){3}\d{1,3}\b(?![~\w-]|\.\d)")),
    ("mac", re.compile(r"\b(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}\b")),
    ("password-assign", re.compile(
        r"""(?ix)\b(pass(word|wd)?|secret|token|api[_-]?key|mqtt_pass)\b["']?\s*[:=]\s*["'](?!<|\$\{|\s*["'])[^"'\s]{4,}["']""")),
]
# 1-wire / serial style identifiers are house-specific but not secret; not flagged.
PII_OK = re.compile(r"(?:#|//|;|<!--)\s*pii-ok\b[^\n]*$")  # only as a trailing comment marker
DENY_STOPLIST = {"none", "null", "true", "false", "unipi", "unipi1", "localhost", "admin"}
# Non-identifying addresses (routing probe targets, documentation ranges).
SAFE_IPS = {"10.255.255.255"}
VERSION_CONTEXT = re.compile(r"(?i)\b(evok|version|ver|sw|v)\s*[:=]?\s*$")
SAFE_VALUE = re.compile(r"<[^>]+>|example\.|your_|changeme|\*{3}")


def run(*cmd: str) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def harvest_denylist() -> set[str]:
    """Literal values from local config files that must never appear in a commit."""
    vals: set[str] = set()
    src_file = os.path.expanduser("~/.config/unipi-pii/sources.txt")
    paths = []
    if os.path.exists(src_file):
        paths = [p.strip() for p in open(src_file) if p.strip() and not p.startswith("#")]
    paths += [p for p in os.environ.get("PII_SOURCES", "").split(":") if p]
    key_re = re.compile(r"pass|secret|token|user|broker|host|serial|sn$", re.I)

    def walk(o, key=""):
        if isinstance(o, dict):
            for k, v in o.items():
                walk(v, k)
        elif isinstance(o, list):
            for v in o:
                walk(v, key)
        elif isinstance(o, str) and key_re.search(key) and len(o) >= 4 and not SAFE_VALUE.search(o):
            vals.add(o)

    for p in paths:
        p = os.path.expanduser(p)
        try:
            text = open(p, encoding="utf-8", errors="ignore").read()
        except OSError:
            continue
        try:
            walk(json.loads(text))
        except ValueError:
            for m in re.finditer(r"""(?m)^\s*(mqtt_(?:pass|user|address)|ws_(?:server|user|pass))\s*=\s*["']([^"']{4,})["']""", text):
                vals.add(m.group(2))
    vals.update(v for v in os.environ.get("PII_DENY", "").split(",") if v)
    # Generic words/topic roots that are not identifying and would only create noise.
    return {v for v in vals if v.lower() not in DENY_STOPLIST}


def added_lines_from_diff(diff: str):
    path, lineno = None, 0
    for line in diff.splitlines():
        if line.startswith("+++ "):
            path = line[6:] if line.startswith("+++ b/") else None
        elif line.startswith("@@"):
            m = re.search(r"\+(\d+)", line)
            lineno = int(m.group(1)) - 1 if m else 0
        elif line.startswith("+") and not line.startswith("+++"):
            lineno += 1
            if path:
                yield path, lineno, line[1:]
        elif not line.startswith("-"):
            lineno += 1


def forbidden(path: str) -> bool:
    base = os.path.basename(path)
    if any(fnmatch.fnmatch(base, a) for a in FORBIDDEN_ALLOW):
        return False
    return any(fnmatch.fnmatch(base, f) for f in FORBIDDEN_FILES)


def scan_line(path, lineno, text, deny):
    if PII_OK.search(text):
        return []
    out = []
    for d in deny:
        if d in text:
            out.append((path, lineno, "denylist", "<local secret/identifier, value hidden>"))
    for name, rx in PATTERNS:
        for m in rx.finditer(text):
            if SAFE_VALUE.search(m.group(0)):
                continue
            if name == "ipv4" and m.group(0) in SAFE_IPS:
                continue
            if name == "ipv4" and VERSION_CONTEXT.search(text[: m.start()]):
                continue  # "evok 3.0.6.1", "version: 1.2.3.4" are versions, not addresses
            out.append((path, lineno, name, m.group(0)[:60]))
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--staged", action="store_true")
    g.add_argument("--range")
    g.add_argument("--paths", nargs="+")
    ap.add_argument("--summary", action="store_true", help="only print counts per category/file")
    args = ap.parse_args()

    try:
        deny = harvest_denylist()
        findings = []
        files = []
        if args.staged:
            files = run("git", "diff", "--cached", "--name-only", "--diff-filter=ACMR").split()
            diff = run("git", "diff", "--cached", "-U0", "--no-color")
            lines = list(added_lines_from_diff(diff))
        elif args.range:
            files = run("git", "diff", "--name-only", "--diff-filter=ACMR", args.range).split()
            diff = run("git", "diff", "-U0", "--no-color", args.range)
            lines = list(added_lines_from_diff(diff))
        else:
            files = args.paths
            lines = []
            for p in args.paths:
                try:
                    for i, t in enumerate(open(p, encoding="utf-8", errors="ignore"), 1):
                        lines.append((p, i, t.rstrip("\n")))
                except OSError:
                    pass
        for f in files:
            if forbidden(f):
                findings.append((f, 0, "forbidden-file", "file must never be committed"))
        for path, lineno, text in lines:
            findings += scan_line(path, lineno, text, deny)
    except Exception as e:  # scanner problems must not silently pass
        print(f"pii_scan: ERROR {e}", file=sys.stderr)
        return 2

    if not findings:
        print(f"pii_scan: clean ({len(lines)} lines, {len(deny)} local denylist values)")
        return 0
    if args.summary:
        from collections import Counter
        c = Counter((f[2], f[0]) for f in findings)
        for (cat, path), n in sorted(c.items()):
            print(f"{cat:16} {n:4}  {path}")
        print(f"total findings: {len(findings)}")
    else:
        for path, lineno, cat, snippet in findings:
            print(f"{path}:{lineno}: [{cat}] {snippet}")
        print(f"\npii_scan: {len(findings)} finding(s). Fix them, or add `pii-ok` to an intentional line.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
