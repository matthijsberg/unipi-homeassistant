"""Which software is this box running? (OS, kernel, evok, Unipi/Pi firmware packages, pending updates)

Collected with a few short, read-only commands (dpkg-query, apt list) and published as diagnostic sensors so Home
Assistant shows the versions of every Unipi in one place. Nothing here installs or changes anything. Every command
has a timeout and a failure just leaves that item out: a missing `dpkg` (not Debian) or a slow apt must never
disturb the bridge.

The *package* version of the Unipi firmware (`unipi-firmware6`) is what apt put on disk; installing it does NOT flash the
boards (the package says so and asks for a manual `sudo /opt/unipi/tools/fwspi --auto -v`). The firmware that is really
RUNNING in the controller boards is read separately: holding register 1000 of every board on the local Modbus-TCP
endpoint (unipitcp), high byte = major, low byte = minor (verified on an M203: 1814 = 0x0716 = 7.22, identical to what
fwspi reported after flashing). A plain read, nothing is written.
"""
from __future__ import annotations

import os
import platform
import re
import socket
import struct
import subprocess
import time
from typing import Any, Callable

Runner = Callable[[list[str]], str]

# key -> (name shown in HA, icon)
ITEMS = {
    "board_firmware_text": ("Board firmware (running)", "mdi:chip"),
    "board_firmware_status": ("Board firmware status", "mdi:memory"),
    "auto_updates": ("Automatic security updates", "mdi:shield-refresh"),
    "bridge": ("Bridge version", "mdi:tag"),
    "os": ("Operating system", "mdi:linux"),
    "kernel": ("Kernel", "mdi:chip"),
    "python": ("Python", "mdi:language-python"),
    "evok": ("Evok version", "mdi:tag-outline"),
    "unipi_firmware": ("Unipi firmware package", "mdi:memory"),
    "pi_firmware": ("Raspberry Pi firmware package", "mdi:raspberry-pi"),
}
PACKAGES = {"evok": "evok", "unipi_firmware": "unipi-firmware6", "pi_firmware": "raspi-firmware"}


def default_runner(cmd: list[str]) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False).stdout


def package_version(pkg: str, run: Runner) -> str | None:
    try:
        out = run(["dpkg-query", "-W", "-f=${Status}|${Version}", pkg]).strip()
    except Exception:
        return None
    status, _, version = out.partition("|")
    return version if version and status.endswith("installed") else None


def parse_upgradable(text: str) -> list[dict[str, str]]:
    """Lines of `apt list --upgradable`: 'pkg/suite new-version arch [upgradable from: old]'."""
    out = []
    for line in text.splitlines():
        m = re.match(r"^(?P<name>[^/\s]+)/\S+\s+(?P<new>\S+)\s+\S+(?:\s+\[upgradable from:\s*(?P<old>[^\]]+)\])?", line)
        if m:
            out.append({"name": m["name"], "new": m["new"], "old": (m["old"] or "").strip()})
    return out


def os_pretty_name(path: str = "/etc/os-release") -> str | None:
    try:
        with open(path) as f:
            for line in f:
                if line.startswith("PRETTY_NAME="):
                    return line.split("=", 1)[1].strip().strip('"')
    except OSError:
        pass
    return None


def apt_lists_age_days(path: str = "/var/lib/apt/lists") -> float | None:
    """How old the package lists are: 'no updates' only means 'none known as of this date'."""
    try:
        newest = max(os.path.getmtime(os.path.join(path, f)) for f in os.listdir(path) if f.endswith("_Packages") or f.endswith("Release"))
        return round((time.time() - newest) / 86400, 1)
    except (OSError, ValueError):
        return None


def evok_modbus_endpoint(path: str = "/etc/evok/autogen.yaml") -> tuple[str, int, list[int]]:
    """(host, port, board unit ids) of the Neuron's local Modbus-TCP channel, from evok's generated config."""
    host, port, units = "127.0.0.1", 502, [1]
    try:
        text = open(path).read()
        block = text.split("LOCAL_TCP:", 1)[1]
        block = re.split(r"\n  [A-Za-z_]+:", block, maxsplit=1)[0]
        m = re.search(r"hostname:\s*(\S+)", block); host = m.group(1) if m else host
        m = re.search(r"port:\s*(\d+)", block); port = int(m.group(1)) if m else port
        found = [int(x) for x in re.findall(r"slave-id:\s*(\d+)", block)]
        units = found or units
    except (OSError, IndexError):
        pass
    return host, port, units


def modbus_read_register(host: str, port: int, unit: int, register: int, timeout: float = 2.0) -> int | None:
    """One Modbus-TCP 'read holding registers' (function 3), count 1. Read-only. None on any problem."""
    req = struct.pack(">HHHBBHH", 1, 0, 6, unit, 3, register, 1)

    def recv_exact(sk: socket.socket, n: int) -> bytes | None:
        buf = b""
        while len(buf) < n:
            chunk = sk.recv(n - len(buf))
            if not chunk:
                return None
            buf += chunk
        return buf

    try:
        with socket.create_connection((host, port), timeout=timeout) as sk:
            sk.settimeout(timeout)
            sk.sendall(req)
            head = recv_exact(sk, 9)                    # MBAP(7) + function + byte count / exception code
            if head is None:
                return None
            _tid, _proto, _length, rsp_unit, func, nbytes = struct.unpack(">HHHBBB", head)
            if func != 3 or nbytes != 2 or rsp_unit != unit:    # exception response (func 0x83) or garbage: stop at once
                return None
            data = recv_exact(sk, 2)
    except OSError:
        return None
    return struct.unpack(">H", data)[0] if data else None


def read_board_firmware(host: str | None = None, port: int | None = None, units: list[int] | None = None,
                        reader=modbus_read_register) -> dict[int, str]:
    """Running firmware of each Neuron board: {unit: '7.22'}. Boards that do not answer are left out."""
    h, p, u = evok_modbus_endpoint()
    host, port, units = host or h, port or p, units or u
    out: dict[int, str] = {}
    for unit in units:
        v = reader(host, port, unit, 1000)
        if v:
            out[unit] = f"{v >> 8}.{v & 0xFF}"
    return out


def version_tuple(v: str | None) -> tuple[int, ...] | None:
    """'7.22' / '7.18.2' / '1:1.2-3' -> numeric tuple of the leading dotted number; None if there is none."""
    m = re.match(r"^(?:\d+:)?(\d+(?:\.\d+)*)", v or "")
    return tuple(int(x) for x in m.group(1).split(".")) if m else None


def board_firmware_text(running: dict[int, str]) -> str | None:
    """'7.22' when every board runs the same version, else 'board 1: 7.22, board 2: 7.6'."""
    if not running:
        return None
    if len(set(running.values())) == 1:
        return next(iter(running.values()))
    return ", ".join(f"board {u}: {v}" for u, v in sorted(running.items()))


def firmware_update_count(info: dict[str, Any]) -> int | None:
    """Pending firmware updates: firmware packages apt would upgrade + boards that run older firmware than the package ships."""
    ups = info.get("updates")
    if ups is None:
        return None
    pkgs = sum(1 for u in ups if u["name"].startswith(("unipi-firmware", "raspi-firmware")))
    return pkgs + (1 if info.get("board_firmware_status") == "update available" else 0)


def board_firmware_status(running: dict[int, str], package: str | None) -> str:
    """'current', 'update available' (a board runs older firmware than the installed package ships) or 'unknown'."""
    pkg = version_tuple(package)
    if not running or pkg is None:
        return "unknown"
    pkg2 = pkg[:2]
    older = [u for u, v in running.items() if (version_tuple(v) or (0,))[:2] < pkg2]
    return "update available" if older else "current"


def auto_security_updates(run: Runner = default_runner) -> str | None:
    """'enabled' when apt's periodic unattended-upgrade is switched on, else 'disabled' (None if apt-config is unavailable)."""
    try:
        out = run(["apt-config", "dump", "APT::Periodic::Unattended-Upgrade"])
    except Exception:
        return None
    m = re.search(r'Unattended-Upgrade\s+"(\d+)"', out)
    return "enabled" if m and int(m.group(1)) > 0 else "disabled"


def collect(bridge_version: str, run: Runner = default_runner, board_reader: Callable[[], dict[int, str]] | None = None) -> dict[str, Any]:
    info: dict[str, Any] = {"bridge": bridge_version, "os": os_pretty_name(), "kernel": platform.release(),
                            "python": platform.python_version()}
    for key, pkg in PACKAGES.items():
        info[key] = package_version(pkg, run)
    try:
        upgradable = parse_upgradable(run(["apt", "list", "--upgradable"]))
    except Exception:
        upgradable = None
    info["updates"] = upgradable
    info["board_firmware"] = (board_reader or read_board_firmware)() or None          # {unit: "7.22"}: what RUNS in the boards
    info["board_firmware_text"] = board_firmware_text(info["board_firmware"] or {})
    info["board_firmware_status"] = board_firmware_status(info["board_firmware"] or {}, info.get("unipi_firmware"))
    info["auto_updates"] = auto_security_updates(run)
    info["apt_lists_age_days"] = apt_lists_age_days()
    return info


def updates_attributes(info: dict[str, Any]) -> dict[str, Any]:
    ups = info.get("updates")
    if ups is None:
        return {}
    firmware = [u for u in ups if u["name"].startswith(("unipi-firmware", "raspi-firmware"))]
    board = info.get("board_firmware") or {}
    return {"board_firmware_running": {f"board {u}": v for u, v in sorted(board.items())},
            "board_firmware_package": info.get("unipi_firmware"),
            "how_to_flash": "sudo /opt/unipi/tools/fwspi --auto -v  (then reboot; never interrupt the power)" if info.get("board_firmware_status") == "update available" else None,
            "packages": [f"{u['name']} {u['old']} -> {u['new']}" if u["old"] else f"{u['name']} {u['new']}" for u in ups][:50],
            "firmware_updates": [f"{u['name']} {u['old']} -> {u['new']}" for u in firmware],
            "package_lists_age_days": info.get("apt_lists_age_days")}
