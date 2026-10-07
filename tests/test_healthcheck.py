"""Regression tests for tools/healthcheck.py (the deploy safety net)."""
import importlib.util
import subprocess
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parent.parent


def load():
    spec = importlib.util.spec_from_file_location("healthcheck", ROOT / "tools" / "healthcheck.py")
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)
    return m


def test_sysd_returns_values_in_requested_order(monkeypatch):
    """systemctl prints NRestarts before ActiveState regardless of -p order; we must not care."""
    hc = load()
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="NRestarts=9\nActiveState=activating\n"))
    assert hc.sysd("x", "ActiveState", "NRestarts") == ["activating", "9"]
    assert hc.sysd("x", "NRestarts", "ActiveState") == ["9", "activating"]


def test_sysd_missing_property_is_empty_string(monkeypatch):
    hc = load()
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: SimpleNamespace(stdout="ActiveState=active\n"))
    assert hc.sysd("x", "ActiveState", "NRestarts") == ["active", ""]
