"""T20: the legacy-traffic recorder must stay strictly read-only and lossless."""
import ast
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import mqtt_capture  # noqa: E402


def test_record_keeps_topic_payload_retain_and_time():
    r = mqtt_capture.record("unipi1/bgg/hal/bel", b'{"circuit": "2_02", "dev": "relay", "state": "off"}', True, now=1700000000.12345)
    assert r == {"ts": 1700000000.123, "topic": "unipi1/bgg/hal/bel", "retain": True, "payload": '{"circuit": "2_02", "dev": "relay", "state": "off"}'}
    assert json.loads(json.dumps(r)) == r


def test_binary_payloads_are_not_lost():
    assert mqtt_capture.record("t", b"\xff\x00\x10", False, now=1)["payload"] == "hex:ff0010"


def test_the_recorder_can_never_publish():
    tree = ast.parse((ROOT / "tools" / "mqtt_capture.py").read_text())
    calls = {n.func.attr for n in ast.walk(tree) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)}
    assert not calls & {"publish", "will_set"}                                        # subscribe-only: it can not disturb the live system


def test_default_roots_cover_all_three_legacy_prefixes():
    assert set(mqtt_capture.DEFAULT_ROOTS) == {"unipi/#", "unipi1/#", "unipi2/#"}
