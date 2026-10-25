#!/usr/bin/env python3
"""Read-only MQTT recorder for the legacy contract (T20): who publishes what, with which payload, how often.

  mqtt_capture.py --config /etc/unipi-ha/config.json --out capture.jsonl [--roots unipi/# unipi1/# unipi2/#] [--hours 24]

Subscribes (QoS 0) and NEVER publishes. One JSON object per line: {"ts": <epoch>, "topic": ..., "retain": bool, "payload": ...}.
Credentials are taken from the `mqtt` section of a bridge config.json (broker, port, username, password) and are never printed.
The recording contains household behaviour (motion, doors, locks): keep it OUT of git. A trimmed, anonymised copy is made later
for the test fixtures.
"""
from __future__ import annotations

import argparse
import json
import os
import signal
import sys
import time

import paho.mqtt.client as mqtt

DEFAULT_ROOTS = ["unipi/#", "unipi1/#", "unipi2/#"]


def record(topic: str, payload: bytes, retain: bool, now: float | None = None) -> dict:
    """The JSON line for one message. Payloads that are not UTF-8 are kept as hex so nothing is lost."""
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError:
        text = "hex:" + payload.hex()
    return {"ts": round(now if now is not None else time.time(), 3), "topic": topic, "retain": bool(retain), "payload": text}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True, help="bridge config.json (mqtt section is used)")
    ap.add_argument("--out", required=True)
    ap.add_argument("--roots", nargs="+", default=DEFAULT_ROOTS)
    ap.add_argument("--hours", type=float, default=24.0)
    ap.add_argument("--client-id", default=f"capture-{os.uname().nodename}")
    a = ap.parse_args()

    cfg = json.load(open(a.config))["mqtt"]
    out = open(a.out, "a", buffering=1)            # line buffered: a crash loses at most one line
    os.chmod(a.out, 0o600)
    count = {"n": 0}
    stop = {"flag": False}

    def on_connect(c, u, flags, rc, props=None):
        for r in a.roots:
            c.subscribe(r, qos=0)                  # subscribe only; this tool has no publish call at all

    def on_message(c, u, msg):
        out.write(json.dumps(record(msg.topic, msg.payload, msg.retain), ensure_ascii=False) + "\n")
        count["n"] += 1

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=a.client_id)
    if cfg.get("username"):
        c.username_pw_set(cfg["username"], cfg.get("password"))
    c.on_connect, c.on_message = on_connect, on_message
    c.reconnect_delay_set(1, 30)
    c.connect(cfg["broker"], int(cfg.get("port", 1883)), 60)
    c.loop_start()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.__setitem__("flag", True))
    end = time.time() + a.hours * 3600
    try:
        while time.time() < end and not stop["flag"]:
            time.sleep(1)
    finally:
        c.loop_stop(); c.disconnect(); out.close()
    print(f"captured {count['n']} messages into {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
