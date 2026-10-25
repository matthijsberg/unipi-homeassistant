#!/usr/bin/env python3
"""Clean stale retained MQTT messages - with a backup first, a dry run by default, and a way back.

  clear_retained.py --config C --patterns 'unipi2/#' [--keep-file legacy.json] [--backup B.json]        # dry run: lists what would go
  clear_retained.py --config C --patterns 'unipi2/#' --backup B.json --apply                            # backup, clear, verify
  clear_retained.py --config C --restore B.json                                                         # put everything back

How it works: subscribe to the patterns, collect the messages the broker flags as retained, write them (topic, payload, qos)
to the backup (mode 600), then publish an empty retained payload to each topic, which makes the broker forget it. Afterwards it
reconnects and proves that nothing retained is left. Live (non-retained) traffic is never touched, a topic that a device
republishes simply comes back - which is exactly how you find out whether something still produces it.
`--keep-state-topics FILE` protects topics (and `<topic>/available`) listed in a legacy unipi_mqtt_config.json (`state_topic`).
Credentials come from the `mqtt` section of a bridge config.json and are never printed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Callable

import paho.mqtt.client as mqtt


def collect_retained(client_factory: Callable[[], "mqtt.Client"], cfg: dict, patterns: list[str], wait: float = 6.0) -> dict[str, dict]:
    """{topic: {"payload": str(hex-safe), "qos": int}} of all RETAINED messages matching the patterns."""
    found: dict[str, dict] = {}
    c = client_factory()

    def on_connect(cl, u, f, rc, p=None):
        for pat in patterns:
            cl.subscribe(pat, qos=1)

    def on_message(cl, u, m):
        if m.retain and m.payload:
            try:
                text = m.payload.decode("utf-8")
            except UnicodeDecodeError:
                text = "hex:" + m.payload.hex()
            found[m.topic] = {"payload": text, "qos": m.qos}

    c.on_connect, c.on_message = on_connect, on_message
    c.connect(cfg["broker"], int(cfg.get("port", 1883)), 30)
    c.loop_start()
    time.sleep(wait)
    c.loop_stop(); c.disconnect()
    return found


def protected(topic: str, keep: set[str]) -> bool:
    return topic in keep or (topic.endswith("/available") and topic[: -len("/available")] in keep)


def plan(found: dict[str, dict], keep: set[str]) -> tuple[list[str], list[str]]:
    """(topics to clear, topics kept because they are protected)."""
    clear = sorted(t for t in found if not protected(t, keep))
    kept = sorted(t for t in found if protected(t, keep))
    return clear, kept


def write_backup(path: str, found: dict[str, dict], topics: list[str]) -> None:
    data = {"created": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "messages": [{"topic": t, **found[t]} for t in topics]}
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as f:
        json.dump(data, f, indent=1)


def publish_all(client_factory, cfg, items: list[tuple[str, bytes]], qos: int = 1) -> None:
    c = client_factory()
    c.connect(cfg["broker"], int(cfg.get("port", 1883)), 30)
    c.loop_start()
    pubs = [c.publish(t, payload, qos=qos, retain=True) for t, payload in items]
    for p in pubs:
        p.wait_for_publish(timeout=10)
    c.loop_stop(); c.disconnect()


def make_client_factory(cfg: dict, client_id: str):
    def factory():
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"{client_id}-{os.getpid()}-{time.time_ns() % 100000}")
        if cfg.get("username"):
            c.username_pw_set(cfg["username"], cfg.get("password"))
        return c
    return factory


def main(argv=None, factory=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", required=True)
    ap.add_argument("--patterns", nargs="+", default=[])
    ap.add_argument("--keep-state-topics", help="legacy unipi_mqtt_config.json: its state_topic values (and /available) are never cleared")
    ap.add_argument("--keep-prefix", nargs="*", default=[], help="never clear topics starting with these prefixes")
    ap.add_argument("--backup")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--restore")
    ap.add_argument("--wait", type=float, default=6.0)
    a = ap.parse_args(argv)
    cfg = json.load(open(a.config))["mqtt"]
    factory = factory or make_client_factory(cfg, "retained-cleanup")

    if a.restore:
        data = json.load(open(a.restore))
        items = [(m["topic"], bytes.fromhex(m["payload"][4:]) if m["payload"].startswith("hex:") else m["payload"].encode()) for m in data["messages"]]
        publish_all(factory, cfg, items)
        print(f"restored {len(items)} retained messages from {a.restore}")
        return 0

    if not a.patterns:
        ap.error("--patterns is required")
    keep: set[str] = set()
    if a.keep_state_topics:
        keep = {x["state_topic"] for x in json.load(open(a.keep_state_topics)) if x.get("state_topic")}
    found = collect_retained(factory, cfg, a.patterns, a.wait)
    found = {t: v for t, v in found.items() if not any(t.startswith(p) for p in a.keep_prefix)}
    clear, kept = plan(found, keep)
    print(f"{len(found)} retained topics match; {len(clear)} would be cleared, {len(kept)} protected")
    for t in clear:
        print("  clear", t, "=>", found[t]["payload"][:70].replace("\n", " "))
    for t in kept:
        print("  keep ", t)
    if not a.apply:
        print("dry run: nothing changed (add --apply and --backup FILE)")
        return 0
    if not a.backup:
        ap.error("--apply needs --backup FILE")
    write_backup(a.backup, found, clear)
    print(f"backup written: {a.backup} ({len(clear)} messages) - restore with --restore")
    publish_all(factory, cfg, [(t, b"") for t in clear])
    time.sleep(1)
    left = [t for t in collect_retained(factory, cfg, a.patterns, a.wait) if t in set(clear)]
    if left:
        print(f"WARNING: {len(left)} topics are still retained (no permission to clear them?):")
        for t in left:
            print("  ", t)
        return 1
    print(f"verified: all {len(clear)} topics are gone from the broker")
    return 0


if __name__ == "__main__":
    sys.exit(main())
