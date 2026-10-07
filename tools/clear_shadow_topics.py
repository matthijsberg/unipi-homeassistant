#!/usr/bin/env python3
"""Remove the retained MQTT topics a shadow instance left behind (dry run unless --yes).

Only topics whose device segment contains "_shadow" are ever touched; the live instance's topics cannot match.
"""
import argparse
import json
import os
import re
import time

SHADOW = re.compile(r"(^|/)[^/]*_shadow(_[^/]*)?(/|$)")


def is_shadow_topic(topic: str) -> bool:
    return bool(SHADOW.search(topic))


def main() -> int:
    import paho.mqtt.client as mqtt
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="/home/unipi/unipi-homeassistant/scripts/config.json")
    ap.add_argument("--yes", action="store_true", help="really delete (default: only list)")
    a = ap.parse_args()
    cfg = json.load(open(a.config))["mqtt"]
    root, prefix = cfg.get("topic", "unipi"), cfg.get("discovery_prefix", "homeassistant")
    found = set()
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"clear-shadow-{os.getpid()}")
    if cfg.get("username"):
        c.username_pw_set(cfg["username"], cfg.get("password"))
    c.on_message = lambda cl, u, m: found.add(m.topic) if m.retain and m.payload and is_shadow_topic(m.topic) else None
    c.on_connect = lambda cl, u, f, rc, p=None: [cl.subscribe(t) for t in (f"{root}/#", f"{prefix}/#")]
    c.connect(cfg["broker"], cfg.get("port", 1883)); c.loop_start(); time.sleep(6)
    for t in sorted(found):
        print(("DELETE " if a.yes else "would delete ") + t)
        if a.yes:
            c.publish(t, b"", qos=1, retain=True).wait_for_publish()
    c.loop_stop()
    print(f"{len(found)} retained shadow topic(s) {'removed' if a.yes else 'found (use --yes to remove)'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
