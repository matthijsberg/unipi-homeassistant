#!/usr/bin/env python3
"""Compare what a shadow instance publishes with what the live bridge publishes (state topics only).

  tools/shadow_compare.py [--seconds 60] [--config /home/unipi/unipi-homeassistant/scripts/config.json]

Subscribes read-only to unipi/<device>/# and unipi/<device>_shadow/#, pairs topics by their path below the
device name and reports payloads that differ, plus topics seen on only one side. Differences are normal for
fast-changing values (analog inputs); look for systematic ones (a different state for the same input).
"""
import argparse
import json
import os
import time


def diff_states(live: dict, shadow: dict) -> dict:
    """live/shadow: {path-below-device: payload}. Returns the three kinds of difference."""
    both = set(live) & set(shadow)
    return {
        "different": {p: (live[p], shadow[p]) for p in sorted(both) if live[p] != shadow[p]},
        "only_live": sorted(set(live) - set(shadow)),
        "only_shadow": sorted(set(shadow) - set(live)),
        "same": len([p for p in both if live[p] == shadow[p]]),
    }


def main() -> int:
    import paho.mqtt.client as mqtt
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds", type=int, default=60)
    ap.add_argument("--config", default="/home/unipi/unipi-homeassistant/scripts/config.json")
    a = ap.parse_args()
    cfg = json.load(open(a.config))["mqtt"]
    dn = open(os.path.join(os.path.dirname(a.config), ".device_name")).read().strip()
    root = cfg.get("topic", "unipi")
    live, shadow = {}, {}

    def on_message(c, u, m):
        for prefix, store in ((f"{root}/{dn}_shadow/", shadow), (f"{root}/{dn}/", live)):
            if m.topic.startswith(prefix):
                path = m.topic[len(prefix):]
                if not path.endswith(("/config", "/attributes")):
                    store[path] = m.payload.decode(errors="replace")
                return

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"shadow-compare-{os.getpid()}")
    if cfg.get("username"):
        c.username_pw_set(cfg["username"], cfg.get("password"))
    c.on_message = on_message
    c.on_connect = lambda cl, u, f, rc, p=None: [cl.subscribe(f"{root}/{dn}/#"), cl.subscribe(f"{root}/{dn}_shadow/#")]
    c.connect(cfg["broker"], cfg.get("port", 1883)); c.loop_start(); time.sleep(a.seconds); c.loop_stop()
    r = diff_states(live, shadow)
    print(f"identical: {r['same']} | different: {len(r['different'])} | only live: {len(r['only_live'])} | only shadow: {len(r['only_shadow'])}")
    for p, (l, s) in r["different"].items():
        print(f"  DIFF  {p}\n        live  : {l}\n        shadow: {s}")
    for p in r["only_live"]:
        print(f"  only live   : {p}")
    for p in r["only_shadow"]:
        print(f"  only shadow : {p}")
    return 0 if not r["different"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
