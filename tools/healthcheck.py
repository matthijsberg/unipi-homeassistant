#!/usr/bin/env python3
"""Health check for the hass-unipi service. Exit 0 healthy, 1 unhealthy, 2 cannot evaluate.

Checks (all must pass):
  1. systemd unit active, and not crash-looping (NRestarts unchanged since --restarts-before)
  2. MQTT  <root>/<device>/status            == "online"
  3. MQTT  <root>/<device>_bridge/startup_error == "0"   (published ~startup_debug_seconds after start)
  4. discovery origin.sw == version expected (default: SCRIPT_VERSION found in the deployed script)

--fresh : after a restart the broker still holds the *previous* run's retained values, so
          require LIVE (non-retained) publishes for checks 2-4. Use this right after a restart.
Fails fast when the service dies or crash-loops instead of waiting for the full timeout.
"""
import argparse
import json
import os
import re
import subprocess
import sys
import time

import paho.mqtt.client as mqtt


def sysd(unit, *props):
    """Values of the requested systemd properties, IN THE REQUESTED ORDER.

    `systemctl show --value` prints in systemd's internal order, not the requested one (this
    swapped ActiveState and NRestarts once and disabled crash detection), so parse key=value.
    """
    out = subprocess.run(["systemctl", "show", unit, *[f"-p{p}" for p in props]],
                         capture_output=True, text=True).stdout
    kv = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
    return [kv.get(p, "") for p in props]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runtime", default=os.environ.get("UNIPI_RUNTIME", "/home/unipi/unipi-homeassistant/scripts"))
    ap.add_argument("--service", default=os.environ.get("UNIPI_SERVICE", "hass-unipi"))
    ap.add_argument("--timeout", type=int, default=90)
    ap.add_argument("--fresh", action="store_true", help="require live publishes (use right after a restart)")
    ap.add_argument("--restarts-before", type=int, default=None, help="NRestarts value before the restart")
    ap.add_argument("--expect-version", default=None)
    ap.add_argument("--skip-systemd", action="store_true", help="MQTT checks only (tests)")
    a = ap.parse_args()

    try:
        cfg = json.load(open(os.path.join(a.runtime, "config.json")))["mqtt"]
        device = open(os.path.join(a.runtime, ".device_name")).read().strip()
        version = a.expect_version
        if not version:
            m = re.search(r'^SCRIPT_VERSION\s*=\s*"([^"]+)"', open(os.path.join(a.runtime, "hass-unipi.py")).read(), re.M)
            version = m.group(1) if m else None
    except Exception as e:
        print(f"healthcheck: cannot evaluate: {e}", file=sys.stderr)
        return 2
    root = cfg.get("topic", "unipi")
    topics = {
        "status": f"{root}/{device}/status",
        "startup_error": f"{root}/{device}_bridge/startup_error",
        "discovery": f"homeassistant/binary_sensor/{device}_bridge_startup_error/config",
    }
    seen: dict[str, tuple[bool, str]] = {}  # name -> (retained, payload)

    def on_message(c, u, msg):
        for name, t in topics.items():
            if msg.topic == t:
                seen[name] = (bool(msg.retain), msg.payload.decode(errors="replace"))

    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"healthcheck-{os.getpid()}")
    if cfg.get("username"):
        c.username_pw_set(cfg["username"], cfg.get("password"))
    c.on_message = on_message
    c.on_connect = lambda cl, u, f, rc, p=None: [cl.subscribe(t) for t in topics.values()]
    try:
        c.connect(cfg["broker"], cfg.get("port", 1883), 30)
    except Exception as e:
        print(f"healthcheck: cannot reach MQTT broker: {e}", file=sys.stderr)
        return 2
    c.loop_start()

    def ok(name, want):
        got = seen.get(name)
        if got is None or (a.fresh and got[0]):
            return False
        return got[1] == want if want is not None else True

    deadline = time.monotonic() + a.timeout
    result, why = 1, "timeout"
    try:
        while time.monotonic() < deadline:
            if not a.skip_systemd:
                state, nrest = sysd(a.service, "ActiveState", "NRestarts")
                if state in ("failed", "inactive"):
                    why = f"service is {state}"; break
                if state == "activating" or (a.restarts_before is not None and nrest.isdigit() and int(nrest) > a.restarts_before):
                    why = f"service crash-looping (state={state}, NRestarts={nrest})"; break
            if ok("status", "online") and ok("startup_error", "0"):
                sw = None
                if "discovery" in seen:
                    try:
                        sw = json.loads(seen["discovery"][1]).get("origin", {}).get("sw")
                    except ValueError:
                        pass
                if version and sw != version:
                    why = f"version mismatch (bridge reports {sw!r}, expected {version!r})"
                    if "discovery" in seen and (not a.fresh or not seen["discovery"][0]):
                        result = 1; break
                else:
                    result, why = 0, f"healthy (version {sw})"; break
            time.sleep(1)
        else:
            why = f"timeout after {a.timeout}s; saw " + ", ".join(f"{k}={'retained ' if v[0] else 'live '}{v[1][:20]!r}" for k, v in seen.items())
    finally:
        c.loop_stop(); c.disconnect()
    print(f"healthcheck: {'OK' if result == 0 else 'FAIL'} - {why}")
    return result


if __name__ == "__main__":
    sys.exit(main())
