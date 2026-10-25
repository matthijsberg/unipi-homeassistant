"""T45: software versions (OS, kernel, evok, Unipi/Pi firmware packages, pending updates) as diagnostic sensors."""
import asyncio
import json
from pathlib import Path

import pytest
from conftest import drain, run

from unipi_core import sysinfo

APT = """Listing...
unipi-firmware6/trixie,trixie 7.22 all [upgradable from: 7.20]
libexpat1/stable-security 2.9.0-1~deb13u1 arm64 [upgradable from: 2.8.3-1~deb13u1]
raspi-firmware/trixie 1:1.20261001-1 all [upgradable from: 1:1.20260915-1]
"""


BOARDS_OLD = lambda: {1: "7.6", 2: "7.22"}      # board 1 still runs old firmware
NO_BOARDS = lambda: {}


def fake_run(table):
    def _run(cmd):
        key = " ".join(cmd)
        for k, v in table.items():
            if key.startswith(k):
                if isinstance(v, Exception):
                    raise v
                return v
        return ""
    return _run


FULL = {"dpkg-query -W -f=${Status}|${Version} evok": "install ok installed|3.0.6.3~trixie",
        "dpkg-query -W -f=${Status}|${Version} unipi-firmware6": "install ok installed|7.20",
        "dpkg-query -W -f=${Status}|${Version} raspi-firmware": "install ok installed|1:1.20260915-1",
        "apt-config dump": 'APT::Periodic::Unattended-Upgrade "1";\n',
        "apt list": APT}


def test_collects_the_versions_and_pending_updates():
    i = sysinfo.collect("2.2.0-x", fake_run(FULL), BOARDS_OLD)
    assert (i["bridge"], i["evok"], i["unipi_firmware"], i["pi_firmware"]) == ("2.2.0-x", "3.0.6.3~trixie", "7.20", "1:1.20260915-1")
    assert i["os"] and i["kernel"] and i["python"]
    assert [u["name"] for u in i["updates"]] == ["unipi-firmware6", "libexpat1", "raspi-firmware"]
    a = sysinfo.updates_attributes(i)
    assert a["firmware_updates"] == ["unipi-firmware6 7.20 -> 7.22", "raspi-firmware 1:1.20260915-1 -> 1:1.20261001-1"]
    assert "libexpat1 2.8.3-1~deb13u1 -> 2.9.0-1~deb13u1" in a["packages"]


def test_a_package_that_is_not_installed_or_a_failing_command_just_leaves_the_item_out():
    table = dict(FULL)
    table["dpkg-query -W -f=${Status}|${Version} evok"] = "deinstall ok config-files|3.0.0"      # removed, config left
    table["dpkg-query -W -f=${Status}|${Version} unipi-firmware6"] = FileNotFoundError("no dpkg")
    table["apt list"] = subprocess_timeout()
    i = sysinfo.collect("v", fake_run(table), NO_BOARDS)
    assert i["evok"] is None and i["unipi_firmware"] is None and i["pi_firmware"] == "1:1.20260915-1"
    assert i["updates"] is None and sysinfo.updates_attributes(i) == {}                            # "unknown", not "0"


def subprocess_timeout():
    import subprocess
    return subprocess.TimeoutExpired("apt", 20)


def test_no_updates_is_an_empty_list_not_unknown():
    i = sysinfo.collect("v", fake_run({**FULL, "apt list": "Listing...\n"}), NO_BOARDS)
    assert i["updates"] == [] and sysinfo.updates_attributes(i)["firmware_updates"] == []


# ---- the bridge ---------------------------------------------------------------------------------------------------------------
@pytest.fixture
def b(discovered, mod, monkeypatch):
    discovered.mod = mod
    drain(discovered.websocket_to_mqtt_queue)
    monkeypatch.setattr(sysinfo, "default_runner", fake_run(FULL))
    return discovered


def topics(b):
    return {t: p for t, p in drain(b.websocket_to_mqtt_queue)}


def test_discovery_creates_diagnostic_sensors_on_the_bridge_device(b):
    b.publish_bridge_discovery()
    cfgs = {t: json.loads(p) for t, p in topics(b).items() if "_sys_" in t and t.endswith("/config")}
    names = sorted(c["name"] for c in cfgs.values())
    assert names == sorted(["Bridge version", "Operating system", "Kernel", "Python", "Evok version", "Unipi firmware package",
                            "Raspberry Pi firmware package", "Updates available", "Firmware updates available",
                            "Board firmware (running)", "Board firmware status", "Automatic security updates"])
    for c in cfgs.values():
        assert c["entity_category"] == "diagnostic" and c["dev"]["identifiers"] == [f"{b.device_name}_bridge"]
        assert c["state_topic"].startswith(f"unipi/{b.device_name}_bridge/system/") and c["unique_id"].startswith(f"{b.device_name}_sys_")
    ups = next(c for c in cfgs.values() if c["name"] == "Updates available")
    assert ups["json_attributes_topic"].endswith("/system/updates/attributes")


def test_values_are_published_and_firmware_updates_are_counted(b):
    b._sysinfo = sysinfo.collect("2.2.0-x", fake_run(FULL), BOARDS_OLD)
    b._publish_system_info()
    t = topics(b)
    base = f"unipi/{b.device_name}_bridge/system"
    assert t[f"{base}/evok/state"] == "3.0.6.3~trixie" and t[f"{base}/unipi_firmware/state"] == "7.20" and t[f"{base}/bridge/state"] == "2.2.0-x"
    assert t[f"{base}/updates/state"] == "3" and t[f"{base}/firmware_updates/state"] == "3"          # 2 packages + 1 board running old firmware
    assert t[f"{base}/board_firmware_text/state"] == "board 1: 7.6, board 2: 7.22" and t[f"{base}/board_firmware_status/state"] == "update available"
    assert t[f"{base}/auto_updates/state"] == "enabled"
    assert "fwspi --auto" in json.loads(t[f"{base}/updates/attributes"])["how_to_flash"]
    assert "unipi-firmware6 7.20 -> 7.22" in json.loads(t[f"{base}/updates/attributes"])["firmware_updates"]


def test_unknown_values_are_not_published_as_text(b):
    b._sysinfo = {"bridge": "v", "os": None, "kernel": "6.1", "python": "3", "evok": None, "unipi_firmware": None, "pi_firmware": None, "updates": None}
    b._publish_system_info()
    t = topics(b)
    assert set(k.split("/")[-2] for k in t) == {"bridge", "kernel", "python"}                       # no 'None', no updates counts


def test_the_background_loop_collects_off_the_event_loop_and_publishes(b, monkeypatch):
    import threading
    threads = []
    real_collect = sysinfo.collect
    monkeypatch.setattr(sysinfo, "collect", lambda v, run=sysinfo.default_runner: (threads.append(threading.current_thread()), real_collect(v, run))[1])

    async def go():
        real = asyncio.sleep
        calls = {"n": 0}

        async def quick(delay, *a, **k):
            calls["n"] += 1
            if calls["n"] >= 2:                                                                   # second sleep = the 24 h wait: stop
                b.should_stop.set()
            await real(0)
        asyncio.sleep, orig = quick, asyncio.sleep
        try:
            await b._system_info_loop()
        finally:
            asyncio.sleep = orig
    run(b, go())
    assert threads and threads[0] is not threading.main_thread()                                   # apt/dpkg never block the event loop
    assert b._sysinfo and b._sysinfo["bridge"] == b.mod.SCRIPT_VERSION
    assert f"unipi/{b.device_name}_bridge/system/evok/state" in topics(b)


def test_republish_resends_the_cached_versions(b, fast_sleep):
    b._sysinfo = sysinfo.collect("v", fake_run(FULL), NO_BOARDS)
    b.mqtt_client.connected = True
    run(b, b.republish_all("test"))
    assert any(t.endswith("/system/evok/state") for t in topics(b))


def test_shadow_instance_publishes_no_version_sensors(b):
    b.shadow = True
    b._sysinfo = sysinfo.collect("v", fake_run(FULL), NO_BOARDS)
    b.publish_bridge_discovery(); b._publish_system_info()
    assert not any("/system/" in t or "_sys_" in t for t in topics(b))


def test_updates_attributes_topic_is_retained():
    src = (Path(__file__).resolve().parent.parent / "hass-unipi.py").read_text()
    assert '"/system/updates/attributes"' in src                                                      # in the MQTT worker's retain list


# ---- running board firmware (Modbus register 1000) ----------------------------------------------------------------------------
import socket
import struct
import threading

AUTOGEN = """comm_channels:
  LOCAL_TCP:
    type: MODBUSTCP
    hostname: 127.0.0.1
    port: 50200
    device_info:
      family: Neuron
      model: M203
    devices:
      1:
        slave-id: 1
        model: '00'
      2:
        slave-id: 2
        model: '08'
  OWBUS:
    type: OWFS
    port: 4304
"""


def test_the_modbus_endpoint_and_board_units_come_from_evoks_generated_config(tmp_path):
    f = tmp_path / "autogen.yaml"; f.write_text(AUTOGEN)
    g = tmp_path / "noport.yaml"; g.write_text(AUTOGEN.replace("    port: 50200\n", ""))
    assert sysinfo.evok_modbus_endpoint(str(g)) == ("127.0.0.1", 502, [1, 2])                      # must not pick up OWBUS's port 4304
    assert sysinfo.evok_modbus_endpoint(str(f)) == ("127.0.0.1", 50200, [1, 2])                    # not the OWBUS port further down
    assert sysinfo.evok_modbus_endpoint(str(tmp_path / "missing.yaml")) == ("127.0.0.1", 502, [1])  # sensible default


class FakeModbus:
    """Minimal Modbus-TCP slave: answers function 3 for register 1000 per unit; other units answer with an exception."""
    def __init__(self, regs):
        self.regs, self.requests = regs, []
        self.srv = socket.socket(); self.srv.bind(("127.0.0.1", 0)); self.srv.listen(5)
        self.port = self.srv.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            with c:
                req = c.recv(12)
                tid, proto, ln, unit, func, addr, cnt = struct.unpack(">HHHBBHH", req)
                self.requests.append((unit, func, addr, cnt))
                if unit in self.regs and addr == 1000:
                    c.sendall(struct.pack(">HHHBBBH", tid, 0, 5, unit, 3, 2, self.regs[unit]))
                else:
                    c.sendall(struct.pack(">HHHBBB", tid, 0, 3, unit, 0x83, 2))             # exception response ...
                    import time; time.sleep(1.5)                                             # ... on a connection that stays open (like a real slave)


def test_the_running_firmware_is_read_with_a_plain_read_of_register_1000():
    srv = FakeModbus({1: 0x0716, 2: 0x0706})                                                      # 7.22 and 7.6
    assert sysinfo.read_board_firmware("127.0.0.1", srv.port, [1, 2]) == {1: "7.22", 2: "7.6"}
    assert srv.requests == [(1, 3, 1000, 1), (2, 3, 1000, 1)]                                     # function 3 only: nothing is ever written


def test_a_board_that_does_not_answer_or_an_unreachable_endpoint_is_left_out():
    srv = FakeModbus({1: 0x0716})
    import time
    t0 = time.time()
    assert sysinfo.read_board_firmware("127.0.0.1", srv.port, [1, 9]) == {1: "7.22"}              # unit 9: exception response
    assert time.time() - t0 < 1.0                                                                 # recognised at once, not after a timeout
    closed = socket.socket(); closed.bind(("127.0.0.1", 0)); p = closed.getsockname()[1]; closed.close()
    assert sysinfo.read_board_firmware("127.0.0.1", p, [1]) == {}                                 # refused: no exception, no sensor value


def test_status_text_and_count_make_old_board_firmware_visible():
    assert sysinfo.board_firmware_status({1: "7.6", 2: "7.22"}, "7.22") == "update available"       # the M203 case before flashing
    assert sysinfo.board_firmware_status({1: "7.22", 2: "7.22"}, "7.22") == "current"
    assert sysinfo.board_firmware_status({1: "7.22"}, "7.22.1") == "current"                        # patch level of the package is ignored
    assert sysinfo.board_firmware_status({}, "7.22") == "unknown" and sysinfo.board_firmware_status({1: "7.22"}, None) == "unknown"
    assert sysinfo.board_firmware_text({1: "7.22", 2: "7.22"}) == "7.22"
    assert sysinfo.board_firmware_text({1: "7.6", 2: "7.22"}) == "board 1: 7.6, board 2: 7.22"
    i = sysinfo.collect("v", fake_run({**FULL, "apt list": "Listing...\n"}), BOARDS_OLD)
    assert sysinfo.firmware_update_count(i) == 1                                                  # no package pending, but a board runs old firmware
    assert sysinfo.firmware_update_count(sysinfo.collect("v", fake_run(FULL), NO_BOARDS)) == 2      # two firmware packages
    assert sysinfo.firmware_update_count({"updates": None}) is None


def test_automatic_security_updates_state():
    assert sysinfo.auto_security_updates(fake_run({"apt-config dump": 'APT::Periodic::Unattended-Upgrade "1";\n'})) == "enabled"
    assert sysinfo.auto_security_updates(fake_run({"apt-config dump": 'APT::Periodic::Unattended-Upgrade "0";\n'})) == "disabled"
    assert sysinfo.auto_security_updates(fake_run({"apt-config dump": ""})) == "disabled"
    assert sysinfo.auto_security_updates(fake_run({"apt-config dump": FileNotFoundError("no apt-config")})) is None
