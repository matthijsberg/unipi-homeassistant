"""T22: the legacy L513 config -> circuits + local rules of the new bridge; the result must be valid AND must really work."""
import json
import sys
from pathlib import Path

import pytest
from conftest import drain, run

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "tools"))
import legacy_to_site as l2s  # noqa: E402
from test_sequencer import FakeTime  # noqa: E402

LEGACY = json.loads((ROOT / "tests" / "fixtures" / "l513_legacy_config_anonymized.json").read_text())   # real structure, generic names
EVOK2 = json.loads((ROOT / "tests" / "fixtures" / "l513_evok2_rest_all.json").read_text())

BY_NAME = {e["description"]: e for e in LEGACY}
HA = {  # the parts of the HA export the converter reads (synthetic copy of the real structure)
    "binary_sensor": [
        {"name": "Front door contact", "state_topic": BY_NAME["Contact 3"]["state_topic"], "device_class": "door"},
        {"name": "Hall motion", "state_topic": BY_NAME["PIR 1"]["state_topic"], "device_class": "presence"},
        {"name": "Front door lock", "state_topic": BY_NAME["Lock 2"]["state_topic"], "device_class": "lock"},
    ],
    "light": [{"name": "Scullery light", "state_topic": BY_NAME["Light switch 1"]["state_topic"], "command_topic": "unipi1/x/light/set",
               "command_on_template": '{"state": "on", "circuit": "2_03", "dev": "analogoutput"}',
               "command_off_template": '{"state": "off", "circuit": "2_03", "dev": "analogoutput"}'},
              {"name": "Test Relay 2_02", "command_topic": "unipi1/x/testrelay/set",
               "command_on_template": '{"state": "on", "circuit": "2_02", "dev": "output"}'},
              {"name": "S103 json light", "command_topic": "unipi/Neuron_S103_2258/ao/xS51_04/set",
               "command_on_template": '{"state": "on", "circuit": "xS51_04", "dev": "analogoutput"}'},
              {"name": "Other box light", "command_topic": "unipi2/x/light/set",
               "command_on_template": '{"state": "on", "circuit": "UART_2_2_01", "dev": "analogoutput"}'}],
    "switch": [{"name": "Roof window", "state_topic": "unipi/x/window/contact", "command_topic": "unipi1/x/window/set",
                "payload_on": '{"state": "on", "circuit": "1_02", "dev": "output", "duration": 25}',
                "payload_off": '{"state": "on", "circuit": "1_01", "dev": "output", "duration": 35}'},
               {"name": "Bell", "command_topic": "unipi1/x/bell/set",
                "payload_on": '{"circuit": "2_02", "dev": "relay", "repeat": "2", "state": "pulse"}',
                "payload_off": '{"circuit": "2_02", "dev": "relay", "state": "off"}'},
               {"name": "Heat pump grid 1", "command_topic": "unipi1/x/grid1/set",
                "payload_on": '{"circuit": "3_01", "dev": "relay", "state": "on"}', "payload_off": '{"circuit": "3_01", "dev": "relay", "state": "off"}'}],
    "fan": [{"name": "Living room fan", "command_topic": "unipi/Neuron_S103_2258/ao/xS51_04/set", "payload_on": "ON"}],
}


@pytest.fixture(scope="module")
def conv():
    return l2s.Converter(LEGACY, HA, EVOK2, "xS30", "Neuron_L513_10")


def test_every_legacy_entry_becomes_a_circuit_with_a_name(conv):
    c = conv.circuits()
    for e in LEGACY:
        assert c[conv.key(e["dev"], e["circuit"])]["name"]


def test_names_keep_the_old_ha_entity_where_the_signal_is_mirrored_and_are_raw_where_ha_must_post_process(conv):
    c = conv.circuits()
    assert c[conv.key("input", BY_NAME["Contact 3"]["circuit"])]["name"] == "Front door contact"      # same name = same entity_id after the switch
    assert c[conv.key("input", BY_NAME["Lock 2"]["circuit"])]["name"] == "Front door lock"
    assert c["di/1_01"]["name"] == "PIR 1"                                                          # PIR: raw; "Hall motion" becomes a template with delay_off
    assert c[conv.key("ai", BY_NAME["Lux sensor 1"]["circuit"])]["name"] == "Lux sensor 1 (V)"      # volts; lux is a template in HA
    assert c["di/xS30_02"]["name"] == "Water meter ticks"
    assert c[conv.key("input", BY_NAME["Light switch 1"]["circuit"])]["name"] == "Light switch 1"   # the HA match is a light, not the switch


def test_dev_names_circuits_and_inversion_follow_the_facts(conv):
    c = conv.circuits()
    assert c["di/2_03"]["inverted"] is True and "inverted" not in c["di/2_02"]                      # nc contact vs no push button
    assert c["di/xS30_15"] == {"name": "Front door lock", "device_class": "lock", "inverted": True}   # UART_4_4_15, nc
    assert c["di/xS30_16"] == {"name": "Lock 1", "inverted": True}                          # no HA class known for this one: none invented
    assert c[conv.key("input", BY_NAME["Contact 3"]["circuit"])]["device_class"] == "door"          # device class comes from the HA export
    assert c["di/xS30_02"] == {"name": "Water meter ticks", "counter": True, "counter_interval_s": 10.0, "unit": "ticks"}
    assert "1wdevice/26729616020000C2/temp" in c and "1wdevice/26729616020000C2/humidity" in c      # the DS2438 gives both
    assert "temp/28D1EFA708000052" in c                                                             # a plain DS18B20
    assert not any(k.startswith(("input", "relay", "output", "analogoutput")) for k in c)          # canonical evok-3 names only


def test_outputs_come_from_the_local_targets_and_what_ha_commands(conv):
    c = conv.circuits()
    assert c["ao/2_03"]["name"] == "Scullery light" and c["ro/3_01"]["name"] == "Heat pump grid 1"
    assert c["ro/1_02"]["name"] == "Roof window openen (25 s)" and c["ro/1_01"]["name"] == "Roof window sluiten (35 s)"
    assert "UART_2_2_01" not in json.dumps(c) and "Living room fan" not in json.dumps(c)           # unipi2 / S103 entities are not ours
    assert "xS51_04" not in json.dumps(c) and "S103 json light" not in json.dumps(c)
    assert c["ro/2_02"]["name"] == "Bel"                                                            # the test relay is the bell relay: not "Test Relay"


def test_safety_settings_for_the_bell_and_the_window_motor(conv):
    c = conv.circuits()
    bell = c["ro/2_02"]
    assert bell["failsafe_off"] is True and bell["max_on_s"] == 5 and bell["pulse_defaults"] == {"on_ms": 100, "off_ms": 300}
    assert set(bell["presets"]) == {"ring_2x", "ring_3x"}
    assert c["ro/1_01"]["failsafe_off"] and c["ro/1_01"]["max_on_s"] == 40 and c["ro/1_02"]["max_on_s"] == 30   # longest run + 5 s
    assert "failsafe_off" not in c["ro/3_01"]                                                       # a smart-grid relay must keep its state


def test_the_rules_are_the_eight_handle_local_entries_in_legacy_terms(conv):
    rules = {r["name"]: r for r in conv.rules()}
    assert len(rules) == 8
    bel = rules["Bell button 2"]
    assert (bel["trigger_dev"], bel["trigger_circuit"], bel["trigger_value"]) == ("di", "2_05", "1")
    assert bel["action_type"] == "pulse" and bel["action_dev"] == "ro" and bel["action_circuit"] == "2_02"
    assert bel["action_pulse"] == {"count": 3, "on_ms": 100, "off_ms": 300}                         # legacy ring_bel: 0.1 s on, 0.3 s off
    assert rules["Bell button 1"]["action_pulse"]["count"] == 2
    nis = rules["Light switch 3"]
    assert nis["trigger_circuit"] == "xS30_03" and nis["action_type"] == "dimmer" and nis["action_value"] == 5
    assert nis["dimmer_hold"] is False and nis["dimmer_fade_on_ms"] == 0                           # legacy = plain toggle, no dimming, no fade
    assert {r["group"] for r in rules.values()} == {"Bel", "Licht"}                                 # generic topics carry no room name
    assert [r["group"] for r in conv.rules()].count("Bel") == 2


def test_the_generated_circuits_are_accepted_by_the_bridge_config(conv, mod):
    cfg = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"}, circuits=conv.circuits())
    assert len(cfg.circuits) == len(conv.circuits())


@pytest.fixture
def b(discovered, mod):
    ft = FakeTime()
    discovered.sequencer.clock, discovered.sequencer.sleep = ft.clock, ft.sleep
    discovered._dim_sleep = ft.sleep
    discovered.ft, discovered.sent_times = ft, []
    real = discovered.websocket_connection.send

    async def timed(m):
        discovered.sent_times.append(round(ft.t, 3))
        await real(m)
    discovered.websocket_connection.send = timed
    conv = l2s.Converter(LEGACY, HA, EVOK2, "xS30", "Neuron_L513_10")
    discovered.config.circuits = mod.AppConfig(mqtt={"broker": "x"}, websocket={"url": "ws://x/ws"}, unipi_http={"url": "http://x/rest/all"},
                                               circuits={k: v for k, v in conv.circuits().items() if k.split("/")[0] in ("ro", "ao", "di")}).circuits
    discovered.mod, discovered.conv = mod, conv
    drain(discovered.websocket_to_mqtt_queue)
    return discovered


def test_every_generated_rule_is_valid_for_the_bridge(b):
    rules = [b.mod.LocalLogicRule(**r) for r in b.conv.rules()]
    assert [b.validate_rule(r) for r in rules] == [None] * 8                                        # none would be disabled at start-up


def press(b, circuit):
    b.device_states[f"di_{circuit}"] = 0
    b.process_websocket_message({"dev": "di", "circuit": circuit, "value": 1})


def test_the_front_door_button_rings_three_times_like_the_old_script(b):
    b.local_logic.rules = [b.mod.LocalLogicRule(**r) for r in b.conv.rules()]
    b.mqtt_client.connected = False                                                                 # no MQTT, no HA
    press(b, "2_05")
    run(b, b.ft.advance_to(4))
    sent = [json.loads(m) for m in b.websocket_connection.sent]
    assert [(m["dev"], m["circuit"], m["value"]) for m in sent] == [("ro", "2_02", v) for v in (1, 0, 1, 0, 1, 0)]
    assert b.sent_times == [0.0, 0.1, 0.4, 0.5, 0.8, 0.9]                                           # 100 ms on / 300 ms off, exactly the old timing


def test_a_light_switch_toggles_between_off_and_its_level(b):
    b.local_logic.rules = [b.mod.LocalLogicRule(**r) for r in b.conv.rules()]
    b.device_states["ao_2_03"] = 0.0
    press(b, "3_02")                                                                                  # "Light switch 1": level 10 V
    run(b, b.ft.advance_to(1))
    assert [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)] == [10.0]
    b.device_states["ao_2_03"] = 10.0
    press(b, "xS30_05")                                                                               # the second switch of the same lamp (was UART_4_4_05)
    run(b, b.ft.advance_to(2))
    assert [json.loads(m)["value"] for m in drain(b.mqtt_to_websocket_queue)] == [0.0]


def test_the_map_document_lists_everything_and_what_moves_to_ha(conv):
    doc = conv.render_map()
    for e in LEGACY:
        assert f"`{e['state_topic']}`" in doc                                                        # every old topic is in the table
    assert "## 1. PIR hold" in doc and "| PIR 1 | `di/1_01` | 20 s |" in doc
    assert "| PIR 2 | `di/1_02` | 120 s |" in doc and "| PIR 8 | `di/xS30_07` | 90 s |" in doc
    assert "lux = round(mean(V) x 200)" in doc and "m3 = ticks x 0.00001" in doc
    assert '{"pulse": {"count": 2}}' in doc and "ring_2x" in doc and '"duration_s": 25' in doc
    assert "unipi/Neuron_L513_10/di/2_05/state" in doc                                               # the new topic names
    assert "Warnings" in doc and "288D219F0A0000" in doc                                              # a temperature sensor that the L513 does not have
    assert "password" not in doc.lower()


def test_cli_writes_files_and_strict_fails_on_a_missing_circuit(tmp_path):
    legacy = tmp_path / "legacy.json"; legacy.write_text(json.dumps(LEGACY))
    evok2 = tmp_path / "evok2.json"; evok2.write_text(json.dumps(EVOK2))
    args = ["--legacy", str(legacy), "--evok2-rest", str(evok2), "--out-dir", str(tmp_path / "out")]
    assert l2s.main(args) == 0 and (tmp_path / "out" / "circuits.json").exists() and (tmp_path / "out" / "local_rules.json").exists()
    assert l2s.main(args + ["--strict"]) == 1                                                         # the boiler sensor is not in the snapshot
    evok3 = tmp_path / "evok3.json"; evok3.write_text(json.dumps([{"dev": "di", "circuit": "2_05"}]))
    assert l2s.main(args + ["--evok3-rest", str(evok3), "--strict"]) == 1                             # nearly everything is missing in this fake listing


def test_unsupported_local_handler_types_fail_loudly():
    bad = [{"circuit": "1_01", "description": "x", "dev": "input", "handle_local": {"type": "teleport", "output_dev": "relay", "output_circuit": "1_01"}}]
    with pytest.raises(ValueError):
        l2s.Converter(bad).rules()


def test_the_editor_group_of_a_light_switch_is_the_room_from_the_legacy_topic():
    def sw(topic):
        return [{"circuit": "3_02", "description": "s", "dev": "input", "state_topic": topic,
                 "handle_local": {"type": "dimmer", "output_dev": "analogoutput", "output_circuit": "2_03", "level": 10}}]
    assert l2s.Converter(sw("unipi1/bgg/woonkamer/nis/licht")).rules()[0]["group"] == "Woonkamer"
    assert l2s.Converter(sw("unipi1/buiten/achterdeur/licht")).rules()[0]["group"] == "Achterdeur"
