"""Characterization: Home Assistant discovery + initial states on the recorded S103 snapshot."""
import json
from collections import Counter

DN = "Neuron_S103_2258"
PFX = f"homeassistant"


def cfg(b, comp, entity):
    return json.loads(b.discovered_messages[f"{PFX}/{comp}/{DN}/{entity}/config"])


def test_device_name_and_last_will(discovered):
    assert discovered.device_name == DN
    assert discovered.mqtt_client.will == (f"unipi/{DN}/status", "offline", 1, True)


def test_entity_counts_per_component(discovered):
    own = Counter()
    for topic in discovered.discovered_messages:
        parts = topic.split("/")
        if parts[0] == PFX and parts[2] == DN and parts[-1] == "config":
            own[parts[1]] += 1
    assert own == {"binary_sensor": 8, "switch": 9, "light": 12, "sensor": 15}


def test_extension_monitor_entities(discovered):
    topics = discovered.discovered_messages
    assert f"{PFX}/binary_sensor/{DN}_ext_xS51_problem/config" in topics
    assert f"{PFX}/sensor/{DN}_bridge_connectivity/ext_xS51_last_comm/config" in topics


def test_digital_input_discovery(discovered, mod):
    c = cfg(discovered, "binary_sensor", "di_1_01")
    assert c["unique_id"] == f"{DN}_di_1_01"
    assert c["state_topic"] == f"unipi/{DN}/di/1_01/state"
    assert (c["payload_on"], c["payload_off"]) == ("ON", "OFF")
    assert c["availability_topic"] == f"unipi/{DN}/status"
    assert c["dev"]["identifiers"] == [DN]
    assert c["origin"]["sw"] == mod.SCRIPT_VERSION


def test_inverted_input_swaps_payloads(bridge, mod):
    from conftest import run, drain
    bridge.config.inputs = {"1_01": {"inverted": True}}
    run(bridge, bridge.perform_discovery_and_mqtt_subscribe())
    msgs = dict(drain(bridge.websocket_to_mqtt_queue))
    c = json.loads(msgs[f"{PFX}/binary_sensor/{DN}/di_1_01/config"])
    assert (c["payload_on"], c["payload_off"]) == ("OFF", "ON")


def test_relay_switch_discovery(discovered):
    c = cfg(discovered, "switch", "ro_xS51_01")
    assert c["command_topic"] == f"unipi/{DN}/ro/xS51_01/set"
    assert c["state_topic"] == f"unipi/{DN}/ro/xS51_01/state"
    assert (c["payload_on"], c["payload_off"], c["retain"]) == ("ON", "OFF", True)


def test_analog_output_is_json_light(discovered):
    c = cfg(discovered, "light", "ao_xS51_01")
    assert c["schema"] == "json"
    assert c["brightness"] is True
    assert c["brightness_scale"] == 1000  # 10 V range x 100
    assert c["supported_color_modes"] == ["brightness"]
    assert c["command_topic"] == f"unipi/{DN}/ao/xS51_01/set"


def test_led_is_on_off_light(discovered):
    c = cfg(discovered, "light", "led_1_01")
    assert c["command_topic"] == f"unipi/{DN}/led/1_01/set"
    assert "schema" not in c


def test_analog_input_sensor(discovered):
    c = cfg(discovered, "sensor", "ai_1_01")
    assert c["value_template"] == "{{ value_json.value }}"
    assert c["state_topic"] == f"unipi/{DN}/ai/1_01/state"


def test_onewire_sensors_per_key(discovered):
    c = cfg(discovered, "sensor", "1-wire_268CCC30020000F2_temp")
    assert c["device_class"] == "temperature" and c["unit_of_measurement"] == "°C"
    assert c["state_topic"] == f"unipi/{DN}/1-wire/268CCC30020000F2/temp"


def test_command_topics_subscribed(discovered):
    topics = discovered.mqtt_subscribe_topics
    assert len(topics) == 21  # do 4 + ro 5 + led 7 + ao 5
    assert f"unipi/{DN}/ro/xS51_05/set" in topics
    assert f"unipi/{DN}/ao/1_01/set" in topics
    assert not any("/di/" in t for t in topics)


def test_initial_states_published(discovered):
    from conftest import fx
    m = discovered.discovered_messages
    expected = "ON" if fx("di", "1_01") == 1 else "OFF"
    assert m[f"unipi/{DN}/di/1_01/state"] == expected
    assert json.loads(m[f"unipi/{DN}/ai/1_01/state"]) == {"value": fx("ai", "1_01")}
    ao = json.loads(m[f"unipi/{DN}/ao/1_01/state"])
    assert ao["color_mode"] == "brightness" and ao["state"] in ("ON", "OFF")
    assert json.loads(m[f"unipi/{DN}/1-wire/268CCC30020000F2/temp"])["value"] == fx("1wdevice", "268CCC30020000F2", "temp")
