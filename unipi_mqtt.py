import paho.mqtt.client as mqtt
import json
import time  # Import the time library
import requests
import logging

# Configure logging
log_file = "/var/log/unipi-mqtt.log"
log_level = logging.DEBUG  # Change this line to set the desired log level
logging.basicConfig(filename=log_file, level=log_level,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# MQTT connection details
mqtt_broker = "YOUR_MQTT_BROKER_IP"
mqtt_port = 1883  # Default MQTT port
mqtt_username = "YOUR_MQTT_USERNAME"  # Optional, if authentication is enabled
mqtt_password = "YOUR_MQTT_PASSWORD"  # Optional, if authentication is enabled

# Home Assistant discovery prefix
discovery_prefix = "homeassistant"

# Device information for auto-discovery
device_id = "your_unique_device_id"  # Make sure this is unique
device_name = "AI0_01"
device_manufacturer = "Your Company"
device_model = "AI0_01 Sensor"

# Function to publish auto-discovery information
def publish_discovery_info(device_data, entity_data, entity_type, device_class=None):
    client = mqtt.Client()
    client.username_pw_set(mqtt_username, mqtt_password)
    client.connect(mqtt_broker, mqtt_port)

    device_info = device_data["device_info"]
    device_id = f"{device_info['family']}_{device_info['sn']}"

    # Construct the discovery topic
    discovery_topic = f"{discovery_prefix}/{entity_type}/{device_id}/{entity_data['circuit']}/config"

    # Create the discovery payload
    payload = {
        "name": f"{device_info['family']} {device_info['model']} {entity_data['dev'].upper()} {entity_data['circuit']}",
        "state_topic": f"{device_id}/{entity_data['dev']}/{entity_data['circuit']}",
        "unique_id": f"{device_id}_{entity_data['dev']}_{entity_data['circuit']}",
        "device": {
            "identifiers": [device_id],
            "name": f"{device_info['family']} {device_info['model']} {device_id}",
            "manufacturer": device_info['family'],
            "model": device_info['model'],
        },
    }

    if device_class:
        payload["device_class"] = device_class

    client.publish(discovery_topic, json.dumps(payload))
    client.disconnect()

#connect to websocket and load deivce and entity information to feed HASSIO.
def get_json_data():
    """Fetches JSON data from the specified URL."""
    url = "http://127.0.0.1:8080/json/all"
    try:
        logging.debug(f"Fetching data from {url}")  # Debug level logging
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        logging.debug(f"Received data: {data}")  # Debug level logging
        return data
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {url}: {e}")  # Error level logging
        return None


###### MAIN ######
logging.info("Starting UniPi MQTT script")  # Info level logging

# Load JSON data from the URL
data = get_json_data()
if data:
    logging.debug("Data fetched successfully")  # Debug level logging
    # ... (rest of your data processing) ...
else:
    logging.warning("Failed to fetch data")  # Warning level logging


# Iterate through entities and publish discovery information
for entity_data in data:
    device_type = entity_data["dev"]

    if device_type == "di":
        publish_discovery_info(data[-1], entity_data, "binary_sensor", "door")  # Assuming DI as door sensor
    elif device_type == "do":
        publish_discovery_info(data[-1], entity_data, "switch")
    elif device_type == "ai":
        publish_discovery_info(data[-1], entity_data, "sensor")
    elif device_type == "ao":
        publish_discovery_info(data[-1], entity_data, "sensor")
    # Add more elif blocks for other device types as needed

print("Home Assistant auto-discovery messages published.")


# Define the publish_data function
def publish_data(data):
    client = mqtt.Client()
    client.username_pw_set(mqtt_username, mqtt_password)  # Optional authentication
    client.connect(mqtt_broker, mqtt_port)
    client.publish("AI0_01", data)  # Make sure this topic is correct
    client.disconnect()

while True:
    # Replace this with your actual data retrieval logic
    sensor_data = "your_sensor_data"

    publish_data(sensor_data)

    # Adjust the sleep interval as needed
    time.sleep(60)
