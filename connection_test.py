import threading
import time
import paho.mqtt.client as mqtt
import asyncio
import websockets
import queue
import json
import logging
import inspect
import requests

# Configure logging
log_file = "/var/log/connection_test.log"
log_level = logging.DEBUG
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

# Load configuration from JSON file
def load_config(config_file):
    with open(config_file, 'r') as f:
        return json.load(f)

config = load_config("config.json")

MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_USERNAME = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_TOPIC = config["mqtt"]["topic"]
WEBSOCKET_URL = config["websocket"]["url"]
UNIPI_URL = config["unipi_http"]["url"]  # Add UNIPI URL to config

# Logging Decorator
def log_function(func):
    def wrapper(*args, **kwargs):
        logging.debug(f"** Function: {func.__name__} | Start **")
        result = func(*args, **kwargs)
        logging.debug(f"** Function: {func.__name__} | End **")
        return result
    return wrapper

logging.debug(f"Using MQTT broker: {MQTT_BROKER}")
logging.debug(f"Using WebSocket server: {WEBSOCKET_URL}")
logging.debug(f"Using Unipi URL: {UNIPI_URL}")

# Couple of "global" vars to memorize some things from the initial discovery phase for later use 
device_name = "" #device name created 
mqtt_subscribe_topics = [] #array to store mqtt topics where we'll listen for on the bus (MQTT message to update unpi circuits over websocket)
#last_values = {} #store mqtt messages to detect changes and only send changed topics / values over the bus.

####################################
# FIRST BOOT MQTT - HASS DISCOVERY #
####################################

# Mapping of UNIPI device types to Home Assistant device types
DEVICE_TYPE_MAPPING = {
    "di": "binary_sensor",
    "do": "switch",
    "ai": "sensor",
    "ao": "number",
    "led": "light",
    #"wd": "switch",
    #"modbus_slave": "sensor",
    #"owbus": "switch"
}

# Function to retrieve JSON data from the UNIPI device via HTTP for initial run and publish all devices to HASSIO
@log_function
def get_unipi_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print("Failed to retrieve UNIPI data")
        return None

# Function to generate MQTT update (values) messages for Home Assistant
# This will create a message like homeassistant/number/Neuron_S103_2258/ao_1_01/state
# We'll get "number" with a match vs "dev" type, Neuron_S103_2258 = global var set in first run, ao_1_01 = dev + _ + circuit, state = fixed
@log_function
def generate_mqtt_topic(dev, circuit, topic_end):
    entity_id = (dev+"_"+circuit)
    device_type_mapped = DEVICE_TYPE_MAPPING[dev]
    return f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/{topic_end}"

@log_function
def json_to_mqtt_topics_discovery(mqtt_topic, json_data, device_info):
    global device_name
    global mqtt_subscribe_topics
    device_model = device_info.get('model', 'unknown')
    device_sn = device_info.get('sn', 'unknown')
    device_family = device_info.get('family', 'unknown')

    logging.debug(f"Extracted device info: model={device_model}, sn={device_sn}")
    device_name = f"{device_family}_{device_model}_{device_sn}"

    if json_data["dev"] in DEVICE_TYPE_MAPPING:
        device_type_mapped = DEVICE_TYPE_MAPPING[json_data["dev"]]
        entity_id = f"{json_data['dev']}_{json_data['circuit']}"
        active_mode = json_data.get("mode") 

        # Base config for all device types
        config = {
            "name": f"{json_data['dev']} {json_data['circuit']}",
            "unique_id": f"{device_name}_{json_data['dev']}_{json_data['circuit']}",
            "dev": {
                "identifiers": f"{device_name}",
                "name": f"Unipi {device_family} {device_model} ({device_sn})",
                "manufacturer": "Unipi Technology s.r.o.",
                "model": f"{device_family} {device_model}",
                "sn": f"{device_sn}"
            },
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": "0.1",
                "url": "https://github.com/matthijsberg/unipi-homeassistant"
            },
            "state_topic": f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/state"
        }

        # Update config based on device type
        if device_type_mapped == "binary_sensor":
            #config["value_template"] = "{{ value }}"
            config["payload_on"] = 1
            config["payload_off"] = 0
            #if json_data['circuit'] == 1:
            #    config["icon"] = "mdi:electric-switch-closed"
            #else:
            #    config["icon"] = "mdi:electric-switch"
            config["icon"] = "mdi:electric-switch"
        elif device_type_mapped == "switch":
            config["command_topic"] = f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/set"
            mqtt_subscribe_topics.append(config["command_topic"])  # Add set topic to the array of topics to subscribe on.
            config["payload_on"] = 1
            config["payload_off"] = 0
            config["icon"] = "mdi:electric-switch"
        elif device_type_mapped == "sensor" and json_data["dev"] == "ai":
            config["value_template"] = "{{ value_json.value }}"
            config["min"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[0]
            config["max"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[1]
            config["unit_of_measurement"] = json_data.get("modes", {}).get(active_mode, {}).get("unit")
            config["icon"] = "mdi:lightning-bolt-circle"
        elif device_type_mapped == "sensor":
            config["value_template"] = "{{ value_json.value }}"
        elif device_type_mapped == "number":
            config["min"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[0]
            config["max"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[1]
            config["unit_of_measurement"] = json_data.get("modes", {}).get(active_mode, {}).get("unit")
            config["command_topic"] = f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/set"
            mqtt_subscribe_topics.append(config["command_topic"])  # Add set topic to the array of topics to subscribe on. 
            config["value_template"] = "{{ value_json.value }}"
            config["icon"] = "mdi:lightning-bolt-circle"
        elif device_type_mapped == "light":
            config["command_topic"] = f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/set"
            mqtt_subscribe_topics.append(config["command_topic"])  # Add set topic to the array of topics to subscribe on.
            config["payload_on"] = 1
            config["payload_off"] = 0
            config["icon"] = "mdi:led-on"
        # else:  # No need for an empty else block

        #Publish topics and payload with HASS auto config settings 
        full_topic = generate_mqtt_topic(json_data.get('dev', 'unknown'), json_data.get('circuit', 'unknown'), "config")
        payload = json.dumps(config)
        websocket_to_mqtt_queue.put((full_topic, payload))

        # Publish the state of the circuit at initial run (updates of state will be handled via websocket)
        state_value = json_data.get("value")
        if "value_template" in config and config["value_template"] == "{{ value_json.value }}":         # Publish based on schema
            websocket_to_mqtt_queue.put((config["state_topic"], json.dumps({"value": state_value})))    # Publish as JSON with raw value
        else:
            websocket_to_mqtt_queue.put((config["state_topic"], state_value)) 

    logging.debug(f"Unipi device type found that is not mapped to HA MQTT device type, skipping")
    logging.debug(f"WebSocket subscribe topics {mqtt_subscribe_topics}")

# Function to handle discovery process
@log_function
def perform_discovery():
    global UNIPI_URL, MQTT_TOPIC, mqtt_subscribe_topics
    unipi_data = get_unipi_data(UNIPI_URL)
    if unipi_data:
        print(unipi_data)
        device_info = None
        for item in unipi_data:
            if item.get("dev") == "device_info":
                device_info = item
                break

        if device_info:
            for item in unipi_data:
                if item.get("dev") != "device_info":
                    json_to_mqtt_topics_discovery(MQTT_TOPIC, item, device_info)  # Pass device_info
        else:
            logging.warning("No device_info found in the Unipi data.")

    # Change the topics the MQTT bus is subscribed to (assuming we need to run it like this as we need the connection open during discovery)
    for topic in mqtt_subscribe_topics:
        mqtt_client.subscribe(topic, qos=1)

#######################################
# REALTIME WEBSOCKET TO MQTT MESSAGES #
#######################################

# --- MQTT Client Setup ---
@log_function
def on_mqtt_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        logging.info("Connected to MQTT Broker")
        # Add a wildcard topic to check if you're missing any messages
        #client.subscribe(f"{MQTT_TOPIC}/#")
    else:
        logging.error(f"Failed to connect to MQTT Broker: {reason_code}")

@log_function
def on_mqtt_message(client, userdata, message):
    logging.debug(f"Received MQTT message on topic {message.topic}: {message.payload.decode()}")
    message_payload = message.payload.decode()
    # Convert the MQTT message to a WebSocket update
    mqtt_to_websocket_converter(message.topic, message_payload)
    # Put the original message in the queue for other processing if needed
    mqtt_to_websocket_queue.put(message_payload)


# --- WebSocket Client Setup ---
@log_function
async def websocket_handler():
    global websocket_global
    async with websockets.connect(WEBSOCKET_URL) as websocket:
        logging.info("WebSocket connection established")
        websocket_global = websocket
        receive_task = asyncio.create_task(websocket_receive(websocket))
        await receive_task

# Function that listens on websockets, and publishes on MQTT
@log_function
async def websocket_receive(websocket):
    while True:
        try:
            message = await websocket.recv()
            logging.debug(f"Received WebSocket message: {message}")
            try:
                json_data = json.loads(message)
                logging.debug(f"JSON_DATA: {json_data}")

                # If the received data is a list, iterate through each item
                if isinstance(json_data, list):
                    for item in json_data:
                        if 'dev' not in item or 'circuit' not in item:
                            logging.warning("Missing keys 'dev' or 'circuit' in JSON item, ignoring message {json_data}!")
                            continue  # Skip processing this item if keys are missing
                        json_to_mqtt_topics(MQTT_TOPIC, item)
                else:
                    # Process single dictionary case (if not a list)
                    if 'dev' not in json_data or 'circuit' not in json_data:
                        logging.warning("Missing keys 'dev' or 'circuit' in JSON data, ignoring message {json_data}!")
                        continue  # Skip processing this message
                    json_to_mqtt_topics(MQTT_TOPIC, json_data)

            except json.JSONDecodeError as e:
                logging.error(f"Invalid JSON message received: {e}")
                return
        except asyncio.CancelledError:
            logging.debug("WebSocket receive cancelled")
            break

#Function to convert incomming MQTT messages and publish on websocket. 
@log_function
async def send_to_websocket(message):
    global websocket_global
    if websocket_global and websocket_global.open:
        logging.debug(f"Sending message to WebSocket: {message}")
        await websocket_global.send(message)
    else:
        logging.warning("WebSocket not connected, cannot send message.")

# Function to convert MQTT messages to WebSocket updates based on the device type and circuit
@log_function
def mqtt_to_websocket_converter(mqtt_topic, message_payload):
    try:
        # Assume the topic format is like "homeassistant/switch/Neuron_S103_2258/do_1_04/set"
        topic_parts = mqtt_topic.split('/')
        if len(topic_parts) < 5:
            logging.error(f"Invalid MQTT topic format: {mqtt_topic}")
            return

        # Extract dev and circuit from the entity_id part of the topic (e.g., "do_1_04")
        entity_id = topic_parts[3]
        dev, circuit = entity_id.split('_', 1)  # Split at the first underscore to get device type and circuit number

        # Extract value from the payload
        state_value = int(message_payload)

        # Construct the WebSocket message
        msg = {
            "cmd": "set",
            "dev": dev,          # E.g., 'do'
            "circuit": circuit,  # E.g., '1_04'
            "value": state_value       # The value received from MQTT payload for state
        }

        # Send the message via WebSocket
        logging.debug(f"Sending WebSocket update: {msg}")
        mqtt_to_websocket_queue.put(json.dumps(msg))

        # Sending the MQTT message to ack that we processed the MQTT to websocket message. 
        logging.debug(f"Sending MQTT message to ackonwledge switching on.")
        full_topic = generate_mqtt_topic(dev, circuit, "state")
        #payload = value #need some json here? 
        #websocket_to_mqtt_queue.put((full_topic, payload))
        #state_value = json_data.get("value")
        if "value_template" in config and config["value_template"] == "{{ value_json.value }}":         # Publish based on schema
            websocket_to_mqtt_queue.put((full_topic, json.dumps({"value": state_value})))    # Publish as JSON with raw value
        else:
            websocket_to_mqtt_queue.put((full_topic, state_value)) 


    except Exception as e:
        logging.error(f"Failed to convert MQTT message to WebSocket: {e}")



# --- Transformation Threads ---

@log_function
def json_to_mqtt_topics(mqtt_toplevel_topic, json_data):
    # Check if the JSON data is a list, if it is, split it up and run this fuction again to process each item separately
    if isinstance(json_data, list):
        for item in json_data:
            # Process each item in the list as a separate JSON object
            json_to_mqtt_topics(mqtt_toplevel_topic, item)
    else:
        if 'value' in json_data: # Check if the 'value' key exists in the JSON data, we only process value here, the rest is set duting initial run and we assume no other values update (we do not check device v / a / r etc. condif changes)
            # Extract dev and circuit from the JSON object
            dev = json_data.get('dev', 'unknown')
            circuit = json_data.get('circuit', 'unknown')
            value = json.dumps({"value": json_data['value']})
            # Form the full MQTT topic path
            full_topic = generate_mqtt_topic(dev, circuit, "state")
            # Publish the MQTT message
            websocket_to_mqtt_queue.put((f"{full_topic}", value))

# USE THIS to publish mqtt, not mqtt_client.publish
#websocket_to_mqtt_queue.put((f"{mqtt_toplevel_topic}/{combined_variable}/{topic}", value))

# --- Worker Threads ---
def mqtt_worker_thread():
    while not should_stop.is_set():
        topic, value = websocket_to_mqtt_queue.get()
        logging.debug(f"Publishing to MQTT: {topic} -> {value}")
        mqtt_client.publish(topic, value)
        websocket_to_mqtt_queue.task_done()

def websocket_worker_thread():
    while not should_stop.is_set():
        message = mqtt_to_websocket_queue.get()
        asyncio.run(send_to_websocket(message))
        mqtt_to_websocket_queue.task_done()

def mqtt_loop_thread():
    while not should_stop.is_set():
        mqtt_client.loop_start()
        #mqtt_client.loop()

# --- Main Thread ---
if __name__ == "__main__":

    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message

    mqtt_to_websocket_queue = queue.Queue()
    websocket_to_mqtt_queue = queue.Queue()
    should_stop = threading.Event()

    try:
        mqtt_client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        exit(1)

    mqtt_loop_thread = threading.Thread(target=mqtt_loop_thread)
    mqtt_loop_thread.daemon = True
    mqtt_loop_thread.start()

    mqtt_worker = threading.Thread(target=mqtt_worker_thread)
    mqtt_worker.daemon = True
    mqtt_worker.start()

    # Start MQTT client loop
    #mqtt_client.loop_start()

    websocket_worker = threading.Thread(target=websocket_worker_thread)
    websocket_worker.daemon = True
    websocket_worker.start()

    # Call the discovery function to create the HASSIO auto discovery messages and first values based current cirtcuit state.
    perform_discovery()

    asyncio.run(websocket_handler())

    try:
        should_stop.wait(timeout=5)
    except KeyboardInterrupt:
        logging.info("Received Ctrl-C, terminating...")

    should_stop.set()
