import threading
import time
import paho.mqtt.client as mqtt
import asyncio
import websockets
import queue
import json
import logging

# Configure logging
log_file = "/var/log/connection_test.log"
log_level = logging.DEBUG  # Change this to adjust logging verbosity
logging.basicConfig(level=log_level,
                    format='%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')

# Load configuration from JSON file
def load_config(config_file):
    """Loads configuration from a JSON file."""
    with open(config_file, 'r') as f:
        config = json.load(f)
    return config

# Load configuration
config = load_config("config.json")  # Make sure to create this file

# Access configuration values
MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_USERNAME = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_TOPIC = config["mqtt"]["topic"]

WEBSOCKET_URL = config["websocket"]["url"]

# Thread-safe queues for communication
mqtt_to_websocket_queue = queue.Queue()
websocket_to_mqtt_queue = queue.Queue()

# --- MQTT Client Setup ---

def on_connect(client, userdata, flags, reason_code, properties):
    if flags.session_present:
        logging.info(f"MQTT Flasg present {flags}")
    if reason_code == 0:
        logging.info(f"Connected to MQTT Broker with result code {reason_code}")
        client.subscribe(MQTT_TOPIC)
    if reason_code > 0:
        logging.info(f"ERROR Connected to MQTT Broker with result code {reason_code}")

def on_message(client, userdata, message):
    message_payload = message.payload.decode()
    logging.debug(f"Received MQTT message: {message_payload}")
    mqtt_to_websocket_queue.put(message_payload)

mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message

# --- WebSocket Client Setup ---
async def websocket_handler():
    async with websockets.connect(WEBSOCKET_URL) as websocket:
        logging.info("WebSocket connection established")
        # Start receiving task
        receive_task = asyncio.create_task(websocket_receive(websocket))
        # Start sending task
        send_task = asyncio.create_task(websocket_send(websocket))
        # Run both tasks concurrently
        await asyncio.gather(receive_task, send_task)

async def websocket_receive(websocket):
    while True:
        message = await websocket.recv()
        logging.debug(f"Received WebSocket message: {message}")
        websocket_to_mqtt_queue.put(message)

async def websocket_send(websocket):
    while True:
        message = await asyncio.to_thread(mqtt_to_websocket_queue.get)
        logging.debug(f"Sending message to WebSocket: {message}")
        await websocket.send(message)
        mqtt_to_websocket_queue.task_done()

# --- Worker Threads ---
def mqtt_worker_thread():
    while True:
        message = websocket_to_mqtt_queue.get()
        logging.debug(f"Publishing to MQTT: {message}")
        mqtt_client.publish(MQTT_TOPIC, message)
        websocket_to_mqtt_queue.task_done()

def websocket_worker_thread():
    while True:
        message = mqtt_to_websocket_queue.get()
        logging.debug(f"Sending to WebSocket: {message}")
        asyncio.run(send_to_websocket(message))
        mqtt_to_websocket_queue.task_done()

async def send_to_websocket(message):
    """Helper function to send a message to the WebSocket server."""
    async with websockets.connect(WEBSOCKET_URL) as websocket:
        await websocket.send(message)

# --- Start Threads ---
if __name__ == "__main__":
    # Create the client with the desired CallbackAPIVersion
    client = mqtt_client.Client(client_id="unipi103") 

    # Start MQTT client in a separate thread
    mqtt_thread = threading.Thread(target=client.loop_forever)  # Use the created client instance
    mqtt_thread.daemon = True
    mqtt_thread.start()

    # Start WebSocket client in an event loop
    asyncio.run(websocket_handler())

    # Start worker threads
    mqtt_worker = threading.Thread(target=mqtt_worker_thread)
    mqtt_worker.daemon = True
    mqtt_worker.start()

    websocket_worker = threading.Thread(target=websocket_worker_thread)
    websocket_worker.daemon = True
    websocket_worker.start()

    # Keep the main thread alive (or handle other tasks)
    while True:
        time.sleep(1)
