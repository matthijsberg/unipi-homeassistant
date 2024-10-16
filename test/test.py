# source /home/unipi/unipi-homeassistant/bin/activate

import websockets
import paho.mqtt.client as mqtt
import asyncio
import threading
import json
import logging

# Load configuration from config.json
with open("config.json") as f:
    config = json.load(f)

# MQTT broker configuration
MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_USER = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_TOPIC = config["mqtt"]["topic"]

# Websocket server configuration
WEBSOCKET_HOST = config["websocket"]["host"]
WEBSOCKET_PORT = config["websocket"]["port"]
WEBSOCKET_URL = config["websocket"]["URL"]

# Function to modify the message (can be customized later)
def modify_message(message):
    return message

# MQTT client initialization
client = mqtt.Client()
client.username_pw_set(MQTT_USER, MQTT_PASSWORD)

# Logging setup
logging.basicConfig(level=logging.INFO)

# Websocket handler
async def websocket_handler(websocket, path):
    try:
        # Connect to MQTT broker
        await connect_to_mqtt()

        # Start MQTT thread
        mqtt_thread = threading.Thread(target=client.loop_start)
        mqtt_thread.start()

        async for message in websocket:
            # Modify message (can be customized later)
            modified_message = modify_message(message)

            # Publish message to MQTT
            await publish_to_mqtt(modified_message)

        # Receive messages from MQTT and publish to WebSocket
        while True:
            msg = await receive_from_mqtt()
            await websocket.send(json.dumps(msg))

    finally:
        # Disconnect from MQTT broker
        await disconnect_from_mqtt()
        mqtt_thread.join()

# Connect to MQTT broker
async def connect_to_mqtt():
    logging.info("Connecting to MQTT broker...")
    await asyncio.get_running_loop().run_in_executor(None, client.connect, MQTT_BROKER, MQTT_PORT)
    logging.info("Connected to MQTT broker")

# Publish message to MQTT
async def publish_to_mqtt(message):
    logging.info(f"Publishing message to MQTT: {message}")
    await asyncio.get_running_loop().run_in_executor(None, client.publish, MQTT_TOPIC, message)
    logging.info("Message published to MQTT")

# Receive message from MQTT
async def receive_from_mqtt():
    loop = asyncio.get_running_loop()
    future = loop.create_future()

    def on_message_callback(client, userdata, msg):
        future.set_result(json.loads(msg.payload.decode()))

    client.on_message = on_message_callback
    client.subscribe(MQTT_TOPIC)

    logging.info(f"Subscribing to MQTT topic: {MQTT_TOPIC}")

    result = await asyncio.wait_for(future, timeout=None)
    logging.info(f"Received message from MQTT: {result.result()}")
    return result.result()

# Main function
async def main():
    logging.info("Starting WebSocket server...")
    async with websockets.serve(websocket_handler, WEBSOCKET_HOST, WEBSOCKET_PORT):
        await asyncio.Future()  # Keep the server running indefinitely

if __name__ == "__main__":
    asyncio.run(main())