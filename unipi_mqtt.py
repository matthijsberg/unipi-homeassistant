"""
This script bridges communication between a Unipi device (over Websocket) and an MQTT broker.

It handles device discovery for Home Assistant, message conversion between Websocket and MQTT,
and graceful shutdown on Ctrl+C.

Version 202503-011
"""

import threading
import time
import queue
import json
import logging
import logging.handlers
import os
import asyncio
import signal # Added for signal handling

import paho.mqtt.client as mqtt
import websockets
from websockets.protocol import State
import requests

from dataclasses import dataclass
from functools import wraps
from typing import Dict, Any, Tuple, Callable, List, Optional
import importlib # For checking library availability

# --- Check Required Libraries ---
REQUIRED_LIBRARIES = {
    "paho.mqtt": "paho-mqtt",
    "websockets": "websockets",
    "requests": "requests",
    "pydantic": "pydantic"
}

missing_libraries = []
for lib_name, install_name in REQUIRED_LIBRARIES.items():
    try:
        importlib.import_module(lib_name)
    except ImportError:
        missing_libraries.append((lib_name, install_name))

if missing_libraries:
    print("-------------------------------------------------------")
    print("ERROR: Required Python libraries are missing.")
    print("Please install them using pip:")
    for lib_name, install_name in missing_libraries:
        print(f"  - {lib_name} (install with: pip install {install_name})")
    print("  Alternatively, use your virtual environment's pip:")
    for lib_name, install_name in missing_libraries:
        print(f"    unipi-homeassistant/bin/pip install {install_name}")
    print("-------------------------------------------------------")
    exit(1)
else:
    # Import necessary components now that we know libraries exist
    from pydantic import BaseModel, Field, HttpUrl, ValidationError, field_validator
    # Other imports like paho.mqtt, websockets, requests are already done above
    print("Required libraries check passed.")

# --- Pydantic Configuration Models ---

class MqttConfig(BaseModel):
    broker: str
    port: int = 1883
    username: Optional[str] = None
    password: Optional[str] = None
    topic: str = "unipi" # Default base topic

class WebSocketConfig(BaseModel):
    url: str # Basic string validation

    @field_validator('url')
    @classmethod
    def check_websocket_url(cls, v: str) -> str:
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("WebSocket URL must start with ws:// or wss://")
        return v

class UnipiHttpConfig(BaseModel):
    url: HttpUrl # Use pydantic's HttpUrl for validation

class AppConfig(BaseModel):
    mqtt: MqttConfig
    websocket: WebSocketConfig
    # Use alias to allow 'unipi-http' or 'unipi_http' in JSON
    unipi_http: UnipiHttpConfig = Field(..., alias='unipi_http', validation_alias='unipi-http')

    model_config = {
        "populate_by_name": True, # Allows using alias
    }


# --- Websocket Version Check ---
WEBSOCKETS_MIN_VERSION = (14, 0)
current_version_str = websockets.__version__
current_version_tuple = tuple(map(int, current_version_str.split('.')))[:2]

if current_version_tuple < WEBSOCKETS_MIN_VERSION:
    logging.ERROR(
        f"Warning: Your 'websockets' library version is {current_version_str}. "
        f"Version {'.'.join(map(str, WEBSOCKETS_MIN_VERSION))}+ is recommended. "
        f"Upgrade with: pip install --upgrade websockets."
    )
else:
    logging.debug(
        f"Websockets library version check passed. Version: {current_version_str} "
        f"(required >= {'.'.join(map(str, WEBSOCKETS_MIN_VERSION))})."
    )
# --- End Websocket Version Check ---

# Configure logging with RotatingFileHandler.
log_dir = os.path.expanduser("~/.local/logs")
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "connection_test.log")
log_level = logging.ERROR

handler = logging.handlers.RotatingFileHandler(
    log_file, maxBytes=5 * 1024 * 1024, backupCount=3
)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.setLevel(log_level)
logger.addHandler(handler)


def test_logging() -> None:
    """Tests the logging setup."""
    logger.info("Starting logging test")
    try:
        with open(log_file, 'r') as f:
            pass  # Log file handling is tested, content printing is omitted
    except FileNotFoundError:
        print(f"Error: Log file '{log_file}' not found.")
    except PermissionError:
        print(f"Error: Permission denied accessing '{log_file}'.")
    except Exception as e:
        print(f"An error occurred: {e}")


test_logging()
logger.info("Ending logging test")


def load_config(config_file_name: str) -> Dict[str, Any]:
    """
    Loads configuration settings from a JSON file in the script's directory.

    Args:
        config_file_name: The name of the JSON configuration file (e.g., "config.json").

    Returns:
        A dictionary containing the configuration settings.

    Raises:
        FileNotFoundError: If the configuration file is not found.
        json.JSONDecodeError: If the configuration file contains invalid JSON.
        Exception: For other errors during file loading.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, config_file_name)

    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError as e:
        logger.error(f"Config file '{config_file_name}' not found in: {script_dir}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON from: '{config_path}'. Check JSON validity.")
        raise
    except Exception as e:
        logger.error(f"Error loading config file '{config_path}': {e}", exc_info=True)
        raise


def log_function(func: Callable[..., Any]) -> Callable[..., Any]:
    """
    Decorator to log function execution start and end (currently commented out).

    Args:
        func: The function to be decorated.

    Returns:
        The wrapper function.
    """
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        # logger.debug(f"** Function: {func.__name__} | Start **")
        result = func(*args, **kwargs)
        # logger.debug(f"** Function: {func.__name__} | End **")
        return result
    return wrapper


config = load_config("config.json")

MQTT_BROKER = config["mqtt"]["broker"]
MQTT_PORT = config["mqtt"]["port"]
MQTT_USERNAME = config["mqtt"]["username"]
MQTT_PASSWORD = config["mqtt"]["password"]
MQTT_TOPIC = config["mqtt"]["topic"]
WEBSOCKET_URL = config["websocket"]["url"]
UNIPI_URL = config["unipi_http"]["url"]

logger.debug(f"Using MQTT broker: {MQTT_BROKER}")
logger.debug(f"Using WebSocket server: {WEBSOCKET_URL}")
logger.debug(f"Using Unipi URL: {UNIPI_URL}")

# Global Vars
device_name: str = ""
mqtt_subscribe_topics: List[str] = []
websocket_global: Optional["websockets.WebSocketClientProtocol"] = None # Use string literal for type hint
should_stop = threading.Event()
is_first_connect: bool = True
# Dictionary to track the latest desired value for ongoing AO transitions
# Key: (dev, circuit) tuple, Value: target brightness (int 0-1000)
active_ao_transitions: Dict[Tuple[str, str], int] = {}


####################################
# SECTION 1: FOUNDATIONAL FUNCTIONS #
####################################


@log_function
async def websocket_connect(url: str) -> Optional["websockets.WebSocketClientProtocol"]: # Use string literal for type hint
    """
    Establishes a WebSocket connection with retries.
    (Note: This function is currently unused, connection handled in websocket_handler)
    """
    retries = 0
    while retries < 5 and not should_stop.is_set():
        try:
            websocket = await websockets.connect(url)
            logging.info("WebSocket connection established")
            return websocket
        except (websockets.exceptions.ConnectionClosedError, OSError, ConnectionRefusedError) as e:
            logging.error(f"WebSocket connection failed: {e}")
            retries += 1
            wait_time = 2**retries
            logging.warning(f"Retrying WebSocket connection in {wait_time} seconds (attempt {retries})...")
            await asyncio.sleep(wait_time)
    if should_stop.is_set():
        logging.info("WebSocket connection attempts stopped due to shutdown signal.")
        print("WebSocket connection attempts stopped due to shutdown signal.")
    else:
        logging.critical("Failed to establish WebSocket connection after multiple retries.")
        print("Failed to establish WebSocket connection after multiple retries.")
    return None


@log_function
def mqtt_connect() -> mqtt.Client:
    """
    Sets up and connects the MQTT client.

    Returns:
        An MQTT client object.
    """
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    mqtt_client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    mqtt_client.on_connect = on_mqtt_connect
    mqtt_client.on_message = on_mqtt_message
    mqtt_client.on_disconnect = on_mqtt_disconnect
    try:
        mqtt_client.connect_async(MQTT_BROKER, MQTT_PORT, keepalive=60)
    except Exception as e:
        logging.error(f"Failed to connect to MQTT broker: {e}")
        exit(1)
    return mqtt_client


@log_function
def on_mqtt_connect(client: mqtt.Client, userdata: Any, flags: Dict[str, Any], reason_code: int, properties: mqtt.Properties) -> None:
    """
    Callback function for MQTT connection events.

    Subscribes to MQTT topics on successful connection and handles first connection delay.

    Args:
        client: The MQTT client instance.
        userdata: User data (not used).
        flags: Connection flags.
        reason_code: Connection reason code.
        properties: Connection properties.
    """
    logger.info(f"on_mqtt_connect called with reason code: {reason_code}, flags: {flags}")
    if reason_code == 0:
        # Reason code 0 means successful connection
        logger.info(f"Successfully connected to MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")

        # Start timer to ignore initial retained messages
        def set_first_connect_false_delayed() -> None:
            """Sets is_first_connect to False after a delay."""
            logger.debug("Starting delay timer for initial retained MQTT messages...")
            time.sleep(2) # Wait 2 seconds
            global is_first_connect
            if is_first_connect: # Check flag again in case it was changed elsewhere
                logger.info("Initial MQTT message delay timer expired. Processing all messages now.")
                is_first_connect = False
            else:
                logger.debug("Initial MQTT message delay timer expired, but is_first_connect was already False.")

        timer_thread = threading.Thread(target=set_first_connect_false_delayed, name="MqttRetainedMsgTimer")
        timer_thread.daemon = True
        timer_thread.start()

        # Subscribe to necessary topics
        logger.info(f"Subscribing to {len(mqtt_subscribe_topics)} MQTT topics...")
        for topic in mqtt_subscribe_topics:
            logger.debug(f"Subscribing to MQTT topic: {topic}")
            result, mid = client.subscribe(topic, qos=1)
            if result == mqtt.MQTT_ERR_SUCCESS:
                logger.debug(f"Successfully initiated subscription to {topic} (MID: {mid})")
            else:
                logger.error(f"Failed to initiate subscription to {topic}. Error code: {result}")
    else:
        # Log connection failure reason
        # Paho reason codes: https://github.com/eclipse/paho.mqtt.python/blob/master/src/paho/mqtt/client.py
        logger.error(f"Failed to connect to MQTT Broker {MQTT_BROKER}:{MQTT_PORT}. Reason Code: {reason_code} ({mqtt.connack_string(reason_code)})")
        # Depending on the reason code, might want to exit or let reconnect logic handle it


def create_queues() -> Tuple[queue.Queue[str], queue.Queue[Tuple[str, str]]]:
    """
    Creates and returns the message queues for MQTT and WebSocket communication.

    Returns:
        A tuple containing:
            - mqtt_to_websocket_queue: Queue for messages from MQTT to WebSocket.
            - websocket_to_mqtt_queue: Queue for messages from WebSocket to MQTT.
    """
    mqtt_to_websocket_queue: queue.Queue[str] = queue.Queue()
    websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]] = queue.Queue()
    return mqtt_to_websocket_queue, websocket_to_mqtt_queue


def start_worker_threads(
    websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]],
    mqtt_to_websocket_queue: queue.Queue[str],
    loop: asyncio.AbstractEventLoop
) -> None:
    """
    Starts worker threads for MQTT and WebSocket message processing.

    Args:
        websocket_to_mqtt_queue: Queue for messages from WebSocket to MQTT.
        mqtt_to_websocket_queue: Queue for messages from MQTT to WebSocket.
        loop: Asyncio event loop.
    """
    mqtt_worker = threading.Thread(target=mqtt_worker_thread, args=(websocket_to_mqtt_queue,))
    mqtt_worker.daemon = True
    mqtt_worker.start()

    websocket_worker = threading.Thread(
        target=websocket_worker_thread, args=(mqtt_to_websocket_queue, loop)
    )
    websocket_worker.daemon = True
    websocket_worker.start()


def start_mqtt_loop(mqtt_client: mqtt.Client) -> None:
    """
    Starts the MQTT client's network loop in a separate thread.

    Args:
        mqtt_client: The MQTT client instance.
    """
    mqtt_loop_thread_instance = threading.Thread(
        target=mqtt_loop_thread, args=(mqtt_client,), name="MQTTLoopThread"
    )
    mqtt_loop_thread_instance.daemon = True
    mqtt_loop_thread_instance.start()


def mqtt_loop_thread(mqtt_client: mqtt.Client) -> None:
    """
    Runs the MQTT client's network loop, handling reconnection logic.

    Args:
        mqtt_client: The MQTT client instance.
    """
    reconnect_delay = 1
    while not should_stop.is_set():
        try:
            mqtt_client.reconnect()
            while not should_stop.is_set():
                mqtt_client.loop(timeout=1.0)
            reconnect_delay = 1
        except (mqtt.WebsocketConnectionError, OSError) as e:
            logging.error(f"MQTT connection error: {e}. Retrying in {reconnect_delay} seconds...")
            time.sleep(reconnect_delay)
            reconnect_delay = min(reconnect_delay * 2, 60)
        except Exception as e:
            logging.exception(f"Unexpected error in MQTT loop: {e}")
            break


def mqtt_worker_thread(websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]]) -> None:
    """
    Worker thread to publish messages from the queue to the MQTT broker.

    Args:
        websocket_to_mqtt_queue: Queue for messages from WebSocket to MQTT.
    """
    logger.info("MQTT worker thread started.")
    while not should_stop.is_set():
        try:
            # Wait for a message from the WebSocket handler
            topic, value = websocket_to_mqtt_queue.get(timeout=1)
            logger.debug(f"MQTT Worker: Dequeued message for topic '{topic}'. Publishing...")
            # Publish to MQTT broker
            mqtt_client.publish(topic, value, qos=1) # Assuming QoS 1 is desired
            logger.debug(f"MQTT Worker: Published to '{topic}' -> {value}")
            websocket_to_mqtt_queue.task_done()
        except queue.Empty:
            # No message in queue, loop continues
            continue
        except Exception as e:
            logger.error(f"Error in mqtt_worker_thread: {e}", exc_info=True)
            # Log error but continue processing next message
            # Ensure task_done is called even on error if item was retrieved?
            try:
                websocket_to_mqtt_queue.task_done()
            except ValueError: # task_done called too many times
                pass
            except Exception as td_err: # Other potential errors
                logger.error(f"Error calling task_done in mqtt_worker_thread exception handler: {td_err}")
            continue # Continue to next loop iteration
    logger.info("MQTT worker thread finished.")


def websocket_worker_thread(
    mqtt_to_websocket_queue: queue.Queue[str], loop: asyncio.AbstractEventLoop
) -> None:
    """
    Worker thread to send messages from the queue to the WebSocket server.

    Args:
        mqtt_to_websocket_queue: Queue for messages from MQTT to WebSocket.
        loop: Asyncio event loop.
    """
    logger.info("WebSocket worker thread started.")
    global websocket_global
    while not should_stop.is_set():
        try:
            # Wait for a message converted from MQTT
            message = mqtt_to_websocket_queue.get(timeout=1)
            logger.debug(f"WebSocket Worker: Dequeued message: {message}")

            # Check if WebSocket is connected and open
            ws = websocket_global # Local reference for check
            if ws is not None and ws.state == State.OPEN:
                logger.debug("WebSocket Worker: Scheduling message send via call_soon_threadsafe.")
                # Schedule the async send_to_websocket coroutine to run in the event loop
                loop.call_soon_threadsafe(asyncio.create_task, send_to_websocket(message))
                # Note: Error handling for the send itself is within send_to_websocket
            else:
                # Log error if WebSocket is not available
                state_str = str(ws.state) if ws else 'None'
                logger.error(f"WebSocket Worker: WebSocket unavailable for sending message. State: {state_str}. Message dropped: {message}")
                # Consider adding message to a retry queue or handling differently

            mqtt_to_websocket_queue.task_done()
        except queue.Empty:
            # No message in queue, loop continues
            continue
        except Exception as e:
            logger.error(f"Error in websocket_worker_thread: {e}", exc_info=True)
            # Log error but continue processing next message
            # Ensure task_done is called even on error if item was retrieved
            try:
                mqtt_to_websocket_queue.task_done()
            except ValueError: # task_done called too many times
                pass
            except Exception as td_err: # Other potential errors
                logger.error(f"Error calling task_done in websocket_worker_thread exception handler: {td_err}")
            continue # Continue to next loop iteration
    logger.info("WebSocket worker thread finished.")


#################################################
# SECTION 2: HOME ASSISTANT AUTODISCOVERY LOGIC #
#################################################

DEVICE_TYPE_MAPPING: Dict[str, str] = {
    "di": "binary_sensor",
    "do": "switch",
    "ro": "switch",
    "ai": "sensor",
    "ao": "light",
    "led": "light",
    "temp": "sensor" # Added for 1-Wire temperature sensors
}


@log_function
def get_unipi_data(dev: Optional[str] = None, circuit: Optional[str] = None, scope: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """
    Fetches JSON data from the UNIPI device via HTTP.

    Args:
        dev: Device type (e.g., "di", "do").
        circuit: Circuit identifier (e.g., "1", "2").
        scope: Data scope ("all", "circuit", "value").

    Returns:
        A dictionary containing the JSON response from UNIPI, or None on failure.
    """
    global UNIPI_URL
    logger.debug(f"get_unipi_data called with: dev={dev}, circuit={circuit}, scope={scope}")
    UNIPI_REQ_URL: str = ""
    if scope == "all":
        UNIPI_REQ_URL = f"{UNIPI_URL}/{scope}"
    elif dev and circuit and scope == "circuit":
        UNIPI_REQ_URL = f"{UNIPI_URL}/{dev}/{circuit}"
    elif dev and circuit and scope == "value":
        UNIPI_REQ_URL = f"{UNIPI_URL}/{dev}/{circuit}/{scope}"
    else:
        logging.warning(f"Invalid get_unipi_data parameters: dev={dev}, circuit={circuit}, scope={scope}. Returning None.")
        return None

    retries = 0
    max_retries = 3
    retry_delay = 2

    while retries <= max_retries:
        logger.debug(f"Attempting UNIPI request (attempt {retries+1}/{max_retries+1}): {UNIPI_REQ_URL}")
        try:
            response = requests.get(UNIPI_REQ_URL, timeout=10) # Added timeout
            response.raise_for_status() # Raises HTTPError for bad responses (4xx or 5xx)
            logger.debug(f"UNIPI request successful: {UNIPI_REQ_URL}")
            return response.json()
        except requests.exceptions.ConnectionError as e:
            # Specific error for network problems (e.g., DNS failure, refused connection)
            logger.error(f"Connection error to UNIPI at {UNIPI_REQ_URL} (attempt {retries+1}/{max_retries+1}): {e}")
            retries += 1
            if retries <= max_retries:
                wait_time = retry_delay * (retries)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached for UNIPI connection. Giving up.")
                logger.debug("get_unipi_data returning None after max connection retries.")
                return None
        except requests.exceptions.Timeout as e:
            # Specific error for timeouts
            logger.error(f"Timeout error connecting to UNIPI at {UNIPI_REQ_URL} (attempt {retries+1}/{max_retries+1}): {e}")
            retries += 1
            if retries <= max_retries:
                wait_time = retry_delay * (retries)
                logger.info(f"Retrying connection after timeout in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error(f"Max retries reached for UNIPI connection after timeout. Giving up.")
                logger.debug("get_unipi_data returning None after max timeout retries.")
                return None
        except requests.exceptions.RequestException as e:
            # Catch-all for other request issues (e.g., invalid URL, HTTP errors if raise_for_status wasn't used)
            logger.error(f"UNIPI data retrieval failed (RequestException) for {UNIPI_REQ_URL}: {e}", exc_info=True)
            logger.debug("get_unipi_data returning None due to RequestException.")
            return None
        except json.JSONDecodeError as e:
            # Error parsing the JSON response from Unipi
            logger.error(f"Failed to decode UNIPI JSON response from {UNIPI_REQ_URL}: {e}", exc_info=True)
            # Optionally log the raw response text if needed for debugging (might be large)
            # logger.debug(f"Raw response text: {response.text}")
            logger.debug("get_unipi_data returning None due to JSONDecodeError.")
            return None

    # This is reached if the loop completes without returning (shouldn't happen with current logic)
    logger.warning(f"get_unipi_data loop completed without success for {UNIPI_REQ_URL}. Returning None.")
    return None


@log_function
def perform_discovery_and_mqtt_subscribe(
    websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]], mqtt_client: mqtt.Client
) -> None:
    """
    Performs device discovery and subscribes to MQTT topics for discovered devices.
    """
    global MQTT_TOPIC, mqtt_subscribe_topics
    unipi_data = get_unipi_data(scope="all")
    if unipi_data:
        device_info = next((item for item in unipi_data if item.get("dev") == "device_info"), None)

        if device_info:
            for item in unipi_data:
                if item.get("dev") != "device_info":
                    publish_discovery_config(
                        MQTT_TOPIC, item, device_info, websocket_to_mqtt_queue
                    )
        else:
            logging.warning("No device_info found in the Unipi data.")

    for topic in mqtt_subscribe_topics:
        mqtt_client.subscribe(topic, qos=1)


@log_function
def publish_discovery_config(
    mqtt_topic: str,
    json_data: Dict[str, Any],
    device_info: Dict[str, Any],
    websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]]
) -> None:
    """
    Generates and sends Home Assistant MQTT discovery messages based on UNIPI device data.
    """
    required_keys = ['dev', 'circuit']
    if not all(key in json_data for key in required_keys):
        missing_keys = [key for key in required_keys if key not in json_data]
        logging.warning(f"Missing required keys: {missing_keys}. Skipping device. Missing: {missing_keys}")
        return

    try:
        global device_name, mqtt_subscribe_topics

        device_model = device_info.get('model', 'unknown')
        device_sn = device_info.get('sn', 'unknown')
        device_family = device_info.get('family', 'unknown')
        device_name = f"{device_family}_{device_model}_{device_sn}"

        device_type_mapped = DEVICE_TYPE_MAPPING.get(json_data["dev"])
        if not device_type_mapped:
            logging.warning(f"Unipi device type {json_data['dev']} not mapped. Skipping.")
            return

        entity_id = f"{json_data['dev']}_{json_data['circuit']}"
        active_mode = json_data.get("mode")

        config: Dict[str, Any] = {
            "name": f"{json_data['dev']} {json_data['circuit']}",
            "unique_id": f"{device_name}_{json_data['dev']}_{json_data['circuit']}",
            "dev": {
                "identifiers": device_name,
                "name": f"Unipi {device_family} {device_model} ({device_sn})",
                "manufacturer": "Unipi Technology s.r.o.",
                "model": f"{device_family} {device_model}",
                "sn": device_sn
            },
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": "0.1",
                "url": "https://github.com/matthijsberg/unipi-homeassistant"
            },
            # Generate state topic using the updated function for consistency
            "state_topic": generate_mqtt_topic_update(json_data['dev'], json_data['circuit'], "state"),
            "qos": 1,
            # --- Add Availability ---
            "availability_topic": f"unipi/{device_name}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
            # --- End Add Availability ---
        }

        if device_type_mapped == "binary_sensor":
            config["payload_on"] = "ON"
            config["payload_off"] = "OFF"
            config["icon"] = "mdi:electric-switch"
        elif device_type_mapped == "switch":
            config["command_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/set"
            mqtt_subscribe_topics.append(config["command_topic"])
            config["payload_on"] = "ON"
            config["payload_off"] = "OFF"
            config["icon"] = "mdi:electric-switch"
            config["retain"] = "true"
        elif device_type_mapped == "sensor" and json_data["dev"] == "ai":
            config["value_template"] = "{{ value_json.value }}"
            config["min"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[0]
            config["max"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[1]
            config["unit_of_measurement"] = json_data.get("modes", {}).get(active_mode, {}).get("unit")
            config["icon"] = "mdi:lightning-bolt-circle"
        elif device_type_mapped == "sensor": # Includes 'temp' now
            config["value_template"] = "{{ value_json.value }}"
            # Add unit specifically for temperature sensors
            if json_data["dev"] == "temp":
                config["unit_of_measurement"] = "°C" # Assuming Celsius
                config["device_class"] = "temperature" # Set device class for better HA integration
                config["icon"] = "mdi:thermometer" # More specific icon
        elif device_type_mapped == "number":
            config["min"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[0]
            config["max"] = json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[1]
            config["unit_of_measurement"] = json_data.get("modes", {}).get(active_mode, {}).get("unit")
            config["command_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/set"
            mqtt_subscribe_topics.append(config["command_topic"])
            config["value_template"] = "{{ value_json.value }}"
            config["icon"] = "mdi:lightning-bolt-circle"
        elif device_type_mapped == "light":
            if json_data["dev"] == "led":
                config["command_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/set"
                mqtt_subscribe_topics.append(config["command_topic"])
                config["payload_on"] = "ON"
                config["payload_off"] = "OFF"
                config["icon"] = "mdi:led-on"
                config["retain"] = "true"
            elif json_data["dev"] == "ao":
                unit_of_measurement = json_data.get("modes", {}).get(active_mode, {}).get("unit")
                if unit_of_measurement == "V":
                    brightness_scale = (json_data.get("modes", {}).get(active_mode, {}).get("range", [0, 100])[1]) * 100
                    config["brightness_scale"] = brightness_scale
                    config["command_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/set"
                    mqtt_subscribe_topics.append(config["command_topic"])
                    config["brightness"] = True
                    config["brightness_state_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/output"
                    config["brightness_command_topic"] = f"unipi/{device_name}/{json_data['dev']}/{json_data['circuit']}/output/set"
                    mqtt_subscribe_topics.append(config["brightness_command_topic"])
                    config["icon"] = "mdi:lightning-bolt-circle"
                    config["on_command_type"] = "brightness"
                    config["retain"] = "true"
                else:
                    logging.error("Device is LIGHT but unit is not Voltage for Analog Output.")

        full_topic = generate_mqtt_topic_discovery(
            json_data.get('dev', 'unknown'), json_data.get('circuit', 'unknown'), "config"
        )
        payload = json.dumps(config, indent=None)
        websocket_to_mqtt_queue.put((full_topic, payload))

        state_value = json_data.get("value")
        dev_type = json_data.get("dev")

        if dev_type and dev_type.lower() == "ai":
            websocket_to_mqtt_queue.put((config["state_topic"], str(state_value)))
        else:
            if dev_type and dev_type.lower() == "ao":
                state_value = int(state_value * 100) # Ensure int for brightness
                state_value = max(0, min(state_value, 1000))
                websocket_to_mqtt_queue.put((config["brightness_state_topic"], str(state_value)))
            if state_value == 0:
                websocket_to_mqtt_queue.put((config["state_topic"], "OFF"))
            else:
                websocket_to_mqtt_queue.put((config["state_topic"], "ON"))

    except Exception as e:
        logging.error(f"Error generating MQTT discovery topic: {e}")


@log_function
def generate_mqtt_topic_discovery(dev: str, circuit: str, topic_end: str) -> str:
    """
    Generates MQTT discovery topic for Home Assistant.

    Args:
        dev: Device type (e.g., "di").
        circuit: Circuit identifier (e.g., "1").
        topic_end: Topic suffix (e.g., "config").

    Returns:
        The generated MQTT discovery topic string.
    """
    device_type_mapped = DEVICE_TYPE_MAPPING.get(dev, "device") # Default to 'device' if not found

    if dev == "temp":
        # Special entity ID and topic structure for 1-Wire temperature sensors
        # circuit here is the 1-wire sensor ID (e.g., 28ABA97E0E000033)
        entity_id = f"1-wire_{circuit}_{dev}" # Unique ID including '1-wire'
        return f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/{topic_end}"
    else:
        # Standard entity ID and topic structure for other devices
        entity_id = f"{dev}_{circuit}"
        return f"homeassistant/{device_type_mapped}/{device_name}/{entity_id}/{topic_end}"


###################################################
# SECTION 3: INCOMING WEBSOCKET MESSAGE PROCESSING #
###################################################


async def websocket_handler(websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]]) -> None:
    """
    Handles incoming WebSocket messages, receiving and processing them in a loop.

    Args:
        websocket_to_mqtt_queue: Queue for messages to MQTT.
    """
    global WEBSOCKET_URL, websocket_global

    reconnect_delay = 1
    max_reconnect_delay = 60 # Max delay 1 minute

    while not should_stop.is_set():
        websocket_global = None # Ensure it's None at the start of each attempt
        try:
            logging.info(f"Attempting to connect to WebSocket: {WEBSOCKET_URL}")
            # Set longer timeouts for connect, ping/pong
            async with websockets.connect(
                WEBSOCKET_URL,
                ping_interval=20, # Send pings more often
                ping_timeout=10, # Shorter timeout for ping replies
                open_timeout=15, # Timeout for initial connection
                close_timeout=10 # Timeout for closing handshake
            ) as websocket:
                websocket_global = websocket # Assign to global var
                logging.info(f"WebSocket connection established: {WEBSOCKET_URL}")
                reconnect_delay = 1 # Reset delay on successful connection

                # --- Start Message Handling Loop ---
                logging.debug("Starting WebSocket message handling loop...")
                while not should_stop.is_set():
                    try:
                        # Wait for a message
                        message = await websocket.recv()
                        # Consider adding a timeout to recv if needed: await asyncio.wait_for(websocket.recv(), timeout=60)
                        logging.debug(f"Received WebSocket message (raw): {message}")
                        try:
                            json_message = json.loads(message)
                            logging.debug(f"Processing WebSocket JSON: {json_message}")
                            websocket_json_to_mqtt_topics(MQTT_TOPIC, json_message, websocket_to_mqtt_queue)
                            logging.debug("Finished processing WebSocket message.")
                        except json.JSONDecodeError as e:
                            logging.error(f"Could not decode JSON from websocket message: {message}", exc_info=True)
                        except Exception as proc_err: # Catch errors during processing
                            logging.exception(f"Error processing websocket message: {proc_err}")

                    except asyncio.TimeoutError:
                        # This would catch timeout from asyncio.wait_for if used above
                        logging.warning("Timeout waiting for WebSocket message.")
                        # Decide if we should continue or break/reconnect
                        continue # Continue waiting for now
                    except asyncio.CancelledError:
                        logging.info("WebSocket receive task cancelled.")
                        # should_stop should be set by the cancellation source
                        break # Exit inner loop
                    except websockets.exceptions.ConnectionClosedOK:
                        logging.info("WebSocket connection closed normally by peer (ConnectionClosedOK).")
                        break # Exit inner loop to reconnect/exit
                    except websockets.exceptions.ConnectionClosedError as e:
                        logging.warning(f"WebSocket connection closed with error: {e}. Breaking inner loop.")
                        break # Exit inner loop to reconnect/exit
                    except Exception as e:
                        # Catch unexpected errors during receive
                        logging.exception(f"Unexpected error in WebSocket receive loop: {e}")
                        break # Exit inner loop to reconnect/exit
                    # Removed sleep(0.01) - recv() should block appropriately

                # --- End Message Handling Loop ---
                logging.debug("WebSocket message handling loop finished.")
                # Check why the inner loop exited
                if should_stop.is_set():
                    logging.info("WebSocket handler inner loop exited due to shutdown signal.")
                    break # Exit outer loop as well
                else:
                    logging.info("WebSocket handler inner loop exited for other reason (e.g., connection closed). Will attempt reconnect.")
                    # Outer loop will handle reconnect after delay

        # --- Outer Exception Handling (Connection/Retry Logic) ---
        except (websockets.exceptions.InvalidURI, websockets.exceptions.InvalidHandshake) as e:
            # Permanent errors - stop trying to connect
            logging.critical(f"WebSocket connection failed (Permanent Error): {e}. Exiting handler.")
            should_stop.set() # Signal other parts to stop
            break # Exit outer loop
        except (websockets.exceptions.ConnectionClosedError, OSError, ConnectionRefusedError, asyncio.TimeoutError) as e:
            # Temporary errors - attempt to reconnect
            logging.error(f"WebSocket connection/establishment failed: {e}. Retrying...")
            # Clear global websocket var if it exists and is closed
            if websocket_global and websocket_global.closed:
                logging.debug("Clearing websocket_global reference after connection failure.")
                websocket_global = None
            # Wait before retrying connection
            logging.info(f"Waiting {reconnect_delay} seconds before retrying WebSocket connection...")
            try:
                await asyncio.sleep(reconnect_delay)
            except asyncio.CancelledError:
                 logging.info("WebSocket handler sleep cancelled during reconnect delay.")
                 should_stop.set()
                 break # Exit outer loop
            reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay) # Exponential backoff
            logging.info(f"Next WebSocket reconnect attempt delay: {reconnect_delay} seconds.")
        except asyncio.CancelledError:
            logging.info("WebSocket handler task cancelled during connection attempt/retry.")
            should_stop.set()
            break # Exit outer loop
        except Exception as handler_exception:
            logging.exception(f"Unexpected error in websocket_handler outer loop: {handler_exception}")
            break
        finally:
            logging.info("websocket_handler outer loop iteration finished.")
        if should_stop.is_set():
            logging.info("WebSocket handler outer loop stopped due to shutdown signal.")
            break

    logging.info("websocket_handler exiting.")


@log_function
def websocket_json_to_mqtt_topics(
    mqtt_toplevel_topic: str,
    json_data: Any,
    websocket_to_mqtt_queue: queue.Queue[Tuple[str, str]]
) -> None:
    """
    Converts WebSocket JSON messages to MQTT topics and pushes them to the queue.
    """
    if isinstance(json_data, list):
        for item in json_data:
            websocket_json_to_mqtt_topics(mqtt_toplevel_topic, item, websocket_to_mqtt_queue)
    else:
        if 'value' in json_data:
            dev = json_data.get('dev', 'unknown')
            circuit = json_data.get('circuit', 'unknown')
            raw_value = json_data['value'] # Keep the original value from WebSocket
            logger.debug(f"Processing WebSocket update for {dev}/{circuit}: Value={raw_value}")

            state_topic = generate_mqtt_topic_update(dev, circuit, "state")

            if dev == "ao":
                # Handle Analog Output: Publish brightness to /output, state to /state
                brightness_topic = generate_mqtt_topic_update(dev, circuit, "output")
                try:
                    # Scale 0-10V from WebSocket to 0-1000 for MQTT
                    brightness_value = int(float(raw_value) * 100)
                    brightness_value = max(0, min(brightness_value, 1000)) # Clamp
                    state_value = "ON" if brightness_value > 0 else "OFF"

                    logger.debug(f"WS->MQTT (AO): Publishing brightness {brightness_value} to {brightness_topic}")
                    websocket_to_mqtt_queue.put((brightness_topic, str(brightness_value)))
                    logger.debug(f"WS->MQTT (AO): Publishing state {state_value} to {state_topic}")
                    websocket_to_mqtt_queue.put((state_topic, state_value))
                except (ValueError, TypeError) as e:
                    logger.error(f"WS->MQTT (AO) Error: Could not process value '{raw_value}' for {dev}/{circuit}: {e}")

            elif dev in ["di", "do", "ro", "led"]:
                # Handle ON/OFF types: Publish state to /state
                try:
                    # Assume 1 means ON, 0 means OFF from WebSocket
                    state_value = "ON" if int(float(raw_value)) == 1 else "OFF"
                    logger.debug(f"WS->MQTT ({dev}): Publishing state {state_value} to {state_topic}")
                    websocket_to_mqtt_queue.put((state_topic, state_value))
                except (ValueError, TypeError) as e:
                     logger.error(f"WS->MQTT ({dev}) Error: Could not process value '{raw_value}' for {dev}/{circuit}: {e}")
            else:
                # For other types (like 'ai'), publish raw value wrapped in JSON to /state topic
                # This matches the original behavior for sensors
                logger.debug(f"WS->MQTT ({dev}): Publishing raw JSON value to {state_topic}")
                value_json = json.dumps({"value": raw_value})
                websocket_to_mqtt_queue.put((state_topic, value_json))


#################################################
# SECTION 4: INCOMING MQTT MESSAGE PROCESSING #
#################################################


@log_function
def on_mqtt_message(client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage) -> None:
    """
    Callback function for incoming MQTT messages.

    Handles MQTT messages, decodes payload, and routes to appropriate processing functions.
    """
    topic = message.topic
    is_retained = message.retain
    global is_first_connect

    logger.debug(f"Received MQTT message - Topic: {topic}, Retained: {is_retained}")

    if is_first_connect and is_retained:
        logger.debug(f"Ignoring retained message during initial connection phase: {topic}")
        return

    try:
        payload = message.payload.decode('utf-8', errors='strict')
        logger.debug(f"Decoded payload for topic {topic}: {payload}")
    except UnicodeDecodeError as e:
        logging.error(f"Payload decoding error for topic {topic}: {e}", exc_info=True)
        return

    try:
        payload_data, data_type = process_payload(payload)
        logger.debug(f"Processed payload for topic {topic}. Type: {data_type}, Data: {payload_data}")
    except Exception as e:
        logging.error(f"Payload processing error for topic {topic}, payload '{payload}': {e}", exc_info=True)
        return

    # Route based on topic suffix
    if topic.endswith("/output/set"):
        logger.debug(f"Routing MQTT message for topic {topic} to AO transition handler.")
        if data_type != "json":
            logging.error(f"Invalid payload type for /output/set topic {topic}. Expected JSON, got: {data_type}. Payload: '{payload}'")
            return
        try:
            transition_value = payload_data.get('transition', 0)
            # Check for both 'brightness' (lowercase) and 'Brightness' (uppercase)
            brightness_value = payload_data.get('brightness', payload_data.get('Brightness'))

            if brightness_value is None:
                logging.error(f"Missing 'brightness' or 'Brightness' key in payload for /output/set topic {topic}. Payload: {payload_data}")
                return
            logging.debug(f"Transition: {transition_value}, Brightness: {brightness_value}")
            mqtt_to_websocket_converter_transition_ao(topic, payload, transition_value, brightness_value)
        except (TypeError, AttributeError, KeyError) as e:
            logging.error(f"Error processing /output/set message for topic {topic}: {e}. Payload: '{payload}'", exc_info=True)

    elif topic.endswith("/set"):
        logger.debug(f"Routing MQTT message for topic {topic} to standard set handler.")
        if data_type == "onoff":
            logger.debug(f"Calling mqtt_to_websocket_converter for topic {topic}, payload {payload_data}")
            mqtt_to_websocket_converter(message.topic, payload_data)
        elif data_type in ("int", "float"):
            # Usually /set expects ON/OFF, numeric might be unexpected here
            logging.warning(f"Unexpected numeric payload type '{data_type}' for /set topic {topic}. Expected 'onoff'. Payload: '{payload}'")
            # Decide if you want to handle this case or just log it
        else: # data_type == "string" (and not ON/OFF)
            logging.error(f"Invalid string payload for /set topic {topic}. Expected 'ON' or 'OFF'. Payload: '{payload}'")


@log_function
def mqtt_to_websocket_converter(mqtt_topic: str, message_payload: str) -> None:
    """
    Converts a simple MQTT message (ON/OFF) to a WebSocket message, queues it,
    performs verification via HTTP read-back, and acknowledges back to MQTT.
    Requires global mqtt_to_websocket_queue to be initialized.

    Args:
        mqtt_topic: The MQTT topic.
        message_payload: The MQTT message payload ("ON" or "OFF").
    """
    global mqtt_to_websocket_queue
    if mqtt_to_websocket_queue is None:
        logger.error("Cannot convert MQTT message: mqtt_to_websocket_queue not initialized.")
        return

    logger.debug(f"mqtt_to_websocket_converter called - Topic: {mqtt_topic}, Payload: {message_payload}")

    try:
        parts = mqtt_split_items_dataclass(mqtt_topic, message_payload)
        logger.debug(f"Parsed MQTT topic parts: {parts}")
    except Exception as e:
        logging.error(f"Failed to parse MQTT topic '{mqtt_topic}' or payload '{message_payload}': {e}", exc_info=True)
        return

    if not parts or not parts.dev or not parts.circuit: # cmd might not be needed if we assume 'set'
        logger.error(f"MQTT topic parsing resulted in incomplete parts for topic: {mqtt_topic}. Parts: {parts}")
        return

    # Determine the WebSocket state value based on payload
    ws_state_value: Any
    expected_read_value: int # 0 or 1 for verification
    payload_upper = str(message_payload).upper()
    if payload_upper == "ON":
        ws_state_value = 1
        expected_read_value = 1
        logger.debug("Converted MQTT payload 'ON' to WebSocket value 1")
    elif payload_upper == "OFF":
        ws_state_value = 0
        expected_read_value = 0
        logger.debug("Converted MQTT payload 'OFF' to WebSocket value 0")
    else:
        logging.error(f"Invalid payload '{message_payload}' for {parts.dev}/{parts.circuit}. Expected 'ON' or 'OFF'. Aborting.")
        return

    # --- Handle AO Interruption from ON/OFF commands ---
    if parts.dev == "ao":
        circuit_key = (parts.dev, parts.circuit)
        if payload_upper == "OFF":
            logger.warning(f"AO {circuit_key}: Received OFF command via /set topic. Interrupting any active transition and setting brightness to 0.")
            active_ao_transitions[circuit_key] = 0 # Signal transition thread to stop
            # ws_state_value is already 0 from earlier logic
        elif payload_upper == "ON":
            # ON command is ambiguous for AO brightness via /set topic.
            # Home Assistant typically restores previous brightness, which we don't know here.
            # For safety, we ignore the ON command if a transition might be active.
            # We could potentially set brightness to 1000, but ignoring seems safer.
            logger.warning(f"AO {circuit_key}: Received ON command via /set topic. Ignoring command as target brightness is unknown. Let any active transition continue or stay at current state.")
            # Do not proceed to send a WS command for ON via this path.
            return # Exit the function early

    # Construct WebSocket message
    ws_msg_dict: Dict[str, Any] = {
        "cmd": "set", # Assuming 'set' is the command for WebSocket
        "dev": parts.dev,
        "circuit": parts.circuit,
        "value": ws_state_value
    }

    try:
        ws_msg_json = json.dumps(ws_msg_dict)
        logger.debug(f"Queueing WebSocket message for MQTT topic {mqtt_topic}: {ws_msg_json}")
        mqtt_to_websocket_queue.put(ws_msg_json)
        logger.debug(f"Successfully queued WebSocket message for topic {mqtt_topic}")

        # --- Verification Step (Re-added for non-AO devices) ---
        verification_delay = 0.3 # Seconds to wait before checking
        logger.debug(f"Verification: Waiting {verification_delay}s before checking state for {parts.dev}/{parts.circuit}")
        time.sleep(verification_delay) # Blocking sleep in MQTT thread - consider alternatives if problematic

        logger.debug(f"Verification: Reading state for {parts.dev}/{parts.circuit} via HTTP")
        read_back_data = get_unipi_data(dev=parts.dev, circuit=parts.circuit, scope="value")

        verified = False
        actual_value_read = None

        if read_back_data and 'value' in read_back_data:
            try:
                # Convert read value to int for comparison (expecting 0.0 or 1.0)
                actual_value_read = int(float(read_back_data['value']))
                if actual_value_read == expected_read_value:
                    verified = True
                    logger.info(f"Verification successful for {parts.dev}/{parts.circuit}: Expected={expected_read_value}, Actual={actual_value_read}")
                else:
                    logger.warning(f"Verification mismatch for {parts.dev}/{parts.circuit}: Expected={expected_read_value}, Actual={actual_value_read}")
            except (ValueError, TypeError) as e:
                logger.error(f"Verification error for {parts.dev}/{parts.circuit}: Cannot process read value '{read_back_data.get('value')}'. Error: {e}")
        else:
            logger.error(f"Verification failed for {parts.dev}/{parts.circuit}: Could not read back value via HTTP.")

        # --- Acknowledge back to MQTT ---
        # Acknowledge the originally requested payload regardless of verification outcome, but log discrepancies.
        if not verified:
            logger.warning(f"Acknowledging MQTT for {parts.dev}/{parts.circuit} with requested payload '{message_payload}' despite verification failure/mismatch.")
        else:
             logger.debug(f"Acknowledging MQTT for {parts.dev}/{parts.circuit} with payload '{message_payload}' after successful verification.")

        MQTT_ack(message_payload, parts.dev, parts.circuit)

    except Exception as e:
        logging.error(f"Failed to queue WebSocket message, verify state, OR acknowledge MQTT for topic {mqtt_topic}: {e}", exc_info=True)
        # If queueing fails, MQTT_ack won't be called.
        return


MINIMUM_STEP_TIME = 0.1

# --- AO Transition Helper Function (Runs in Separate Thread) ---
def _perform_ao_transition(
    dev: str,
    circuit: str,
    desired_value: int,
    current_value: int,
    transition_time: float,
    circuit_key: Tuple[str, str],
    this_instance_desired_value: int
) -> None:
    """
    Runs the AO transition loop in a separate thread. Queues WS commands and MQTT ACKs.
    """
    global mqtt_to_websocket_queue, active_ao_transitions

    try:
        # Recalculate transition parameters
        value_diff = desired_value - current_value
        steps = int(abs(value_diff))
        step_time = transition_time / steps if steps > 0 else 0
        logger.debug(f"AO Thread ({circuit_key}): Calculated: value_diff={value_diff}, initial_steps={steps}, initial_step_time={step_time}s")

        if steps > 0 and step_time < MINIMUM_STEP_TIME:
            step_time = MINIMUM_STEP_TIME
            steps = max(1, int(round(transition_time / step_time)))
            logger.debug(f"AO Thread ({circuit_key}): Adjusted for MINIMUM_STEP_TIME: steps={steps}, step_time={step_time}s")

        step_size = value_diff / steps if steps > 0 else 0
        logger.debug(f"AO Thread ({circuit_key}): Final calculation: steps={steps}, step_time={step_time}s, step_size={step_size}")

        ws_msg_dict: Dict[str, Any] = {
            "cmd": "set", "dev": dev, "circuit": circuit, "value": None
        }

        current_step_value = float(current_value)
        start_time = time.monotonic()
        last_ack_time = 0 # Initialize time of last intermediate ACK
        min_ack_interval = 0.5 # Seconds (~2 Hz)

        logger.info(f"AO Thread ({circuit_key}): Starting loop: {steps} steps over ~{transition_time:.2f}s")

        for step_num in range(steps):
            # Check for Interruption
            current_target = active_ao_transitions.get(circuit_key)
            if current_target != this_instance_desired_value:
                logger.warning(f"AO Thread ({circuit_key}): Interrupted by new target value: {current_target}.")
                break

            # Check for Shutdown
            if should_stop.is_set():
                logging.warning(f"AO Thread ({circuit_key}): Aborted before step {step_num+1} due to shutdown.")
                break

            loop_start_time = time.monotonic()

            # Calculate & Clamp Value
            current_step_value += step_size
            final_step_value = min(max(current_step_value, 0), 1000) # Ensure value stays within 0-1000
            if step_size > 0: final_step_value = min(final_step_value, desired_value)
            else: final_step_value = max(final_step_value, desired_value)

            # Queue WebSocket Command
            ws_value = round(final_step_value / 100, 3)
            ws_msg_dict["value"] = ws_value
            logger.debug(f"AO Thread Step {step_num+1}/{steps} ({circuit_key}): Queueing WS value: {ws_value}")
            try:
                if mqtt_to_websocket_queue:
                    mqtt_to_websocket_queue.put(json.dumps(ws_msg_dict))
            except Exception as q_err:
                logging.error(f"AO Thread Step {step_num+1} ({circuit_key}): Failed to queue WS message: {q_err}", exc_info=True)
                break

            # Queue Intermediate MQTT ACK immediately (Rate Limited)
            now = time.monotonic()
            if now - last_ack_time >= min_ack_interval:
                current_ack_value = int(round(final_step_value))
                logger.debug(f"Intermediate Ack: Calling MQTT_ack for AO {dev}/{circuit} with current value '{current_ack_value}'")
                MQTT_ack(str(current_ack_value), dev, circuit) # Ack with current step value
                last_ack_time = now

            # Calculate sleep time
            loop_elapsed_time = time.monotonic() - loop_start_time
            total_elapsed_time = time.monotonic() - start_time
            remaining_transition_time = max(0, transition_time - total_elapsed_time)
            sleep_duration = max(0, min(step_time - loop_elapsed_time, remaining_transition_time))

            logger.debug(f"AO Thread Step {step_num+1} ({circuit_key}): Loop took {loop_elapsed_time:.4f}s, sleeping for {sleep_duration:.4f}s")
            time.sleep(sleep_duration)

        else: # Executes if the loop completed without a break
            logger.info(f"AO Thread ({circuit_key}): Loop completed.")

        # --- Final Optimistic Acknowledgement ---
        # Always acknowledge the final desired value after the loop
        logger.debug(f"AO Thread ({circuit_key}): Queueing final MQTT ACK: {desired_value}")
        MQTT_ack(str(desired_value), dev, circuit)

    except Exception as e:
         logger.exception(f"Error within AO transition thread for {circuit_key}: {e}")
    finally:
        logger.debug(f"AO transition thread for {circuit_key} finishing.")
        # --- Cleanup Active Transition Tracking ---
        # Remove entry only if this thread's target value is still the active one
        current_target_at_end = active_ao_transitions.get(circuit_key)
        if current_target_at_end == this_instance_desired_value:
            logger.debug(f"AO Thread ({circuit_key}): Removing active transition tracking (target {this_instance_desired_value})")
            active_ao_transitions.pop(circuit_key, None) # Remove key if it exists
        else:
            logger.debug(f"AO Thread ({circuit_key}): Not removing active transition tracking. Current target ({current_target_at_end}) differs from this thread's target ({this_instance_desired_value}).")


@log_function
@log_function
def mqtt_to_websocket_converter_transition_ao(
    mqtt_topic: str, mqtt_payload: str, transition_value: Any, brightness_value: Any
) -> None:
    """
    Gradually transitions an analog output (AO) to a new brightness value over time.
    Uses optimistic acknowledgement and supports interruption.
    Requires global mqtt_to_websocket_queue and active_ao_transitions to be initialized.

    Args:
        mqtt_topic: MQTT topic.
        mqtt_payload: MQTT payload (JSON string containing brightness/transition).
        transition_value: Transition time in seconds.
        brightness_value: Target brightness value (0-1000).
    """
    global mqtt_to_websocket_queue, active_ao_transitions
    if mqtt_to_websocket_queue is None:
        logger.error("Cannot convert AO transition: mqtt_to_websocket_queue not initialized.")
        return

    logger.debug(
        f"AO Transition Start - Topic: {mqtt_topic}, Brightness: {brightness_value}, Transition: {transition_value}s"
    )

    # Define variables needed in finally block early
    circuit_key = None
    this_instance_desired_value = None

    try: # Outer try for initial setup errors
        parts = mqtt_split_items_dataclass(mqtt_topic, mqtt_payload)
        logger.debug(f"AO Transition - Parsed parts: {parts}")
        if not parts or not parts.dev or not parts.circuit:
            logging.error(f"AO Transition - Could not extract dev/circuit from topic: {mqtt_topic}")
            return

        dev = parts.dev
        circuit = parts.circuit
        circuit_key = (dev, circuit) # Tuple key for dictionary

        # Validate inputs
        try:
            desired_value = int(float(brightness_value)) # Target brightness (0-1000)
            transition_time = float(transition_value) # Transition duration in seconds
            logger.debug(f"AO Transition - Validated inputs: desired_value={desired_value}, transition_time={transition_time}s")

            if not 0 <= desired_value <= 1000:
                raise ValueError(f"Brightness value {desired_value} out of range (0-1000)")
            if not 0 <= transition_time <= 60: # Clamp transition time
                original_transition = transition_time
                transition_time = max(0, min(transition_time, 60))
                logging.warning(f"AO Transition - Transition time {original_transition}s clamped to {transition_time}s (range 0-60s)")

        except (ValueError, TypeError) as e:
            logging.error(f"AO Transition - Invalid input for topic {mqtt_topic}: {e}", exc_info=True)
            return

        # --- Update Active Transition Tracking ---
        logger.debug(f"Updating active transition target for {circuit_key} to {desired_value}")
        active_ao_transitions[circuit_key] = desired_value
        # Store the desired value this instance is working towards
        this_instance_desired_value = desired_value # Used in finally and loop check

        # --- Main Transition Logic ---
        # Get current value (Removed inner try block)
        logger.debug(f"AO Transition - Fetching current value for {dev}/{circuit}")
        current_value_dict = get_unipi_data(dev=dev, circuit=circuit, scope="value")
        if not current_value_dict or 'value' not in current_value_dict:
            logging.error(f"AO Transition - Failed to get current value for {dev}/{circuit} from Unipi.")
            # Acknowledge desired state optimistically if read fails
            logger.warning(f"AO Transition - Could not read current value. Proceeding with optimistic ack for desired value {desired_value}.")
            MQTT_ack(str(desired_value), dev, circuit)
            return # Exit function after ack
        try:
            # Assuming Unipi value is 0-10V, convert to 0-1000 for internal use
            current_value = int(float(current_value_dict['value']) * 100)
            logger.debug(f"AO Transition - Current value for {dev}/{circuit}: {current_value} (0-1000 scale)")
        except (KeyError, TypeError, ValueError) as e:
            logging.error(f"AO Transition - Invalid current value received from Unipi for {dev}/{circuit}: {current_value_dict}. Error: {e}", exc_info=True)
            # Acknowledge desired state optimistically if parse fails
            logger.warning(f"AO Transition - Could not parse current value. Proceeding with optimistic ack for desired value {desired_value}.")
            MQTT_ack(str(desired_value), dev, circuit)
            return # Exit function after ack

        # Check if already at target
        if desired_value == current_value:
            logging.info(f"AO Transition - Desired value ({desired_value}) already matches current value for {dev}/{circuit}. No transition needed.")
            # Still need to publish the state and brightness topics for HA confirmation
            logger.debug(f"Optimistic Ack: Calling MQTT_ack for AO {dev}/{circuit} with value '{desired_value}'")
            MQTT_ack(str(desired_value), dev, circuit)
            return # Exit function after ack

        # --- Start Transition Thread ---
        logger.info(f"AO Transition - Starting background thread for {dev}/{circuit} to {desired_value} over {transition_time}s")
        transition_thread = threading.Thread(
            target=_perform_ao_transition,
            args=(
                dev,
                circuit,
                desired_value,
                current_value,
                transition_time,
                circuit_key,
                this_instance_desired_value # Pass the target this instance is aiming for
            ),
            name=f"AOTransition-{dev}-{circuit}",
            daemon=True # Allows script to exit even if thread is running
        )
        transition_thread.start()
        logger.debug(f"AO Transition thread for {circuit_key} started.")

        # Note: The finally block is removed here. Cleanup is now handled within _perform_ao_transition.
        # The except block below catches errors during initial setup or thread start.
    except Exception as e:
        logging.exception(f"Error during AO transition for topic {mqtt_topic}: {e}")
        # Consider if an error state should be published to MQTT here


async def send_to_websocket(message: str) -> None:
    """
    Sends a message to the WebSocket if the connection is open.
    Runs in the asyncio event loop.
    """
    ws = websocket_global # Local reference
    logger.debug(f"send_to_websocket called. WS State: {ws.state if ws else 'None'}. Message: {message}")
    if ws is not None and ws.state == State.OPEN:
        try:
            logger.debug(f"Attempting to send message via WebSocket: {message}")
            await ws.send(message)
            logger.debug(f"Successfully sent message via WebSocket.")
        except websockets.exceptions.ConnectionClosedError as e:
            # Error if connection closes during send
            logger.error(f"WebSocket connection closed while trying to send message: {e}. Message lost: {message}", exc_info=True)
            # Consider notifying the handler or attempting reconnect?
        except Exception as e:
            # Catch other potential errors during send
            logger.error(f"Unexpected error sending WebSocket message: {e}. Message: {message}", exc_info=True)
    else:
        # Log if websocket is None or not in OPEN state
        state_str = str(ws.state) if ws else 'None'
        logger.error(f"WebSocket not available or not open, cannot send message. State: {state_str}. Message dropped: {message}")


@log_function
def MQTT_ack(final_value_payload: str, dev: str, circuit: str) -> None:
    """
    Sends MQTT message(s) to update Home Assistant state optimistically
    after a command has been sent to the Unipi device.
    Requires global websocket_to_mqtt_queue to be initialized.

    Args:
        final_value_payload: The intended final state payload ("ON", "OFF", or brightness 0-1000 as string).
        dev: Device type (e.g., "ro", "ao").
        circuit: Circuit identifier (e.g., "1_01").
    """
    global websocket_to_mqtt_queue
    if websocket_to_mqtt_queue is None:
        logger.error("Cannot ACK to MQTT: websocket_to_mqtt_queue not initialized.")
        return

    logger.debug(f"MQTT_ack called for {dev}/{circuit} with final payload: {final_value_payload}")

    state_topic = generate_mqtt_topic_update(dev, circuit, "state")

    if dev == "ao":
        # For Analog Output, publish brightness to /output and state to /state
        brightness_topic = generate_mqtt_topic_update(dev, circuit, "output")
        try:
            # Assume final_value_payload is the target brightness 0-1000 as a string/number
            brightness_value = int(float(final_value_payload))
            brightness_value = max(0, min(brightness_value, 1000)) # Clamp
            state_value = "ON" if brightness_value > 0 else "OFF"

            logger.info(f"MQTT_ack (AO): Publishing brightness {brightness_value} to {brightness_topic}")
            websocket_to_mqtt_queue.put((brightness_topic, str(brightness_value)))
            logger.info(f"MQTT_ack (AO): Publishing state {state_value} to {state_topic}")
            websocket_to_mqtt_queue.put((state_topic, state_value))

        except (ValueError, TypeError) as e:
            logger.error(f"MQTT_ack (AO) Error: Could not parse brightness from payload '{final_value_payload}' for {dev}/{circuit}: {e}")
            # Fallback: Publish OFF state if brightness parsing fails
            logger.warning(f"MQTT_ack (AO) Fallback: Publishing state OFF to {state_topic}")
            websocket_to_mqtt_queue.put((state_topic, "OFF"))

    elif dev in ["di", "do", "ro", "led"]:
        # For ON/OFF device types
        # Assume final_value_payload is "ON" or "OFF"
        state_value = "ON" if str(final_value_payload).upper() == "ON" else "OFF"
        logger.info(f"MQTT_ack ({dev}): Publishing state {state_value} to {state_topic}")
        websocket_to_mqtt_queue.put((state_topic, state_value))
    else:
        # Log if called for an unexpected device type
        logger.warning(f"MQTT_ack called for unhandled device type '{dev}' with payload '{final_value_payload}'. No MQTT message sent.")


@dataclass
class MqttParts:
    """
    Data class to hold parsed MQTT topic and payload items.
    """
    dev: Optional[str] = None
    circuit: Optional[str] = None
    state_value: Any = None
    cmd: Optional[str] = None


@log_function
def mqtt_split_items_dataclass(mqtt_topic: str, message_payload: str) -> MqttParts:
    """
    Splits an MQTT topic and payload into individual items using dataclass.

    Args:
        mqtt_topic: The MQTT topic string.
        message_payload: The MQTT message payload string.

    Returns:
        An MqttParts dataclass instance or an empty MqttParts on error.
    """
    try:
        # Example topic: unipi/Neuron_M203_154/ro/2_01/set
        # Example topic: unipi/Neuron_M203_154/ao/1_01/output/set
        topic_parts = mqtt_topic.split('/')
        if len(topic_parts) < 5:
            logging.error(f"Invalid MQTT topic format: {mqtt_topic}. Expected at least 5 parts.")
            return MqttParts()

        # More robust parsing assuming fixed structure relative to device name
        try:
            device_name_index = topic_parts.index(device_name) # Find device name
            dev = topic_parts[device_name_index + 1]
            circuit = topic_parts[device_name_index + 2]
            cmd = topic_parts[-1] # Last part is command ('set' or 'output')
        except (ValueError, IndexError):
             logging.error(f"Could not parse dev/circuit/cmd from topic structure: {mqtt_topic}")
             return MqttParts()


        # Try to interpret payload
        processed_payload, _ = process_payload(message_payload) # Use existing processor

        return MqttParts(dev, circuit, processed_payload, cmd)

    except Exception as e:
        logging.error(f"Error parsing MQTT topic/payload: {e}: Topic='{mqtt_topic}', Payload='{message_payload}'", exc_info=True)
        return MqttParts()


@log_function
def generate_mqtt_topic_update(dev: str, circuit: str, topic_end: str) -> str:
    """
    Generates MQTT topic for device state or output updates.

    Args:
        dev: Device type.
        circuit: Circuit identifier.
        topic_end: Topic suffix ("state" or "output").

    Returns:
        The generated MQTT update topic string.
    """
    if dev == "temp":
        # Special structure for 1-Wire temperature sensors
        # circuit here is the 1-wire sensor ID (e.g., 28ABA97E0E000033)
        return f"unipi/{device_name}/1-wire/{circuit}/{dev}/{topic_end}"
    else:
        # Standard structure for other devices
        return f"unipi/{device_name}/{dev}/{circuit}/{topic_end}"


@log_function
def process_payload(payload: str) -> Tuple[Any, str]:
    """
    Processes MQTT payload, attempting to parse as JSON, then as other types.

    Args:
        payload: The MQTT message payload string.

    Returns:
        A tuple containing:
            - The processed payload data (JSON object, string, int, float, or original string).
            - A string indicating the data type ("json", "onoff", "int", "float", "string").
    """
    try:
        data = json.loads(payload)
        if isinstance(data, int):
            data = {"brightness": data}
        return data, "json"
    except json.JSONDecodeError:
        if payload.upper() in ("ON", "OFF"):
            return payload.upper(), "onoff"
        try:
            data = int(payload)
            return data, "int"
        except ValueError:
            try:
                data = float(payload)
                return data, "float"
            except ValueError:
                return payload, "string"


@log_function
def on_mqtt_disconnect(client: mqtt.Client, userdata: Any, flags: Dict[str, Any], reason_code: int, properties: mqtt.Properties) -> None:
    """
    Callback for MQTT disconnection events. Handles unexpected disconnects by attempting reconnection.

    Args:
        client: MQTT client instance.
        userdata: User data (not used).
        flags: Disconnection flags.
        reason_code: Disconnection reason code.
        properties: Disconnection properties.
    """
    # Paho documentation: reason_code == 0 means disconnect() was called by the client.
    # Any other reason_code indicates an unexpected disconnect.
    # There isn't a direct disconnect_string function, log the code itself.
    logger.info(f"on_mqtt_disconnect called with reason code: {reason_code}")

    if reason_code == 0:
        logger.info(f"Disconnected cleanly from MQTT Broker: {MQTT_BROKER}:{MQTT_PORT}")
    else:
        # Use connack_string for connection-related errors if available, otherwise just log the code
        reason_str = mqtt.connack_string(reason_code) if hasattr(mqtt, 'connack_string') else f"Code {reason_code}"
        logger.warning(f"Unexpected disconnection from MQTT Broker {MQTT_BROKER}:{MQTT_PORT}. Reason: {reason_str}")
        # Automatic reconnection is handled by the paho-mqtt library's loop
        # when reconnect_delay_set() is configured. No need to call client.reconnect() here.
        logger.info("The library will attempt to reconnect automatically.")


#################################################
#            --- Main Thread ---                #
#################################################


if __name__ == "__main__":
    logging.info("--- Script Starting ---")
    # Configure logging (ensure this is done early)
    log_dir = os.path.expanduser("~/.local/logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "connection_test.log")
    log_level = logging.DEBUG
    handler = logging.handlers.RotatingFileHandler(log_file, maxBytes=5 * 1024 * 1024, backupCount=3)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(threadName)s - %(message)s')
    handler.setFormatter(formatter)
    logging.basicConfig(level=log_level, handlers=[handler])
    logger = logging.getLogger(__name__) # Get logger for this module
    logger.info("Logging configured.")

    # Load config (ensure this happens after logging is set up)
    try:
        logger.info("Loading configuration from config.json...")
        config = load_config("config.json")
        MQTT_BROKER = config["mqtt"]["broker"]
        MQTT_PORT = config["mqtt"]["port"]
        MQTT_USERNAME = config["mqtt"]["username"]
        MQTT_PASSWORD = config["mqtt"]["password"]
        MQTT_TOPIC = config["mqtt"]["topic"]
        WEBSOCKET_URL = config["websocket"]["url"]
        UNIPI_URL = config["unipi_http"]["url"]
        logger.info("Configuration loaded successfully.")
        logger.debug(f"Using MQTT broker: {MQTT_BROKER}:{MQTT_PORT}")
        logger.debug(f"Using WebSocket server: {WEBSOCKET_URL}")
        logger.debug(f"Using Unipi URL: {UNIPI_URL}")
    except (FileNotFoundError, json.JSONDecodeError, KeyError) as e:
        logging.critical(f"Failed to load or parse configuration: {e}. Exiting.")
        exit(1) # Exit if config fails

    logger.info("Initializing MQTT client...")
    mqtt_client = mqtt_connect() # Note: mqtt_connect now uses connect_async and loop_start internally
    logger.info("Creating message queues...")
    mqtt_to_websocket_queue, websocket_to_mqtt_queue = create_queues()

    websocket_global = None # Initialize global websocket variable

    logger.info("Creating and setting asyncio event loop...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    logger.info("Starting MQTT loop thread...")
    start_mqtt_loop(mqtt_client) # Starts the custom MQTT loop thread
    logger.info("Performing initial Home Assistant discovery...")
    perform_discovery_and_mqtt_subscribe(websocket_to_mqtt_queue, mqtt_client)
    # --- Publish Online Status ---
    if device_name: # Ensure device_name was set during discovery
        online_topic = f"unipi/{device_name}/status"
        logger.info(f"Publishing 'online' status to {online_topic}")
        # Use the client directly to publish with retain=True
        mqtt_client.publish(online_topic, payload="online", qos=1, retain=True)
    else:
        logger.warning("Device name not set after discovery, cannot publish online status.")
    # --- End Publish Online Status ---
    logger.info("Attempting initial WebSocket connection...")
    # websocket_connect is removed, connection handled within websocket_handler
    # websocket_global = loop.run_until_complete(websocket_connect(WEBSOCKET_URL)) # Old way
    logger.info("Starting worker threads (MQTT Publisher, WebSocket Sender)...")
    start_worker_threads(websocket_to_mqtt_queue, mqtt_to_websocket_queue, loop)
    logger.info("Worker threads started.")

    websocket_handler_task: Optional[asyncio.Task[None]] = None
    worker_threads = threading.enumerate() # Get list of currently running threads (includes main, workers)

    # --- Signal Handling ---
    exit_event = asyncio.Event() # Event to signal graceful shutdown
    def shutdown_handler(signame: str) -> None:
        """Sets the stop event and the asyncio exit event upon receiving a signal."""
        if not should_stop.is_set():
            logger.warning(f"Received signal {signame}. Initiating graceful shutdown...")
            should_stop.set() # Signal threads to stop
            # Signal the asyncio loop to stop running
            loop.call_soon_threadsafe(loop.stop)
        else:
            logger.debug(f"Received signal {signame} again, shutdown already in progress.")

    logger.info("Registering signal handlers (SIGINT, SIGTERM)...")
    for signame in ('SIGINT', 'SIGTERM'):
        try:
            loop.add_signal_handler(getattr(signal, signame), lambda signame=signame: shutdown_handler(signame))
            logger.debug(f"Registered signal handler for {signame}")
        except NotImplementedError:
            logger.warning(f"Cannot register signal handler for {signame} on this OS.")
        except ValueError:
            logger.warning(f"Invalid signal {signame} for registration.")

    # --- Start Main Async Task ---
    logger.info("Creating WebSocket handler task...")
    # websocket_handler now manages its own connection lifecycle internally
    websocket_handler_task = loop.create_task(websocket_handler(websocket_to_mqtt_queue))

    # --- Run Event Loop Until Shutdown ---
    logger.info("Application startup complete. Running event loop until shutdown signal...")
    try:
        # Run the loop indefinitely until loop.stop() is called by the shutdown handler
        loop.run_forever()

    except KeyboardInterrupt:
        # Fallback if Ctrl+C happens before signal handler registration or if handler fails
        logger.warning("KeyboardInterrupt caught directly in main loop. Forcing shutdown...")
        if not should_stop.is_set():
            should_stop.set()
            # No need to set exit_event here as the loop is already stopping
    except Exception as e:
        logger.exception(f"Critical error during main event loop execution: {e}")
        if not should_stop.is_set():
            should_stop.set() # Ensure shutdown is triggered
    finally:
        # --- Final Shutdown Sequence ---
        logger.warning("Starting final shutdown sequence...")
        if not should_stop.is_set():
            logger.warning("Shutdown sequence started unexpectedly. Setting stop signal.")
            should_stop.set()

        # 1. Cancel the main async task (websocket_handler)
        if websocket_handler_task and not websocket_handler_task.done():
            logger.info("Cancelling main websocket_handler task...")
            websocket_handler_task.cancel()
            try:
                # Wait briefly for the task to finish processing cancellation
                logger.debug("Waiting for websocket_handler task to finish cancellation...")
                loop.run_until_complete(asyncio.wait_for(websocket_handler_task, timeout=5.0))
                logger.info("Websocket_handler task finished after cancellation request.")
            except asyncio.CancelledError:
                logger.info("Websocket_handler task successfully cancelled.")
            except asyncio.TimeoutError:
                logger.warning("Websocket_handler task did not finish within timeout after cancellation request.")
            except Exception as e:
                logger.error(f"Error occurred while waiting for websocket_handler task cancellation: {e}", exc_info=True)

        # 2. Wait for message queues to empty (best effort)
        logger.info("Waiting for message queues to empty (best effort)...")
        try:
            # No guaranteed way to wait for put() completion, but join() waits for task_done()
            mqtt_to_websocket_queue.join()
            websocket_to_mqtt_queue.join()
            logger.info("Message queues joined (all task_done() received).")
        except Exception as e:
            logger.error(f"Error joining message queues during shutdown: {e}", exc_info=True)

        # 3. Wait for worker threads to exit
        logger.info("Waiting for worker threads to join (timeout 2s each)...")
        # Filter to join only the threads we started (daemons)
        our_threads = [t for t in worker_threads if t != threading.main_thread() and t.daemon]
        for t in our_threads:
            logger.debug(f"Joining thread {t.name}...")
            t.join(timeout=2.0)
            if t.is_alive():
                logger.warning(f"Thread {t.name} did not exit within timeout.")
            else:
                logger.debug(f"Thread {t.name} joined successfully.")

        # 4. Publish Offline Status and Disconnect MQTT client
        if mqtt_client:
            # --- Publish Offline Status ---
            if device_name: # Ensure device_name was set
                offline_topic = f"unipi/{device_name}/status"
                logger.warning(f"Publishing 'offline' status to {offline_topic}")
                try:
                    # Publish offline message with retain=True
                    # Use publish directly, ensuring it happens before disconnect
                    mqtt_client.publish(offline_topic, payload="offline", qos=1, retain=True)
                    # Give a very short moment for the message to potentially go out
                    # Note: This is not a guarantee, but a best effort.
                    time.sleep(0.1)
                except Exception as pub_err:
                    logger.error(f"Failed to publish offline status during shutdown: {pub_err}")
            else:
                logger.warning("Device name not set during shutdown, cannot publish offline status.")
            # --- End Publish Offline Status ---

            logger.info("Disconnecting MQTT client...")
            mqtt_client.disconnect()
            # Stop the loop thread explicitly if needed? No, it checks should_stop.
            logger.info("MQTT client disconnected.")

        # 5. Close the asyncio loop
        logger.info("Closing asyncio event loop...")
        try:
            # Cancel any other potentially remaining tasks
            logger.debug("Cancelling any remaining asyncio tasks...")
            tasks = asyncio.all_tasks(loop=loop)
            for task in tasks:
                if task is not websocket_handler_task and not task.done(): # Avoid cancelling the main task again
                    task.cancel()
            # Run loop once to allow tasks to process cancellation
            if tasks:
                loop.run_until_complete(asyncio.sleep(0.1)) # Short sleep to allow processing
            logger.debug("Remaining asyncio tasks cancelled.")
        except Exception as e:
            logger.error(f"Error cancelling remaining asyncio tasks during loop close: {e}", exc_info=True)
        finally:
            if loop.is_running():
                logger.debug("Stopping event loop...")
                loop.stop()
            if not loop.is_closed():
                logger.debug("Closing event loop...")
                loop.close()
                logger.info("Asyncio event loop closed.")
            else:
                logger.info("Asyncio event loop was already closed.")

        logger.warning("--- Script Exited ---")
