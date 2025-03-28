# Unipi to Home Assistant MQTT Bridge

This Python script acts as a bridge between a Unipi controller (using its WebSocket interface) and an MQTT broker. It enables integration with Home Assistant through MQTT Discovery, allowing you to monitor and control Unipi devices (Digital Inputs/Outputs, Relays, Analog Inputs/Outputs, LEDs) within Home Assistant.

## Features

*   **Home Assistant MQTT Discovery:** Automatically generates configuration topics for Home Assistant to discover and add Unipi devices.
*   **Bidirectional Communication:** Relays device state changes from Unipi to MQTT and sends commands from MQTT (Home Assistant) back to the Unipi via WebSocket.
*   **Analog Output (AO) Transitions:** Supports smooth brightness transitions for AO devices configured as lights in Home Assistant. Transitions can be interrupted by new commands.
*   **Device Availability:** Publishes the script's status (online/offline) to MQTT, allowing Home Assistant to accurately reflect device availability.
*   **Resilience:** Includes automatic reconnection logic for both MQTT and WebSocket connections with exponential backoff. Handles common errors gracefully.
*   **Configuration:** Uses a simple `config.json` file for setup.
*   **Logging:** Logs activity and errors to `~/.local/logs/connection_test.log` with rotation.

## Prerequisites

*   Python 3.x
*   Access to the Unipi controller's WebSocket and HTTP API.
*   An MQTT broker accessible by both this script and Home Assistant.

## Installation

1.  **Create Project Directory:**
    It's recommended to place the script and its configuration in a dedicated directory. If you want it in your home folder:
    ```bash
    mkdir ~/unipi-homeassistant
    cd ~/unipi-homeassistant
    # Copy hass-unipi.py and config.json into this directory
    ```

2.  **Create Python Virtual Environment:**
    Using a virtual environment is highly recommended to isolate dependencies.
    ```bash
    python3 -m venv .venv
    ```
    *(Note: `.venv` is a common name for virtual environment directories within a project)*

3.  **Activate Virtual Environment:**
    *   On Linux/macOS:
        ```bash
        source .venv/bin/activate
        ```
    *   On Windows (Git Bash/WSL):
        ```bash
        source .venv/Scripts/activate
        ```
    *   On Windows (Command Prompt):
        ```bash
        .\.venv\Scripts\activate.bat
        ```
    *   On Windows (PowerShell):
        ```bash
        .\.venv\Scripts\Activate.ps1
        ```
    You should see `(.venv)` prepended to your command prompt.

4.  **Install Required Packages:**
    ```bash
    python -m pip install paho-mqtt websockets requests pydantic
    ```
    *(Note: The script also performs a check at startup and will list any missing dependencies.)*

## Configuration

The script requires a `config.json` file in the same directory. Create one with the following structure:

```json
{
  "mqtt": {
    "broker": "YOUR_MQTT_BROKER_IP_OR_HOSTNAME",
    "port": 1883,
    "username": "YOUR_MQTT_USERNAME",
    "password": "YOUR_MQTT_PASSWORD",
    "topic": "unipi" // Base topic for Unipi devices (optional, defaults to "unipi")
  },
  "websocket": {
    "url": "ws://YOUR_UNIPI_IP/ws" // WebSocket URL of your Unipi controller
  },
  "unipi-http": {
    "url": "http://YOUR_UNIPI_IP/rest" // REST API URL of your Unipi controller
  }
}
```

*   Replace placeholders (`YOUR_MQTT_BROKER_IP_OR_HOSTNAME`, `YOUR_MQTT_USERNAME`, `YOUR_MQTT_PASSWORD`, `YOUR_UNIPI_IP`) with your actual settings.
*   Adjust the MQTT port if necessary.
*   The `mqtt.topic` is used as a prefix for device topics (e.g., `unipi/DEVICE_NAME/...`).

## Running the Script

1.  Ensure your virtual environment is activated (`source .venv/bin/activate`).
2.  Make sure you are in the directory containing `hass-unipi.py` and `config.json`.
3.  Run the script:
    ```bash
    python hass-unipi.py
    ```
4.  The script will connect to the Unipi, perform discovery, connect to MQTT, and start relaying messages.
5.  To stop the script gracefully, press `Ctrl+C`.

## Home Assistant Integration

*   Ensure the MQTT integration is configured in Home Assistant and connected to the same broker specified in `config.json`.
*   Once the script runs, devices should automatically appear in Home Assistant under the MQTT integration.
*   The script publishes an availability topic (`unipi/DEVICE_NAME/status`). Devices will show as "unavailable" in Home Assistant when the script is not running.

### Manual Analog Output (AO) Transitions via MQTT

While Home Assistant handles transitions automatically for lights, you can also manually trigger a brightness transition for an Analog Output by publishing an MQTT message directly.

*   **Topic:** `unipi/DEVICE_NAME/ao/CIRCUIT/output/set`
    *   Replace `DEVICE_NAME` with the name derived from your Unipi's model and serial number (e.g., `Neuron_M203_154`).
    *   Replace `CIRCUIT` with the specific AO circuit identifier (e.g., `1_01`).
*   **Payload:** A JSON string containing the desired brightness and transition time.
    ```json
    {"transition": TIME_IN_SECONDS, "brightness": TARGET_BRIGHTNESS}
    ```
    *   `TIME_IN_SECONDS`: The duration of the transition (e.g., `5` for 5 seconds). The script clamps this between 0 and 60 seconds.
    *   `TARGET_BRIGHTNESS`: The target brightness level (0-1000). Note that the script uses an internal scale of 0-1000, which corresponds to 0-10V on the Unipi AO.

**Example:** To transition AO `1_01` on device `Neuron_M203_154` to 50% brightness (500) over 10 seconds, publish the following payload:
```json
{"transition": 10, "brightness": 500}
```
to the topic:
`unipi/Neuron_M203_154/ao/1_01/output/set`

This allows for more direct control or automation outside of Home Assistant's standard light controls if needed.

## Logging

*   The script logs detailed information, warnings, and errors to `~/.local/logs/connection_test.log`.
*   The log file rotates automatically when it reaches 5MB, keeping up to 3 backup files.
*   Check this log file for troubleshooting connection issues or unexpected behavior.

## Troubleshooting

*   **Connection Errors:** Check the log file (`~/.local/logs/connection_test.log`) for details. Verify IP addresses, ports, usernames, and passwords in `config.json`. Ensure network connectivity between the script runner, the Unipi, and the MQTT broker.
*   **Devices Not Appearing in HA:** Verify the MQTT integration is set up correctly in Home Assistant. Check the Home Assistant logs and the script logs for MQTT connection errors or discovery message issues. Ensure the `device_name` derived from your Unipi model/SN is consistent.
*   **Dependencies Missing:** The script checks for required libraries on startup. If it exits with an error message about missing libraries, ensure you have activated the virtual environment and run the `pip install` command again.