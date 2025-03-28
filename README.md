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

## Running the Script Manually

1.  Ensure your virtual environment is activated (`source .venv/bin/activate`).
2.  Make sure you are in the directory containing `hass-unipi.py` and `config.json`.
3.  Run the script:
    ```bash
    python hass-unipi.py
    ```
4.  The script will connect to the Unipi, perform discovery, connect to MQTT, and start relaying messages.
5.  To stop the script gracefully, press `Ctrl+C`.

## Deploying to a Raspberry Pi (or other remote machine) using Git

Using Git is a convenient way to manage the script on a remote machine like a Raspberry Pi.

1.  **SSH into your Raspberry Pi:**
    ```bash
    ssh pi@YOUR_RASPBERRY_PI_IP
    ```

2.  **Install Git (if not already installed):**
    ```bash
    sudo apt update
    sudo apt install git -y
    ```

3.  **Clone the Repository:**
    Choose a directory where you want to store the script (e.g., `/home/pi/unipi-homeassistant`).
    ```bash
    cd /home/pi # Or your preferred location
    git clone https://github.com/matthijsberg/unipi-homeassistant.git
    cd unipi-homeassistant
    ```

4.  **Follow Installation Steps (on the Pi):**
    Now, follow steps 2-4 from the main [Installation](#installation) section *on your Raspberry Pi* within the cloned directory (`/home/pi/unipi-homeassistant` in this example):
    *   Create and activate a Python virtual environment (`python3 -m venv .venv`, `source .venv/bin/activate`).
    *   Install the required packages (`python -m pip install paho-mqtt websockets requests pydantic`).

5.  **Create `config.json` (on the Pi):**
    The `config.json` file is *not* stored in Git (due to `.gitignore`). You need to create it manually on the Raspberry Pi inside the `unipi-homeassistant` directory. Use the structure described in the [Configuration](#configuration) section, filling in the correct details for your Pi's environment (MQTT broker accessible from the Pi, Unipi IP accessible from the Pi).
    ```bash
    nano config.json
    # Paste the JSON structure and edit your details, then save (Ctrl+X, then Y, then Enter)
    ```

6.  **Running as a Systemd Service (Recommended for Persistence):**
    To ensure the script runs automatically on boot and restarts if it fails, set it up as a systemd service. A template service file (`hass-unipi.service`) is included in the repository.

    a.  **Edit the Service File (if necessary):**
        The provided `hass-unipi.service` assumes the script is located in `/home/pi/unipi-homeassistant` and run by the `pi` user. If your path or username is different, edit the file *before* copying it:
        ```bash
        nano hass-unipi.service
        # Adjust User=, WorkingDirectory=, and ExecStart= paths if needed
        # Save changes (Ctrl+X, then Y, then Enter)
        ```

    b.  **Copy the Service File:**
        ```bash
        sudo cp hass-unipi.service /etc/systemd/system/hass-unipi.service
        ```

    c.  **Reload Systemd, Enable and Start the Service:**
        ```bash
        sudo systemctl daemon-reload
        sudo systemctl enable hass-unipi.service
        sudo systemctl start hass-unipi.service
        ```

    d.  **Check Service Status:**
        ```bash
        sudo systemctl status hass-unipi.service
        ```
        You should see `Active: active (running)`. Press `q` to exit the status view.

    e.  **View Logs:**
        To see the script's output when run as a service:
        ```bash
        sudo journalctl -u hass-unipi.service -f
        ```
        Press `Ctrl+C` to stop following the logs.

7.  **Updating the Script:**
    When you make changes to the script on your development machine and push them to GitHub, you can easily update the script on the Raspberry Pi:
    ```bash
    cd /home/pi/unipi-homeassistant # Navigate to the script directory
    git pull origin main           # Fetch and merge the latest changes

    # If you updated hass-unipi.service, copy it again and reload systemd
    # sudo cp hass-unipi.service /etc/systemd/system/hass-unipi.service
    # sudo systemctl daemon-reload

    # Restart the service to apply changes
    sudo systemctl restart hass-unipi.service
    ```

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
*   When running as a systemd service, you can also view logs using `sudo journalctl -u hass-unipi.service`.

## Troubleshooting

*   **Connection Errors:** Check the log file (`~/.local/logs/connection_test.log` or `journalctl -u hass-unipi.service`) for details. Verify IP addresses, ports, usernames, and passwords in `config.json`. Ensure network connectivity between the script runner, the Unipi, and the MQTT broker.
*   **Devices Not Appearing in HA:** Verify the MQTT integration is set up correctly in Home Assistant. Check the Home Assistant logs and the script logs for MQTT connection errors or discovery message issues. Ensure the `device_name` derived from your Unipi model/SN is consistent.
*   **Dependencies Missing:** The script checks for required libraries on startup. If it exits with an error message about missing libraries, ensure you have activated the virtual environment and run the `pip install` command again.
*   **Service Fails to Start:** Check the service status (`sudo systemctl status hass-unipi.service`) and logs (`sudo journalctl -u hass-unipi.service`) for errors. Common issues include incorrect paths in the `.service` file, incorrect user permissions, or problems with the Python script itself (check `config.json`).
