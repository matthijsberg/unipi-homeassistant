"""
This script bridges communication between a Unipi device (over Websocket) and an MQTT broker.

It handles device discovery for Home Assistant, message conversion between Websocket and MQTT,
and graceful shutdown on Ctrl+C.

Version 202504-001
"""

import argparse  # Added for argument parsing
from collections import deque
import queue  # Added back
import threading
import time
import json
import logging
import random
import logging.handlers
import os
import asyncio
import signal
import secrets
import uuid
from dataclasses import dataclass
from functools import wraps
from typing import Any, Callable, Literal
from urllib.parse import urlparse
import importlib

import paho.mqtt.client as mqtt
import websockets
from websockets.protocol import State
import aiohttp
from aiohttp import web

from unipi_core.circuits import CircuitConfig, CircuitRegistry, canonicalize_circuits, normalize_dev
from unipi_core.commands import CommandService
from unipi_core.events import AVAILABILITY, INPUT_CHANGED, OUTPUT_CHANGED, EventBus
from unipi_core.sequencer import (
    OutputSequencer,
    SequenceRejected,
    WebSocketUnavailable,
    parse_command,
)

# --- Check Required Libraries ---
REQUIRED_LIBRARIES = {
    "paho.mqtt": "paho-mqtt",
    "websockets": "websockets",
    "aiohttp": "aiohttp",
    "pydantic": "pydantic",
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
    from pydantic import BaseModel, Field, HttpUrl, ValidationError, field_validator

    # Try to import pam here for usage later, but handle if missing during
    # actual execution inside handler logic if needed (though dependency check
    # should catch it)
    try:
        import pam  # type: ignore[import]
    except ImportError:
        pam = None
        print(
            "WARNING: 'python-pam' library not found. Authentication features will be disabled."
        )

    print("Required libraries check passed.")

# --- Script Version ---
SCRIPT_VERSION = "2.2.0-rc9"

# --- Constants ---
# Last discovered device name, so the MQTT last-will can use the device's own
# status topic from the very first connect (before evok has been queried).
DEVICE_NAME_CACHE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), ".device_name"
)

# Exit (and let systemd restart us) if MQTT stays down longer than this.
MQTT_WATCHDOG_SECONDS = 600

DEVICE_TYPE_MAPPING = {
    "input": "binary_sensor",
    "di": "binary_sensor",
    "relay": "switch",
    "ro": "switch",
    "do": "switch",
    "ai": "sensor",
    "ao": "light",
    "temp": "sensor",
    "led": "light",
    "1wdevice": "sensor",
}

# --- Pydantic Configuration Models ---


@dataclass
class MqttParts:
    dev: str | None = None
    circuit: str | None = None
    payload: Any = None
    cmd: str | None = None


class MqttConfig(BaseModel):
    broker: str
    port: int = 1883
    username: str | None = None
    password: str | None = None
    topic: str = "unipi"
    keepalive: int = 60
    discovery_prefix: str = "homeassistant"


class WebSocketConfig(BaseModel):
    url: str

    @field_validator("url")
    @classmethod
    def check_websocket_url(cls, v: str) -> str:
        if not v.startswith(("ws://", "wss://")):
            raise ValueError("WebSocket URL must start with ws:// or wss://")
        return v


class UnipiHttpConfig(BaseModel):
    url: HttpUrl


class LoggingConfig(BaseModel):
    level: str = "ERROR"
    file_path: str = "~/.local/logs/unipi_bridge.log"
    max_bytes: int = 5 * 1024 * 1024  # 5 MB
    backup_count: int = 3
    startup_debug_seconds: int = 60


class ExtensionsConfig(BaseModel):
    check_interval: int = 60
    max_comm_delay: int = 10


class WebServerConfig(BaseModel):
    enabled: bool = True
    port: int = 8088
    host: str = "0.0.0.0"
    allowed_users: list[str] = Field(default_factory=list)


class AppConfig(BaseModel):
    mqtt: MqttConfig
    websocket: WebSocketConfig
    unipi_http: UnipiHttpConfig = Field(
        ..., alias="unipi_http", validation_alias="unipi-http"
    )
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    extensions: ExtensionsConfig = Field(default_factory=ExtensionsConfig)
    web_server: WebServerConfig = Field(default_factory=WebServerConfig)
    inputs: dict[str, dict[str, bool]] = Field(default_factory=dict)
    # "shadow" = read-only twin that can run next to the live bridge: never writes to evok, never runs rules,
    # never starts the web UI, publishes under "<device>_shadow" names (and no HA discovery unless asked).
    mode: Literal["live", "shadow"] = "live"
    shadow_discovery: bool = False
    # Per-circuit settings keyed "<dev>/<circuit>", see unipi_core/circuits.py
    circuits: dict[str, CircuitConfig] = Field(default_factory=dict)

    @field_validator("circuits", mode="before")
    @classmethod
    def _canonical_circuit_keys(cls, v):
        return canonicalize_circuits(v)

    model_config = {
        "populate_by_name": True,
    }

    @classmethod
    def load_from_env_and_file(cls, config_file: str) -> "AppConfig":
        """Loads config from file and overrides with environment variables."""
        config_data = {}

        # 1. Load from file
        if os.path.exists(config_file):
            try:
                with open(config_file, "r") as f:
                    config_data = json.load(f)
            except json.JSONDecodeError as e:
                # Fail loudly instead of silently continuing to a confusing
                # "field required" pydantic error.
                logging.error(f"Error loading config file: {e}")
                raise ValueError(
                    f"Config file {config_file} is not valid JSON: {e}"
                ) from e

        # 2. Override with Env Vars
        # MQTT
        if os.getenv("MQTT_BROKER"):
            config_data.setdefault("mqtt", {})["broker"] = os.getenv("MQTT_BROKER")
        mqtt_port = os.getenv("MQTT_PORT")
        if mqtt_port:
            try:
                config_data.setdefault("mqtt", {})["port"] = int(mqtt_port)
            except ValueError as e:
                raise ValueError(
                    f"Environment variable MQTT_PORT must be an integer, got '{mqtt_port}'"
                ) from e
        if os.getenv("MQTT_USERNAME"):
            config_data.setdefault("mqtt", {})["username"] = os.getenv("MQTT_USERNAME")
        if os.getenv("MQTT_PASSWORD"):
            config_data.setdefault("mqtt", {})["password"] = os.getenv("MQTT_PASSWORD")
        if os.getenv("MQTT_TOPIC"):
            config_data.setdefault("mqtt", {})["topic"] = os.getenv("MQTT_TOPIC")
        mqtt_keepalive = os.getenv("MQTT_KEEPALIVE")
        if mqtt_keepalive:
            try:
                config_data.setdefault("mqtt", {})["keepalive"] = int(mqtt_keepalive)
            except ValueError as e:
                raise ValueError(
                    f"Environment variable MQTT_KEEPALIVE must be an integer, got '{mqtt_keepalive}'"
                ) from e
        if os.getenv("MQTT_DISCOVERY_PREFIX"):
            config_data.setdefault("mqtt", {})["discovery_prefix"] = os.getenv(
                "MQTT_DISCOVERY_PREFIX"
            )

        if os.getenv("UNIPI_MODE"):
            config_data["mode"] = os.getenv("UNIPI_MODE", "").lower()

        # WebSocket
        if os.getenv("WEBSOCKET_URL"):
            config_data.setdefault("websocket", {})["url"] = os.getenv("WEBSOCKET_URL")

        # Unipi HTTP
        if os.getenv("UNIPI_URL"):
            config_data.setdefault("unipi_http", {})["url"] = os.getenv("UNIPI_URL")

        # Logging
        log_level = os.getenv("LOG_LEVEL")
        if log_level:
            config_data.setdefault("logging", {})["level"] = log_level.upper()
        if os.getenv("LOG_FILE_PATH"):
            config_data.setdefault("logging", {})["file_path"] = os.getenv(
                "LOG_FILE_PATH"
            )

        return cls(**config_data)


# --- Websocket Version Check ---
WEBSOCKETS_MIN_VERSION = (14, 0)
current_version_str = getattr(websockets, "__version__", "0.0.0")
current_version_tuple = tuple(map(int, current_version_str.split(".")))[:2]

if current_version_tuple < WEBSOCKETS_MIN_VERSION:
    print(
        f"Warning: Your 'websockets' library version is {current_version_str}. "
        f"Version {'.'.join(map(str, WEBSOCKETS_MIN_VERSION))}+ is recommended. "
        f"Upgrade with: pip install --upgrade websockets."
    )

# --- Helper Decorator ---


def log_function(func: Callable) -> Callable:
    """
    Decorator to log function entry and exceptions.

    Args:
        func: The function to wrap.

    Returns:
        Callable: The wrapped function.
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        # logger is now an instance attribute or we get it via logging.getLogger
        # For simplicity in transition, we'll use a local logger lookup
        local_logger = logging.getLogger(func.__module__)
        local_logger.debug(f"Entering function: {func.__name__}")
        try:
            return func(*args, **kwargs)
        except Exception as e:
            local_logger.error(f"Error in function {func.__name__}: {e}", exc_info=True)
            raise

    return wrapper


# --- Traffic Recorder ---


class TrafficRecorder:
    def __init__(self, filename: str):
        self.filename = filename
        self.record_count = 0
        self.lock = threading.Lock()
        self.start_time = time.time()

    def record_ws_to_mqtt(
        self, ws_message: dict[str, Any], mqtt_messages: list[tuple[str, str]]
    ) -> None:
        entry = {
            "type": "ws_to_mqtt",
            "timestamp": time.time() - self.start_time,
            "input": ws_message,
            "output": [{"topic": t, "payload": p} for t, p in mqtt_messages],
        }
        self._append(entry)

    def record_mqtt_to_ws(
        self,
        mqtt_topic: str,
        mqtt_payload: str,
        ws_command: dict[str, Any],
        ack_messages: list[tuple[str, str]],
    ) -> None:
        entry = {
            "type": "mqtt_to_ws",
            "timestamp": time.time() - self.start_time,
            "input": {"topic": mqtt_topic, "payload": mqtt_payload},
            "output": ws_command,
            "ack": [{"topic": t, "payload": p} for t, p in ack_messages],
        }
        self._append(entry)

    def _append(self, entry: dict[str, Any]) -> None:
        # Append one JSON object per line (JSON-Lines). Avoids rewriting the
        # whole file on every message (previously O(n^2) and unbounded memory).
        with self.lock:
            try:
                with open(self.filename, "a") as f:
                    f.write(json.dumps(entry) + "\n")
                self.record_count += 1
            except Exception as e:
                print(f"Error saving traffic record: {e}")


# -----------------------------------------------------------------------------
# Local Logic Engine
# -----------------------------------------------------------------------------


class RuleCondition(BaseModel):
    dev: str
    circuit: str
    operator: str = "eq"  # eq, ne, gt, lt, ge, le
    value: int | float | str | None = None


class LocalLogicRule(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    trigger_dev: str
    trigger_circuit: str
    trigger_operator: str = "eq"  # eq, ne, gt, lt, ge, le, any
    trigger_value: int | float | str | None = None
    conditions: list[RuleCondition] = Field(default_factory=list)
    action_type: str = "set"  # set, dimmer
    action_dev: str
    action_circuit: str
    action_value: int | float | str | None = None
    action_transition: float | None = None
    action_delay: float | None = None
    # T17: pulse / toggle / dimmer options and the 'only when Home Assistant is unreachable' switch
    action_pulse: dict[str, Any] | None = None   # {"count": 3, "on_ms": 100, "off_ms": 250} (on/off optional)
    action_preset: str | None = None             # name of a preset on the target circuit (pulse or timed)
    when: str = "always"                         # always | ha_offline
    dimmer_hold: bool = True                     # False = toggle on the press edge, no hold-to-dim
    dimmer_hold_ms: int = 500                    # how long to hold before dimming starts (short press = toggle)
    dimmer_speed: float = 2.5                    # volts per second while dimming (2.5 = full range in 4 s)
    dimmer_min: float = 1.0                      # dimming down stops here (lamp stays on); a short press switches off



class LocalLogicEngine:
    def __init__(self, rules_file: str, logger: logging.Logger, validator=None):
        self.rules_file = rules_file
        self.logger = logger
        self.validator = validator            # rule -> error text | None (set by the bridge)
        self.disabled: dict[str, str] = {}    # rule id -> why it is not evaluated (rules stay in the file)
        # What the engine saw and decided (newest last); shown live in the editor's activity panel
        self.trace: deque = deque(maxlen=300)
        self._seq = 0
        self._last_nonmatch: dict[str, float] = {}
        self.rules: list[LocalLogicRule] = []
        self.load_rules()

    def record(self, rule_id: str | None, rule_name: str | None, step: str, ok: bool, detail: str) -> None:
        self._seq += 1
        self.trace.append({"seq": self._seq, "ts": time.time(), "rule_id": rule_id, "rule": rule_name,
                           "step": step, "ok": ok, "detail": detail})

    def trace_since(self, since: int = 0) -> dict[str, Any]:
        return {"last": self._seq, "events": [e for e in self.trace if e["seq"] > since]}

    def revalidate(self) -> None:
        """Disable (not delete) rules that are invalid for the current circuit configuration."""
        self.disabled = {}
        if not self.validator:
            return
        for r in self.rules:
            err = self.validator(r)
            if err:
                self.disabled[r.id] = err
                # A WARNING, not an ERROR: a typo in a user's rule is not a bridge fault (it must not raise the
                # "startup error" alarm or fail a deploy). It is shown on the rule's block in the editor.
                self.logger.warning(f"Local rule '{r.name}' ({r.id}) is DISABLED: {err}")

    def load_rules(self) -> None:
        try:
            if os.path.exists(self.rules_file):
                with open(self.rules_file, "r") as f:
                    data = json.load(f)
                    self.rules = [LocalLogicRule(**item) for item in data]
                self.revalidate()
                self.logger.info(f"Loaded {len(self.rules)} local logic rules.")
            else:
                self.logger.info(
                    "No local rules file found. Starting with empty rules."
                )
                self.rules = []
        except Exception as e:
            self.logger.error(f"Failed to load local rules: {e}")
            self.rules = []

    def save_rules(self) -> None:
        try:
            data = [rule.model_dump() for rule in self.rules]
            with open(self.rules_file, "w") as f:
                json.dump(data, f, indent=4)
            self.logger.info("Saved local logic rules.")
        except Exception as e:
            self.logger.error(f"Failed to save local rules: {e}")

    def _compare_values(self, actual_val: Any, expected_val: Any, operator: str) -> bool:
        if expected_val is None:
            return False
        if operator == "any":
            return True
        try:
            # Try numeric conversion
            try:
                val_actual = float(actual_val)
                val_expected = float(expected_val)
            except (ValueError, TypeError):
                # Fallback to string
                val_actual = str(actual_val)
                val_expected = str(expected_val)

            match operator:
                case "eq":
                    if isinstance(val_actual, float) and isinstance(val_expected, float):
                        return abs(val_actual - val_expected) < 0.01
                    return str(val_actual) == str(val_expected)
                case "ne":
                    if isinstance(val_actual, float) and isinstance(val_expected, float):
                        return abs(val_actual - val_expected) >= 0.01
                    return str(val_actual) != str(val_expected)
                case "gt":
                    return val_actual > val_expected  # type: ignore
                case "lt":
                    return val_actual < val_expected  # type: ignore
                case "ge":
                    return val_actual >= val_expected  # type: ignore
                case "le":
                    return val_actual <= val_expected  # type: ignore
        except Exception:
            pass
        return False

    def _get_device_state(self, dev: str, circuit: str, device_states: dict[str, Any]) -> Any:
        # Try direct match (canonical evok-3 name first: input->di, analogoutput->ao, ...)
        val = device_states.get(f"{normalize_dev(dev)}_{circuit}")
        if val is None:
            val = device_states.get(f"{dev}_{circuit}")
        if val is not None:
            return val
        # Try aliases
        if dev == "input":
            val = device_states.get(f"di_{circuit}")
        elif dev == "di":
            val = device_states.get(f"input_{circuit}")
        elif dev == "analogoutput":
            val = device_states.get(f"ao_{circuit}")
        elif dev == "ao":
            val = device_states.get(f"analogoutput_{circuit}")
        elif dev in ("temp", "humidity", "vdd", "vad", "vis"):
            val = device_states.get(f"1wdevice_{circuit}_{dev}")
        return val

    def evaluate(self, message: dict[str, Any], device_states: dict[str, Any] = None) -> list[dict[str, Any]]:
        """
        Evaluates a WebSocket message against the rules.
        Returns a list of actions (MQTT-like command dicts) to execute.
        """
        actions = []

        msg_dev = message.get("dev")
        msg_circuit = message.get("circuit")
        msg_value = message.get("value")

        if msg_dev is None or msg_circuit is None or msg_value is None:
            return actions

        OPS = {"eq": "=", "ne": "!=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=", "any": "any change"}
        now = time.time()
        for rule in self.rules:
            # Device names are compared in their canonical evok-3 form: rules saved with the old evok-2
            # names ("input", "analogoutput", "relay", "output") keep working.
            if not (normalize_dev(rule.trigger_dev) == normalize_dev(msg_dev) and rule.trigger_circuit == msg_circuit):
                continue
            if rule.id in self.disabled:
                self.record(rule.id, rule.name, "disabled", False, f"rule is disabled: {self.disabled[rule.id]}")
                continue

            dim_hold = rule.action_type == "dimmer" and rule.dimmer_hold
            try:
                is_release = int(float(msg_value)) == 0
            except (TypeError, ValueError):
                is_release = False
            if rule.trigger_operator == "any" or dim_hold:
                match = True
            else:
                match = self._compare_values(msg_value, rule.trigger_value, rule.trigger_operator)
            need = OPS.get(rule.trigger_operator, rule.trigger_operator)
            if rule.trigger_operator != "any":
                need += f" {rule.trigger_value}"
            if dim_hold:
                need = "button press and release (push-to-dim)"
            seen = f"{msg_dev}/{msg_circuit} = {msg_value}"
            if match:
                self.record(rule.id, rule.name, "trigger", True, f"{seen} - trigger matched ({need})")
            elif now - self._last_nonmatch.get(rule.id, 0) >= 2.0:   # don't flood on fast analog values
                self._last_nonmatch[rule.id] = now
                self.record(rule.id, rule.name, "trigger", False, f"{seen} - no match, needs {need}")

            # Check secondary conditions if trigger matched (never on the release of a push-to-dim)
            if match and rule.conditions and not (dim_hold and is_release):
                for cond in rule.conditions:
                    current_val = self._get_device_state(cond.dev, cond.circuit, device_states or {})
                    if not self._compare_values(current_val, cond.value, cond.operator):
                        match = False
                        self.record(rule.id, rule.name, "conditions", False,
                                    f"condition {cond.dev}/{cond.circuit} is {current_val!r}, needs "
                                    f"{OPS.get(cond.operator, cond.operator)} {cond.value} - stopped")
                        break

            if match:
                self.logger.info(
                    f"Rule '{rule.name}' triggered by {msg_dev} {msg_circuit} ({msg_value}) {rule.trigger_operator} {rule.trigger_value}"
                )
                action = {
                    "type": getattr(rule, "action_type", "set"),
                    "dev": normalize_dev(rule.action_dev),
                    "circuit": rule.action_circuit,
                    "value": rule.action_value,
                    "trigger_value": msg_value,
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "pulse": rule.action_pulse,
                    "preset": rule.action_preset,
                    "when": rule.when,
                    "hold": rule.dimmer_hold,
                    "hold_ms": rule.dimmer_hold_ms,
                    "speed": rule.dimmer_speed,
                    "min_v": rule.dimmer_min,
                }
                if rule.action_transition is not None:
                    action["transition"] = rule.action_transition
                if getattr(rule, "action_delay", None) is not None:
                    action["delay"] = rule.action_delay
                actions.append(action)

        return actions

    def replace_rules(self, new_rules: list[LocalLogicRule]) -> None:
        """
        Replaces all existing rules with a new set of rules.

        Args:
            new_rules: The new list of rules.
        """
        self.rules = new_rules
        self.revalidate()
        self.save_rules()
        self.logger.info(f"Replaced all rules. Count: {len(self.rules)}")


class UnipiBridge:
    def __init__(
        self,
        config: AppConfig,
        config_path: str,
        recorder: TrafficRecorder | None = None,
    ):
        """
        Initialize the UnipiBridge.

        Args:
            config: The application configuration object.
            config_path: Path to the configuration file (for saving).
            recorder: Optional TrafficRecorder instance.
        """
        self.config = config
        self.config_path = config_path
        self.recorder = recorder
        self.logger = self._setup_logging()
        self.circuits = CircuitRegistry(self.config, self.logger)
        # local_rules.json lives next to the config file, not in whatever CWD we were started from
        rules_path = os.path.join(os.path.dirname(os.path.abspath(config_path)), "local_rules.json")
        # Home Assistant reachability (birth/last-will on <discovery_prefix>/status); None = never seen
        self.ha_online: bool | None = None
        self._dimmer_state_file = os.path.join(os.path.dirname(rules_path), "local_rules_state.json")
        self._dim_sleep = asyncio.sleep   # replaced in tests so dimming can run on a fake clock
        self.dimmer_persist: dict[str, dict[str, Any]] = self._load_dimmer_state()
        self.local_logic = LocalLogicEngine(rules_path, self.logger, validator=self.validate_rule)

        # State
        self.shadow: bool = self.config.mode == "shadow"
        self.device_name: str = self._load_cached_device_name()
        # Topic of the last-will registered with the broker on (re)connect
        self._lwt_topic: str = ""
        self.mqtt_subscribe_topics: list[str] = []
        self.websocket_connection: Any | None = None
        self.should_stop = threading.Event()
        self.is_first_connect: bool = True
        self.mqtt_connected: bool = False
        # Home Assistant publishes "online" here when it (re)starts
        self.ha_status_topic = f"{self.config.mqtt.discovery_prefix}/status"
        self._republish_task: asyncio.Task | None = None
        # Process exit code; set to 1 by the MQTT watchdog so systemd restarts us
        self.exit_code: int = 0
        self.active_ao_transitions: dict[tuple[str, str], int] = {}
        self.active_delayed_actions: dict[tuple[str, str], asyncio.Task] = {}
        # Stores current value of devices: "dev_circuit" -> value
        self.device_states: dict[str, Any] = {}
        # Store discovered devices for API
        self.discovered_devices: list[dict[str, Any]] = []
        self.sessions: dict[str, dict[str, Any]] = {}  # HTTP Sessions
        # Per-IP failed login timestamps (brute-force rate limiting)
        self.failed_logins: dict[str, list[float]] = {}
        # Throttle for periodic expired-session sweep
        self._last_session_cleanup: float = 0.0
        # Stores device info (SN, Model)
        self.device_info: dict[str, Any] | None = None
        self.dimmer_states: dict[str, dict[str, Any]] = {}
        # T15: per-circuit counter publishing state {published, published_at, pending}
        self._counters: dict[str, dict[str, Any]] = {}

        # Queues
        self.mqtt_to_websocket_queue: queue.Queue[str] = queue.Queue(maxsize=1000)
        self.websocket_to_mqtt_queue: queue.Queue[tuple[str, str]] = queue.Queue(
            maxsize=1000
        )
        self.worker_threads: list[threading.Thread] = []

        # HTTP Session
        self.session: aiohttp.ClientSession | None = None
        self.web_static_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "web", "static"
        )

        # Asyncio
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.initial_websocket_ready = asyncio.Event()
        self.initial_discovery_complete = asyncio.Event()

        # Internal API: observe state changes (events) / drive outputs (commands)
        self.events = EventBus(self.loop, self.logger)
        self.commands = CommandService(self)
        # Output sequencer: pulse trains / timed outputs with limits and fail-safes (T12)
        self.sequencer = OutputSequencer(
            write=self._ws_write_now,
            ack=lambda dev, circuit, on, origin: self.mqtt_ack(
                "ON" if on else "OFF", dev, circuit, origin=origin
            ),
            set_attributes=self._publish_attributes,
            limits_for=lambda dev, circuit: self.circuits.limits(dev, circuit),
            is_ready=lambda: self.websocket_connection is not None
            and self.websocket_connection.state == State.OPEN,
            logger=self.logger,
        )
        self.events.subscribe(INPUT_CHANGED, self._on_input_for_sequencer)
        self.events.subscribe(OUTPUT_CHANGED, self._on_output_for_sequencer)
        self.events.subscribe(OUTPUT_CHANGED, self._on_output_track_state)

        # MQTT Client
        api_version = getattr(mqtt, "CallbackAPIVersion", None)
        if api_version is not None:
            self.mqtt_client = mqtt.Client(api_version.VERSION2)
        else:
            self.mqtt_client = mqtt.Client(2)  # type: ignore

        self.mqtt_client.username_pw_set(
            self.config.mqtt.username, self.config.mqtt.password
        )
        self.mqtt_client.on_connect = self.on_mqtt_connect  # type: ignore
        self.mqtt_client.on_message = self.on_mqtt_message
        self.mqtt_client.on_disconnect = self.on_mqtt_disconnect  # type: ignore

    def _setup_logging(self) -> logging.Logger:
        """
        Configures the root logger and returns a class-specific logger.

        Returns:
            logging.Logger: The logger instance for UnipiBridge.
        """
        log_file_path = self.config.logging.file_path
        if log_file_path.startswith("~"):
            log_dir = os.path.expanduser(os.path.dirname(log_file_path))
            log_file = os.path.join(log_dir, os.path.basename(log_file_path))
        else:
            log_dir = os.path.dirname(log_file_path)
            log_file = log_file_path

        if log_dir:
            os.makedirs(log_dir, exist_ok=True)

        # Create Handlers
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self.config.logging.max_bytes,
            backupCount=self.config.logging.backup_count,
        )
        # Force a rollover on startup to ensure a fresh log file for this session
        # This makes scanning for startup errors much easier and accurate
        if os.path.exists(log_file) and os.path.getsize(log_file) > 0:
            file_handler.doRollover()

        console_handler = logging.StreamHandler()

        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(name)s - %(threadName)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)

        # Configure Root Logger to capture EVERYTHING
        root_logger = logging.getLogger()

        # Force DEBUG level initially for startup analysis
        root_logger.setLevel(logging.DEBUG)

        # Remove existing handlers to avoid duplication
        if root_logger.hasHandlers():
            root_logger.handlers.clear()

        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)

        # Return a specific logger for this class, but rely on root
        # configuration
        return logging.getLogger("UnipiBridge")

    def run(self) -> None:
        """
        Main entry point. Starts worker threads, MQTT connection, and the asyncio loop.
        """
        # Always log startup information regardless of log level
        self.logger.critical("---------------------------------------------------")
        self.logger.critical("   UnipiBridge Started")
        self.logger.critical(f"   Version: {SCRIPT_VERSION}")
        if self.shadow:
            self.logger.critical("   MODE: SHADOW (read-only; no evok writes, no rules, no web UI)")
        self.logger.critical("---------------------------------------------------")

        self.logger.info(f"UnipiBridge Started - Version: {SCRIPT_VERSION}")
        self.logger.info(f"Log Level: {self.config.logging.level}")

        # Start Web Server
        if self._web_server_enabled():
            self.loop.create_task(self.setup_web_server())

        if self.recorder:
            self.logger.critical(
                f"   Traffic Recording ENABLED. Saving to: {self.recorder.filename}"
            )
            self.logger.info(
                f"Traffic recording enabled. Saving to: {self.recorder.filename}"
            )

        # Register signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Start Worker Threads
        self.start_worker_threads()

        # Connect MQTT
        self.connect_mqtt()

        # Create WebSocket Handler Task
        self.loop.create_task(self.websocket_handler(), name="WebSocketHandlerTask")

        # Start Async Startup Task
        self.loop.create_task(self._async_startup(), name="AsyncStartupTask")

        # Start Dynamic Logging Manager
        self.loop.create_task(
            self._manage_startup_logging(), name="StartupLoggingManager"
        )

        # Start Extension Monitor
        self.loop.create_task(self._monitor_extensions(), name="ExtensionMonitorTask")

        # Start MQTT Monitor
        self.loop.create_task(self._mqtt_monitor(), name="MqttMonitorTask")

        # Pulse counters: REST safety net next to the WebSocket pushes (only if any counter is configured)
        if any(c.counter for c in self.config.circuits.values()):
            self.loop.create_task(self._counter_poller(), name="CounterPollerTask")

        # Max-on watchdog for outputs with max_on_s
        self.loop.create_task(self._sequencer_watchdog(), name="SequencerWatchdogTask")

        # Run Loop
        try:
            self.logger.info("Starting asyncio event loop...")
            self.loop.run_forever()
        except (KeyboardInterrupt, SystemExit):
            self.logger.info("Received stop signal. Shutting down...")
        finally:
            self.shutdown()

    def _signal_handler(self, signum, frame):
        self.logger.info(f"Signal {signum} received. Initiating shutdown...")
        self.should_stop.set()
        # Schedule shutdown in the loop to be thread-safe
        asyncio.run_coroutine_threadsafe(self._stop_loop(), self.loop)

    async def _stop_loop(self):
        self.loop.stop()

    def shutdown(self) -> None:
        """
        Gracefully shuts down the application, cancelling tasks and joining threads.
        """
        self.logger.info("Shutdown sequence started.")
        self.should_stop.set()

        # Never leave a bell coil / window motor energised: cancel sequences, drive failsafe outputs OFF
        try:
            self.loop.run_until_complete(asyncio.wait_for(self._shutdown_outputs(), 5))
        except Exception as e:
            self.logger.error(f"Output fail-safe at shutdown failed: {e}")

        # Cancel all running tasks
        pending = asyncio.all_tasks(self.loop)
        for task in pending:
            task.cancel()

        # Allow tasks to cancel
        if pending:
            self.loop.run_until_complete(
                asyncio.gather(*pending, return_exceptions=True)
            )

        # Join Worker Threads
        self.logger.info("Joining worker threads...")
        for t in self.worker_threads:
            if t.is_alive():
                t.join(timeout=2.0)
                if t.is_alive():
                    self.logger.warning(f"Thread {t.name} did not exit within timeout.")
                else:
                    self.logger.debug(f"Thread {t.name} joined successfully.")

        # Stop MQTT
        if self.mqtt_client:
            self.logger.info("Disconnecting MQTT...")
            try:
                # Explicitly publish 'offline' before disconnecting. A clean
                # disconnect discards the LWT, and will_set() has no effect once
                # the connection is established, so we publish it ourselves. The
                # network loop must still be running for the publish to go out,
                # so loop_stop() is called only after wait_for_publish().
                lwt_topic = self._lwt_topic_for_device()
                info = self.mqtt_client.publish(
                    lwt_topic, payload="offline", qos=1, retain=True
                )
                try:
                    info.wait_for_publish(timeout=3)
                except Exception:
                    pass

                self.mqtt_client.disconnect()
                self.mqtt_client.loop_stop()
            except Exception as e:
                self.logger.error(f"Error disconnecting MQTT: {e}")

        # Close Loop
        # shutdown() runs in the finally of run(), i.e. after run_forever()
        # returned, so the loop is NOT running (but not yet closed). Gate on
        # is_closed() so the session-close and loop.close() actually execute.
        if not self.loop.is_closed():
            if self.session and not self.session.closed:
                self.logger.info("Closing aiohttp session...")
                self.loop.run_until_complete(self.session.close())

            self.loop.close()
        self.logger.info("UnipiBridge stopped.")

    async def _manage_startup_logging(self) -> None:
        """
        Manages the startup logging period.
        1. Waits for the configured startup duration (default 60s).
        2. Reverts the log level to the configured value.
        3. Scans the current log file for errors.
        4. Publishes error status and details to MQTT.
        """
        startup_duration = self.config.logging.startup_debug_seconds
        self.logger.info(
            f"Startup logging: DEBUG mode enabled for {startup_duration} seconds."
        )

        try:
            await asyncio.sleep(startup_duration)
        except asyncio.CancelledError:
            return

        # 1. Scan Log File for Errors (Before changing level)
        log_file_path = self.config.logging.file_path
        # Expand user if needed (though _setup_logging handles this for the
        # handler, we need the path here)
        if log_file_path.startswith("~"):
            log_file_path = os.path.expanduser(log_file_path)

        errors_found = []
        try:
            if os.path.exists(log_file_path):
                with open(log_file_path, "r") as f:
                    for line in f:
                        if "ERROR" in line or "CRITICAL" in line:
                            # Skip the startup banner criticals to avoid false
                            # positives
                            if (
                                "UnipiBridge Started" in line
                                or "Version:" in line
                                or "Device Discovered:" in line
                                or "Traffic Recording ENABLED" in line
                                or "----------------" in line
                            ):
                                continue
                            errors_found.append(line.strip())
            else:
                self.logger.warning(
                    f"Startup logging: Log file not found at {log_file_path}"
                )
        except Exception as e:
            self.logger.error(f"Startup logging: Failed to scan log file: {e}")
            errors_found.append(f"Failed to scan log file: {e}")

        # 2. Prepare Status Message & Publish to MQTT
        root_topic = self.config.mqtt.topic

        log_level_str = self.config.logging.level.upper()

        if errors_found:
            # Limit to first 20 errors
            error_summary = "\n".join(errors_found[:20])
            if len(errors_found) > 20:
                error_summary += f"\n... and {len(errors_found) - 20} more."

            msg = f"Startup logging: Detected {len(errors_found)} errors during startup. Log level will be changed to {log_level_str}."
            self.logger.error(msg)
            # Use _bridge topic
            bridge_topic = (
                f"{root_topic}/{self.device_name}_bridge"
                if self.device_name
                else f"{root_topic}/unknown_device_bridge"
            )
            self.mqtt_client.publish(f"{bridge_topic}/startup_error", "1", retain=True)
            self.mqtt_client.publish(
                f"{bridge_topic}/startup_details", error_summary, retain=True
            )
        else:
            msg = f"Startup logging: No errors detected during startup. Log level will be changed to {log_level_str}."
            self.logger.info(msg)
            # Use _bridge topic
            bridge_topic = (
                f"{root_topic}/{self.device_name}_bridge"
                if self.device_name
                else f"{root_topic}/unknown_device_bridge"
            )
            self.mqtt_client.publish(f"{bridge_topic}/startup_error", "0", retain=True)
            self.mqtt_client.publish(
                f"{bridge_topic}/startup_details",
                "No startup errors detected.",
                retain=True,
            )

        # 3. Revert Log Level
        target_level = getattr(logging, log_level_str, logging.INFO)
        logging.getLogger().setLevel(target_level)
        self.logger.setLevel(target_level)
        self.logger.info(f"Startup logging: Reverted log level to {log_level_str}.")

    async def _async_startup(self) -> None:
        self.logger.info("Async startup: Waiting for initial WebSocket connection...")

        # Initialize aiohttp session
        self.session = aiohttp.ClientSession()

        try:
            await self.initial_websocket_ready.wait()
            self.logger.info("Async startup: WebSocket ready signal received.")

            self.logger.info(
                "Async startup: Performing initial Home Assistant discovery..."
            )
            # Now we can await directly since it's async
            discovered_device_name = await self.perform_discovery_and_mqtt_subscribe()

            if discovered_device_name:
                self.device_name = discovered_device_name
                self.logger.critical(f"   Device Discovered: {self.device_name}")
                self.logger.info(
                    f"Async startup: Discovery complete. Device Name: {self.device_name}"
                )
                self.initial_discovery_complete.set()

                # Last-will was already moved to this topic during discovery
                lwt_topic = self._lwt_topic_for_device()
                self.mqtt_client.publish(lwt_topic, "online", retain=True)

                # 3. Publish Discovery Config
                # Publish Bridge Discovery (Error Sensors)
                self.publish_bridge_discovery()

            else:
                self.logger.error(
                    "Async startup: Discovery failed. Device name not found."
                )

        except asyncio.CancelledError:
            self.logger.info("Async startup task cancelled.")
        except Exception as e:
            self.logger.error(f"Async startup error: {e}", exc_info=True)

    def _web_server_enabled(self) -> bool:
        """The rule-editor web UI never runs in a shadow instance (it would fight the live one for the port)."""
        return self.config.web_server.enabled and not self.shadow

    def _shadow_tag(self) -> str:
        return " (shadow)" if self.shadow else ""

    def _load_cached_device_name(self) -> str:
        if self.shadow:
            return ""  # the cache belongs to the live instance
        try:
            with open(DEVICE_NAME_CACHE_FILE) as f:
                return f.read().strip()
        except OSError:
            return ""

    def _save_cached_device_name(self) -> None:
        if self.shadow:
            return
        try:
            with open(DEVICE_NAME_CACHE_FILE, "w") as f:
                f.write(self.device_name)
        except OSError as e:
            self.logger.warning(f"Could not cache device name: {e}")

    def _lwt_topic_for_device(self) -> str:
        if self.device_name:
            return f"{self.config.mqtt.topic}/{self.device_name}/status"
        return f"{self.config.mqtt.topic}/{self.config.mqtt.topic}_bridge/status"

    async def _ensure_lwt_topic(self) -> None:
        """
        Makes sure the broker holds a last-will on the device's own status
        topic. will_set() only applies to the next CONNECT, so if we are
        connected with a different will (first run, or the device name
        changed) do one clean reconnect. Called before discovery is published,
        so nothing is lost across the reconnect.
        """
        lwt_topic = self._lwt_topic_for_device()
        if lwt_topic == self._lwt_topic:
            return

        self.logger.warning(
            f"Last-will topic changes to '{lwt_topic}'; reconnecting MQTT to apply it."
        )
        self._lwt_topic = lwt_topic
        self.mqtt_client.will_set(lwt_topic, payload="offline", qos=1, retain=True)
        if not self.mqtt_client.is_connected():
            return  # paho's pending (re)connect will use the new will

        def reconnect() -> None:
            # A clean disconnect ends paho's loop thread, so restart it.
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()
            self.mqtt_client.connect_async(
                self.config.mqtt.broker,
                self.config.mqtt.port,
                keepalive=self.config.mqtt.keepalive,
            )
            self.mqtt_client.loop_start()

        await self.loop.run_in_executor(None, reconnect)
        for _ in range(100):  # wait up to 10s; paho keeps retrying after that
            if self.mqtt_client.is_connected():
                break
            await asyncio.sleep(0.1)

    def connect_mqtt(self) -> None:
        """
        Starts the (non-blocking) MQTT connection; paho handles all retries.
        """
        lwt_topic = self._lwt_topic_for_device()
        self._lwt_topic = lwt_topic
        self.logger.info(f"Configuring LWT: Topic='{lwt_topic}', Payload='offline'")
        self.mqtt_client.will_set(lwt_topic, payload="offline", qos=1, retain=True)

        # Configure exponential backoff for automatic reconnection (Paho MQTT)
        self.mqtt_client.reconnect_delay_set(min_delay=1, max_delay=30)

        # connect_async() does not block: paho's loop thread keeps retrying
        # until the broker is reachable, so the WebSocket side and local logic
        # start even when the broker (or its host) is still down.
        self.logger.info(
            f"Connecting to MQTT broker {self.config.mqtt.broker}:{self.config.mqtt.port} "
            "(retrying in the background until reachable)..."
        )
        self.mqtt_client.connect_async(
            self.config.mqtt.broker,
            self.config.mqtt.port,
            keepalive=self.config.mqtt.keepalive,
        )
        self.mqtt_client.loop_start()
        self.logger.info("MQTT background loop started.")

    def start_worker_threads(self) -> None:
        """
        Starts the MQTT and WebSocket worker threads.
        """
        mqtt_worker = threading.Thread(
            target=self.mqtt_worker_thread, name="MQTTWorker"
        )
        mqtt_worker.daemon = True
        self.worker_threads.append(mqtt_worker)
        mqtt_worker.start()

        websocket_worker = threading.Thread(
            target=self.websocket_worker_thread, name="WebSocketWorker"
        )
        websocket_worker.daemon = True
        self.worker_threads.append(websocket_worker)
        websocket_worker.start()

    def on_mqtt_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: int,
        properties: Any = None,
    ) -> None:
        """
        Callback for MQTT connection.

        Args:
            client: The MQTT client instance.
            userdata: User data (unused).
            flags: Response flags.
            reason_code: The connection reason code.
            properties: MQTT v5 properties.
        """
        self.logger.info(
            f"on_mqtt_connect called with reason code: {reason_code}, flags: {flags}"
        )
        if reason_code == 0:
            self.mqtt_connected = True
            self.logger.info(
                f"Successfully connected to MQTT Broker: {self.config.mqtt.broker}:{self.config.mqtt.port}"
            )

            def set_first_connect_false_delayed() -> None:
                self.logger.debug(
                    "Starting delay timer for initial retained MQTT messages..."
                )
                time.sleep(2)
                if self.is_first_connect:
                    self.logger.info(
                        "Initial MQTT message delay timer expired. Processing all messages now."
                    )
                    self.is_first_connect = False
                else:
                    self.logger.debug(
                        "Initial MQTT message delay timer expired, but is_first_connect was already False."
                    )

            timer_thread = threading.Thread(
                target=set_first_connect_false_delayed, name="MqttRetainedMsgTimer"
            )
            timer_thread.daemon = True
            timer_thread.start()

            if self.device_name:
                # Only publish online if WebSocket is also connected (or if we are just starting up and don't know yet?
                # Actually, if we are reconnecting MQTT, we should reflect the
                # current WS state)
                if (
                    self.websocket_connection
                    and self.websocket_connection.state == State.OPEN
                ):
                    self.publish_availability("online")
                else:
                    # If WS is down, we might want to publish offline, or just
                    # wait for WS to come up
                    self.logger.info(
                        "MQTT connected, but WebSocket is not open. Skipping 'online' availability publish."
                    )
            else:
                self.logger.warning(
                    "Cannot publish online status: device_name not yet set."
                )

            self.logger.info(
                f"Subscribing to {len(self.mqtt_subscribe_topics)} MQTT topics..."
            )
            for topic in self.mqtt_subscribe_topics:
                self.logger.debug(f"Subscribing to MQTT topic: {topic}")
                try:
                    result, mid = client.subscribe(topic, qos=1)
                    if result == mqtt.MQTT_ERR_SUCCESS:
                        self.logger.debug(
                            f"Successfully initiated subscription to {topic} (MID: {mid})"
                        )
                    else:
                        self.logger.error(
                            f"Failed to initiate subscription to {topic}. Result: {result}"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Error during subscription to {topic}: {e}", exc_info=True
                    )

            # Home Assistant announces its (re)start on this topic
            client.subscribe(self.ha_status_topic, qos=1)

            # A reconnect after startup usually means the broker (or its host)
            # restarted and may have lost our retained discovery/state
            # messages, so send everything again.
            if self.initial_discovery_complete.is_set():
                self.loop.call_soon_threadsafe(
                    self._schedule_republish, "MQTT reconnected"
                )
        else:
            error_message = (
                f"Failed to connect to MQTT Broker. Reason Code: {reason_code}"
            )
            self.logger.error(error_message)

    def on_mqtt_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        """
        Callback for incoming MQTT messages.

        Args:
            client: The MQTT client instance.
            userdata: User data (unused).
            message: The received MQTT message object.
        """
        topic = message.topic
        is_retained = message.retain

        self.logger.debug(
            f"Received MQTT message - Topic: {topic}, Retained: {is_retained}"
        )

        if topic == self.ha_status_topic:
            # Retained copies arrive on every reconnect, which already
            # triggers a republish; only react to a live HA birth message.
            status = message.payload.decode("utf-8", errors="ignore").strip().lower()
            self.ha_online = status == "online"
            if (
                status == "online"
                and not is_retained
                and self.initial_discovery_complete.is_set()
            ):
                self.loop.call_soon_threadsafe(
                    self._schedule_republish, "Home Assistant (re)started"
                )
            return

        if self.is_first_connect and is_retained:
            self.logger.debug(
                f"Ignoring retained message during initial connection phase: {topic}"
            )
            return

        if is_retained and (topic.endswith("/set") or topic.endswith("/output/set")):
            self.logger.debug(f"Ignoring retained command message: {topic}")
            return

        try:
            payload = message.payload.decode("utf-8", errors="strict")
        except UnicodeDecodeError as e:
            self.logger.error(
                f"Payload decoding error for topic {topic}: {e}", exc_info=True
            )
            return

        try:
            payload_data, data_type = self.process_payload(payload)
        except Exception as e:
            self.logger.error(
                f"Payload processing error for topic {topic}: {e}", exc_info=True
            )
            return

        if topic.endswith("/set"):
            if data_type == "json":
                route = self.mqtt_split_items_dataclass(topic, "")
                if route and route.dev in ("do", "ro", "led"):
                    # Digital outputs never take the analog fade path (fixes K1/K7)
                    self.loop.call_soon_threadsafe(
                        asyncio.create_task, self.process_output_json(topic, payload_data)
                    )
                    return
                try:

                    transition_value = payload_data.get("transition", 0.0)
                    brightness_value = payload_data.get(
                        "brightness", payload_data.get("Brightness")
                    )

                    if brightness_value is not None:
                        # Schedule async converter for AO transition
                        self.loop.call_soon_threadsafe(
                            asyncio.create_task,
                            self.process_ao_transition(
                                topic, payload, transition_value, brightness_value
                            ),
                        )
                    else:
                        state_val = payload_data.get("state", "").upper()
                        if state_val == "OFF":
                            # OFF without brightness: fade to 0 using
                            # transition
                            self.loop.call_soon_threadsafe(
                                asyncio.create_task,
                                self.process_ao_transition(
                                    topic, payload, transition_value, 0
                                ),
                            )
                        elif state_val == "ON":
                            # ON without brightness: fade to 100% using
                            # transition
                            self.loop.call_soon_threadsafe(
                                asyncio.create_task,
                                self.process_ao_transition(
                                    topic, payload, transition_value, 1000
                                ),
                            )
                        else:
                            self.logger.error(
                                f"Missing 'brightness' or 'state' key in JSON payload for {topic}"
                            )
                except Exception as e:
                    self.logger.error(
                        f"Error processing JSON /set message: {e}", exc_info=True
                    )
            elif data_type == "onoff":  # Schedule async converter
                self.loop.call_soon_threadsafe(
                    asyncio.create_task,
                    self.process_mqtt_message(message.topic, payload_data),
                )
            elif data_type in ("int", "float"):
                self.logger.warning(
                    f"Unexpected numeric payload type '{data_type}' ({payload_data}) for /set topic {topic}. Expected 'onoff' or JSON."
                )
            else:
                self.logger.error(
                    f"Invalid string payload '{payload}' for /set topic {topic}. Expected 'ON', 'OFF', or JSON."
                )

    def mqtt_worker_thread(self) -> None:
        """
        Worker thread that processes messages from the WebSocket queue and publishes them to MQTT.
        """
        self.logger.info("MQTT worker thread started.")
        while not self.should_stop.is_set():
            try:
                topic, value = self.websocket_to_mqtt_queue.get(timeout=1)
                if (
                    self.shadow
                    and not self.config.shadow_discovery
                    and topic.startswith(f"{self.config.mqtt.discovery_prefix}/")
                    and topic.endswith("/config")
                ):
                    self.websocket_to_mqtt_queue.task_done()
                    continue  # shadow without shadow_discovery creates no Home Assistant entities
                self.logger.debug(
                    f"MQTT Worker: Dequeued message for topic '{topic}'. Publishing..."
                )
                should_retain = topic.endswith(
                    (
                        "/state",
                        "/output",
                        "/config",
                        "/status",
                        "/counter",
                        "/temp",
                        "/humidity",
                        "/vdd",
                        "/vad",
                        "/vis",
                    )
                )
                self.mqtt_client.publish(topic, value, qos=1, retain=should_retain)
                self.logger.debug(f"MQTT Worker: Published to '{topic}' -> {value}")
                self.websocket_to_mqtt_queue.task_done()
            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(f"Error in mqtt_worker_thread: {e}", exc_info=True)
                try:
                    self.websocket_to_mqtt_queue.task_done()
                except ValueError:
                    pass
        self.logger.info("MQTT worker thread finished.")

    def websocket_worker_thread(self) -> None:
        """
        Worker thread that processes messages from the MQTT queue and sends them to the WebSocket.
        """
        self.logger.info("WebSocket worker thread started.")
        while not self.should_stop.is_set():
            try:
                message = self.mqtt_to_websocket_queue.get(timeout=1)
                self.logger.debug(f"WebSocket Worker: Dequeued message: {message}")

                # Hold the SAME message and retry each second until the WS is
                # open, shutdown is requested, or we exceed the max hold. Do not
                # re-queue (that reorders commands, e.g. ON then OFF -> OFF then
                # ON) and do not pull new items while holding one.
                MAX_HOLD_SECONDS = 30
                held_for = 0
                while not self.should_stop.is_set():
                    ws = self.websocket_connection
                    if ws is not None and ws.state == State.OPEN:
                        self.logger.debug(
                            "WebSocket Worker: Scheduling message send via call_soon_threadsafe."
                        )
                        self.loop.call_soon_threadsafe(
                            asyncio.create_task, self.send_to_websocket(message)
                        )
                        break

                    if held_for >= MAX_HOLD_SECONDS:
                        state_str = str(ws.state) if ws else "None"
                        self.logger.error(
                            f"WebSocket Worker: WebSocket unavailable (State: {state_str}) "
                            f"for {MAX_HOLD_SECONDS}s. Dropping message: {message}"
                        )
                        break

                    state_str = str(ws.state) if ws else "None"
                    self.logger.warning(
                        f"WebSocket Worker: WebSocket unavailable (State: {state_str}). "
                        f"Holding message: {message}"
                    )
                    time.sleep(1)
                    held_for += 1

                self.mqtt_to_websocket_queue.task_done()

            except queue.Empty:
                continue
            except Exception as e:
                self.logger.error(
                    f"Error in websocket_worker_thread: {e}", exc_info=True
                )
                try:
                    self.mqtt_to_websocket_queue.task_done()
                except ValueError:
                    pass
        self.logger.info("WebSocket worker thread finished.")

    # ---- T15: pulse counters ------------------------------------------------------------------
    def _publish_counter(self, circuit: str, value: int, st: dict[str, Any], now: float, source: str) -> None:
        topic = self.generate_mqtt_topic_update("di", circuit, "counter")
        self._enqueue_ws_to_mqtt((topic, json.dumps({"value": value})))
        st.update(published=value, published_at=now, pending=None)
        self.events.emit(INPUT_CHANGED, dev="di", circuit=circuit, value=value, raw=value,
                         ts=time.time(), source=source, subkey="counter")

    def _counter_update(self, circuit: str, counter: Any, force: bool = False, source: str = "ws") -> None:
        """Publish evok's hardware counter, at most every counter_interval_s and only when it changed.
        A lower value than before (evok/device reset) is published as it is; HA copes with total_increasing."""
        cfg = self.circuits.get("di", circuit)
        if not cfg.counter:
            return
        try:
            value = int(counter)
        except (TypeError, ValueError):
            return
        st = self._counters.setdefault(circuit, {"published": None, "published_at": 0.0, "pending": None})
        now = time.monotonic()
        if value == st["published"] and not force:
            st["pending"] = None
        elif force or st["published"] is None or now - st["published_at"] >= cfg.counter_interval_s:
            self._publish_counter(circuit, value, st, now, source)
        else:
            st["pending"] = value  # too soon; the poller (or the next update) publishes it

    def _counter_flush(self) -> None:
        now = time.monotonic()
        for circuit, st in self._counters.items():
            interval = self.circuits.get("di", circuit).counter_interval_s
            if st["pending"] is not None and now - st["published_at"] >= interval:
                self._publish_counter(circuit, st["pending"], st, now, "ws")

    async def _counter_poller(self) -> None:
        """Safety net next to the WebSocket pushes: read all counters over REST every interval."""
        interval = min(c.counter_interval_s for c in self.config.circuits.values() if c.counter)
        while not self.should_stop.is_set():
            try:
                await asyncio.sleep(interval)
                data = await self.get_unipi_data(scope="all")
                for item in data or []:
                    if item.get("dev") == "di" and "counter" in item:
                        self._counter_update(str(item.get("circuit")), item["counter"], source="rest")
                self._counter_flush()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Counter poller error: {e}", exc_info=True)

    async def _ws_write_now(self, dev: str, circuit: str, value: int) -> None:
        """Write one `set` straight to the evok WebSocket (no queue, no hold). Raises if it cannot."""
        if self.shadow:
            self.logger.info(f"SHADOW: would write {dev}/{circuit} = {value}")
            return
        ws = self.websocket_connection
        if ws is None or ws.state != State.OPEN:
            raise WebSocketUnavailable("evok WebSocket is not open")
        try:
            await ws.send(json.dumps({"cmd": "set", "dev": dev, "circuit": circuit, "value": value}))
        except Exception as e:
            raise WebSocketUnavailable(f"WebSocket send failed: {e}") from e

    def _publish_attributes(self, dev: str, circuit: str, attrs: dict[str, Any]) -> None:
        topic = self.generate_mqtt_topic_update(dev, circuit, "attributes")
        self._enqueue_ws_to_mqtt((topic, json.dumps(attrs)))

    def _on_input_for_sequencer(self, dev, circuit, value, subkey=None, **_):
        if subkey is None and dev in ("do", "ro", "led"):
            self.sequencer.note_state(dev, circuit, value)

    def _on_output_track_state(self, dev, circuit, value, **_):
        # Remember what we last commanded (evok does not push every output, e.g. front-panel LEDs)
        if dev in ("do", "ro", "led"):
            self.device_states[f"{dev}_{circuit}"] = value

    def _on_output_for_sequencer(self, dev, circuit, value, origin=None, **_):
        # Plain ON/OFF commands (mqtt/rule) are known to us even if evok never pushes this output.
        if origin != "sequence" and dev in ("do", "ro", "led"):
            self.sequencer.note_state(dev, circuit, value)

    async def _sequencer_watchdog(self) -> None:
        while not self.should_stop.is_set():
            try:
                await asyncio.sleep(1)
                await self.sequencer.watchdog_tick()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Sequencer watchdog error: {e}", exc_info=True)

    async def _shutdown_outputs(self) -> None:
        await self.sequencer.cancel_all("shutdown")
        await self.sequencer.failsafe_all(self.circuits.failsafe_keys(), "fail-safe at shutdown")

    async def process_output_json(self, topic: str, payload_data: Any) -> None:
        """JSON command for a digital output: pulse / duration / preset / plain state."""
        parts = self.mqtt_split_items_dataclass(topic, "")
        if not parts or not parts.dev or not parts.circuit:
            self.logger.error(f"MQTT topic parsing failed for: {topic}")
            return
        dev, circuit = parts.dev, parts.circuit
        try:
            if not isinstance(payload_data, dict):
                raise SequenceRejected("expected a JSON object")
            spec = parse_command(
                payload_data, self.circuits.limits(dev, circuit), self.circuits.get(dev, circuit).presets
            )
            if spec is None:  # plain {"state": "ON"|"OFF"}
                await self.process_mqtt_message(topic, str(payload_data["state"]).upper())
                return
            await self.sequencer.start(dev, circuit, spec, origin="mqtt")
        except SequenceRejected as e:
            self.logger.warning(f"Rejected command for {dev}/{circuit}: {e} (payload: {payload_data!r})")
            self._publish_attributes(dev, circuit, {"last_error": str(e)})

    async def send_to_websocket(self, message: str) -> None:
        """
        Sends a message to the WebSocket server.

        Args:
            message: The message string to send.
        """
        if self.shadow:
            self.logger.info(f"SHADOW: would send {message}")
            return
        ws = self.websocket_connection
        if ws is not None and ws.state == State.OPEN:
            try:
                self.logger.debug(
                    f"Attempting to send message via WebSocket: {message}"
                )
                await ws.send(message)
                self.logger.debug("Successfully sent message via WebSocket.")
            except Exception as e:
                self.logger.error(
                    f"Error sending WebSocket message: {e}", exc_info=True
                )
        else:
            self.logger.error(
                f"WebSocket not available, cannot send message: {message}"
            )

    @log_function
    async def get_unipi_data(
        self,
        dev: str | None = None,
        circuit: str | None = None,
        scope: str | None = None,
    ) -> dict[str, Any] | None:
        """
        Fetches data from the Unipi device via HTTP (async).

        Args:
            dev: The device type (e.g., 'ai', 'do').
            circuit: The circuit identifier.
            scope: The scope of data ('all', 'circuit', 'value').

        Returns:
            dict[str, Any] | None: The JSON response as a dictionary, or None if failed.
        """
        self.logger.debug(
            f"get_unipi_data called with: dev={dev}, circuit={circuit}, scope={scope}"
        )
        unipi_url_str = str(self.config.unipi_http.url)
        unipi_req_url: str = ""

        if scope == "all":
            unipi_req_url = unipi_url_str
        elif dev and circuit and scope == "circuit":
            unipi_req_url = f"{unipi_url_str.replace('/all', '')}/{dev}/{circuit}"
        elif dev and circuit and scope == "value":
            unipi_req_url = (
                f"{unipi_url_str.replace('/all', '')}/{dev}/{circuit}/{scope}"
            )
        else:
            self.logger.warning(
                f"Invalid get_unipi_data parameters: dev={dev}, circuit={circuit}, scope={scope}. Returning None."
            )
            return None

        retries = 0
        max_retries = 3
        retry_delay = 2

        if self.session is None:
            self.logger.error("aiohttp session is not initialized.")
            return None

        while retries <= max_retries:
            self.logger.debug(
                f"Attempting UNIPI request (attempt {retries + 1}/{max_retries + 1}): {unipi_req_url}"
            )
            try:
                async with self.session.get(
                    unipi_req_url, timeout=aiohttp.ClientTimeout(total=10)
                ) as response:
                    if response.status != 200:
                        text = await response.text()
                        self.logger.error(
                            f"UNIPI request to {unipi_req_url} failed with status code {response.status}. Response text: {text}"
                        )
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                self.logger.error(
                    f"UNIPI request failed for {unipi_req_url} (attempt {retries + 1}): {e}"
                )
                retries += 1
                if retries <= max_retries:
                    await asyncio.sleep(retry_delay * retries)
                else:
                    self.logger.error("Max retries reached for UNIPI connection.")
                    return None
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode UNIPI JSON response: {e}")
                return None
            except Exception as e:
                self.logger.error(
                    f"Unexpected error in get_unipi_data: {e}", exc_info=True
                )
                return None
        return None

    @log_function
    async def perform_discovery_and_mqtt_subscribe(self) -> str | None:
        """
        Performs device discovery and subscribes to relevant MQTT topics.

        Returns:
            str | None: The discovered device name, or None if discovery failed.
        """
        unipi_data = None
        max_retries = 5
        retry_delay = 5

        for attempt in range(max_retries):
            self.logger.info(
                f"Attempting to fetch Unipi data (scope='all', attempt {attempt + 1}/{max_retries})..."
            )
            unipi_data = await self.get_unipi_data(scope="all")
            if unipi_data:
                break
            else:
                self.logger.warning(
                    f"Failed to fetch Unipi data. Retrying in {retry_delay} seconds..."
                )
                await asyncio.sleep(retry_delay)

        if not unipi_data:
            self.logger.critical(
                "Failed to fetch Unipi data after multiple retries. Cannot proceed with discovery."
            )
            return None

        # Find device info
        device_info = None
        for item in unipi_data:
            if item.get("dev") == "device_info":
                family = item.get("family")
                if family in ["Neuron", "Axon"]:
                    device_info = item
                    break

        if device_info is None:
            for item in unipi_data:
                if item.get("dev") == "device_info":
                    device_info = item
                    self.logger.warning(
                        "Could not find Neuron/Axon device_info. Using first available device_info."
                    )
                    break

        if device_info:
            device_model = device_info.get("model", "unknown")
            device_sn = device_info.get("sn", "unknown")
            device_family = device_info.get("family", "unknown")
            self.device_name = f"{device_family}_{device_model}_{device_sn}"
            if self.shadow:
                # Every topic, unique_id and discovery node id derives from this name, so the shadow can
                # never collide with (or overwrite the retained discovery of) the live instance.
                self.device_name += "_shadow"
            self.device_info = device_info
            self.logger.info(f"Determined device name: {self.device_name}")
            self._save_cached_device_name()
            await self._ensure_lwt_topic()
        else:
            self.logger.warning("No device_info found. Using 'unknown_device'.")
            self.device_name = "unknown_device"

        # Store discovered items for internal use (API)
        self.discovered_devices = unipi_data

        # Populate initial device states from REST data
        for item in unipi_data:
            dev = item.get("dev")
            circuit = item.get("circuit")

            if dev == "1wdevice" and circuit:
                sensors = ["temp", "humidity", "vdd", "vad", "vis"]
                for sensor_key in sensors:
                    if sensor_key in item:
                        self.device_states[f"{dev}_{circuit}_{sensor_key}"] = item[
                            sensor_key
                        ]
            else:
                value = item.get("value")
                if dev and circuit and value is not None:
                    self.device_states[f"{dev}_{circuit}"] = self.circuits.logical(dev, circuit, value)

        # Discovery
        if self.device_name and device_info:
            for item in unipi_data:
                if item.get("dev") != "device_info" and "dev" in item:
                    self.publish_discovery_config(item, device_info)

                    # Check for extensions (modbus_slave)
                    if item.get("dev") == "modbus_slave":
                        circuit = item.get("circuit")
                        if circuit:
                            self.publish_extension_discovery(circuit)
        else:
            self.logger.error(
                "Cannot proceed with discovery as device name or info is missing."
            )
            return None

        # Subscribe
        self.logger.info(
            f"Subscribing to {len(self.mqtt_subscribe_topics)} collected MQTT command topics..."
        )
        for topic in self.mqtt_subscribe_topics:
            self.mqtt_client.subscribe(topic, qos=1)

        return self.device_name

    @log_function
    def publish_extension_discovery(self, circuit: str) -> None:
        """
        Publishes Home Assistant MQTT discovery configuration for an extension module.
        """
        if not self.device_name:
            return

        discovery_prefix = self.config.mqtt.discovery_prefix
        root_topic = self.config.mqtt.topic
        device_topic = f"{root_topic}/{self.device_name}_bridge"

        # Extension Device Info (Group with Bridge Device)
        device_info = {
            "identifiers": [f"{self.device_name}_bridge"],
            "name": f"Unipi {self.device_name} Bridge",
            "manufacturer": "Unipi Technology s.r.o.",
            "model": "MQTT Bridge",
            "sw_version": SCRIPT_VERSION,
        }

        # 1. Problem Sensor (Binary)
        # Topic:
        # [discovery_prefix]/binary_sensor/[device_name]_ext_[circuit]_problem/config
        unique_id_problem = f"{self.device_name}_ext_{circuit}_problem"
        discovery_topic_problem = f"{discovery_prefix}/binary_sensor/{self.device_name}_ext_{circuit}_problem/config"

        config_problem = {
            "name": f"Unipi Bridge Extension {circuit} Status",
            "unique_id": unique_id_problem,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/extension/{circuit}/problem",
            "device_class": "problem",
            "payload_on": "1",
            "payload_off": "0",
            "availability_topic": f"{root_topic}/{self.device_name}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_problem, json.dumps(config_problem))
        )

        # 2. Last Comm Sensor (Sensor)
        # Topic:
        # [discovery_prefix]/sensor/[device_name]_bridge_connectivity/[circuit]_last_comm/config
        unique_id_comm = f"{self.device_name}_ext_{circuit}_last_comm"
        discovery_topic_comm = f"{discovery_prefix}/sensor/{self.device_name}_bridge_connectivity/ext_{circuit}_last_comm/config"

        config_comm = {
            "name": f"Unipi Bridge Extension {circuit} Last Comm",
            "unique_id": unique_id_comm,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/extension/{circuit}/last_comm",
            "unit_of_measurement": "s",
            "icon": "mdi:clock-outline",
            "availability_topic": f"{root_topic}/{self.device_name}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_comm, json.dumps(config_comm))
        )

    async def _mqtt_monitor(self) -> None:
        """
        Watches the MQTT connection. Reconnecting is left to paho's own loop
        thread (calling reconnect() from here would race with it). If the link
        stays down for MQTT_WATCHDOG_SECONDS anyway, exit with an error so
        systemd restarts the service with a fresh client.
        """
        self.logger.info("MQTT Monitor: Started.")
        disconnected_since: float | None = None

        while not self.should_stop.is_set():
            try:
                await asyncio.sleep(10)

                if self.mqtt_client.is_connected():
                    if disconnected_since is not None:
                        self.logger.warning(
                            "MQTT Monitor: Connection restored after "
                            f"{int(time.monotonic() - disconnected_since)} seconds."
                        )
                    disconnected_since = None
                    self.mqtt_connected = True
                    continue

                self.mqtt_connected = False
                now = time.monotonic()
                if disconnected_since is None:
                    disconnected_since = now
                down = int(now - disconnected_since)
                if down % 60 < 10:  # log about once a minute
                    self.logger.warning(
                        f"MQTT Monitor: Disconnected for {down} seconds; paho keeps retrying."
                    )

                if down > MQTT_WATCHDOG_SECONDS:
                    self.logger.critical(
                        f"MQTT Monitor: Disconnected for over {MQTT_WATCHDOG_SECONDS} "
                        "seconds. Exiting so systemd restarts the service."
                    )
                    self.exit_code = 1
                    self.should_stop.set()
                    self.loop.stop()
                    break

            except asyncio.CancelledError:
                self.logger.info("MQTT Monitor task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"MQTT Monitor unexpected error: {e}")
                await asyncio.sleep(5)

    def _schedule_republish(self, reason: str) -> None:
        """Starts republish_all() unless one is already pending (loop thread only)."""
        if self._republish_task and not self._republish_task.done():
            self.logger.info(f"Republish already pending, ignoring trigger: {reason}")
            return
        self._republish_task = self.loop.create_task(
            self.republish_all(reason), name="RepublishTask"
        )

    async def republish_all(self, reason: str) -> None:
        """
        Re-sends Home Assistant discovery configs, availability and the current
        state of every device. Needed after a broker restart (retained messages
        may be gone) or a Home Assistant restart (birth message).
        """
        try:
            # Let HA finish subscribing after its birth message and let the
            # reconnect settle (retained-/set guard window) before publishing.
            await asyncio.sleep(random.uniform(3, 6))
            if not self.mqtt_client.is_connected():
                self.logger.warning(f"Republish ({reason}) skipped: MQTT not connected.")
                return

            self.logger.warning(
                f"Republishing discovery, availability and states ({reason})..."
            )
            unipi_data = await self.get_unipi_data(scope="all")
            if not unipi_data or not self.device_info:
                self.logger.error(
                    f"Republish ({reason}) failed: could not fetch Unipi data."
                )
                return

            for item in unipi_data:
                if item.get("dev") != "device_info" and "dev" in item:
                    self.publish_discovery_config(item, self.device_info)
                    if item.get("dev") == "modbus_slave" and item.get("circuit"):
                        self.publish_extension_discovery(item["circuit"])
            # publish_discovery_config() appends the command topics again
            self.mqtt_subscribe_topics = list(dict.fromkeys(self.mqtt_subscribe_topics))
            self.publish_bridge_discovery()

            ws = self.websocket_connection
            if ws is not None and ws.state == State.OPEN:
                self.publish_availability("online")
                self.publish_error(False, "OK")
            else:
                self.publish_availability("offline")

            # Current states, straight from the REST snapshot
            self.process_websocket_message(
                [item for item in unipi_data if item.get("dev") in DEVICE_TYPE_MAPPING],
                force=True,
            )
            self.logger.warning(f"Republish ({reason}) complete.")
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.error(f"Republish ({reason}) failed: {e}", exc_info=True)

    async def _monitor_extensions(self) -> None:
        """
        Periodically checks the status of extension modules via REST API.
        """
        check_interval = self.config.extensions.check_interval
        max_delay = self.config.extensions.max_comm_delay

        self.logger.info(
            f"Extension Monitor: Started (Interval: {check_interval}s, Max Delay: {max_delay}s)"
        )

        while not self.should_stop.is_set():
            try:
                await asyncio.sleep(check_interval)

                # We need to know which extensions exist.
                # We can fetch rest/all or just iterate known ones if we stored them.
                # Fetching rest/all is safest to discover them dynamically or re-check.
                # However, rest/all is large.
                # Better: Fetch rest/modbus_slave to get list of slaves?
                # But rest/modbus_slave failed in testing (404).
                # So we must use rest/all or rely on initial discovery.
                # Let's use rest/all for now as it's reliable.

                # Reuse the shared session (created in _async_startup) instead of
                # opening a new one every cycle, and bound the request time.
                if self.session is None:
                    continue
                async with self.session.get(
                    str(self.config.unipi_http.url),
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Find all modbus_slave devices
                        slaves = [d for d in data if d.get("dev") == "modbus_slave"]

                        for slave in slaves:
                            circuit = slave.get("circuit")
                            last_comm = slave.get("last_comm", 0)

                            # Skip internal bus (circuit "1") if it exists and looks healthy/different?
                            # Usually circuit "1" is the main EMO-R8? No, main unit is usually SPI/I2C.
                            # In the user's log, circuit "1" had last_comm 0.01 (TCP slave?),
                            # and circuit "xS51" had last_comm 37712 (RTU slave).
                            # We should monitor ALL of them.

                            root_topic = self.config.mqtt.topic
                            device_topic = f"{root_topic}/{self.device_name}_bridge"

                            # Publish Last Comm
                            topic_comm = (
                                f"{device_topic}/extension/{circuit}/last_comm"
                            )
                            self.mqtt_client.publish(
                                topic_comm, str(last_comm), retain=True
                            )

                            # Determine Problem Status
                            is_problem = last_comm > max_delay
                            topic_problem = (
                                f"{device_topic}/extension/{circuit}/problem"
                            )
                            payload_problem = "1" if is_problem else "0"
                            self.mqtt_client.publish(
                                topic_problem, payload_problem, retain=True
                            )

                            if is_problem:
                                self.logger.warning(
                                    f"Extension Monitor: Extension {circuit} is DOWN! Last comm: {last_comm}s ago."
                                )

                        # Process 1wdevice updates since EVOK websocket doesn't always send them
                        onewire_devices = [d for d in data if d.get("dev") == "1wdevice"]
                        if onewire_devices:
                            self.process_websocket_message(onewire_devices)
                    else:
                        self.logger.error(
                            f"Extension Monitor: Failed to fetch status. Status: {response.status}"
                        )

            except asyncio.CancelledError:
                self.logger.info("Extension Monitor: Task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Extension Monitor: Error: {e}")
                await asyncio.sleep(10)  # Wait a bit before retrying on error

    @log_function
    def publish_bridge_discovery(self) -> None:
        """
        Publishes Home Assistant MQTT discovery configuration for the bridge's internal sensors.
        """
        if not self.device_name:
            self.logger.warning(
                "publish_bridge_discovery called but device_name is not set."
            )
            return

        self.logger.info(f"Publishing Bridge Discovery for device: {self.device_name}")
        discovery_prefix = self.config.mqtt.discovery_prefix
        root_topic = self.config.mqtt.topic
        device_topic = f"{root_topic}/{self.device_name}_bridge"

        # Bridge Device Info
        # Bridge Device Info (Separate from physical device to maintain
        # availability)

        # Determine pretty names
        if self.device_info:
            d_family = self.device_info.get("family", "Unipi")
            d_model = self.device_info.get("model", "Unknown")
            d_sn = self.device_info.get("sn", "0000")

            pretty_name = f"Unipi {d_family} {d_model} ({d_sn}) - Status{self._shadow_tag()}"
            # User requested model to be "Neuron S103" (family + model)
            pretty_model = f"{d_family} {d_model}"
        else:
            pretty_name = f"Unipi {self.device_name} - Status"
            pretty_model = "MQTT Bridge"

        device_info = {
            "identifiers": [f"{self.device_name}_bridge"],
            "name": pretty_name,
            "manufacturer": "Unipi Technology s.r.o.",
            "model": pretty_model,
            "sw_version": SCRIPT_VERSION,
        }

        # 1. Startup Error (Binary Sensor)
        unique_id_startup = f"{self.device_name}_bridge_startup_error"
        discovery_topic_startup = f"{discovery_prefix}/binary_sensor/{self.device_name}_bridge_startup_error/config"

        config_startup = {
            "name": "Unipi Bridge Startup Error",
            "unique_id": unique_id_startup,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/startup_error",
            "device_class": "problem",
            "payload_on": "1",
            "payload_off": "0",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_startup, json.dumps(config_startup))
        )

        # 2. Startup Details (Sensor)
        unique_id_startup_details = f"{self.device_name}_bridge_startup_details"
        # Use _bridge_connectivity node for details
        discovery_topic_startup_details = f"{discovery_prefix}/sensor/{self.device_name}_bridge_connectivity/startup_details/config"

        config_startup_details = {
            "name": "Unipi Bridge Startup Details",
            "unique_id": unique_id_startup_details,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/startup_details",
            "icon": "mdi:alert-circle-outline",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_startup_details, json.dumps(config_startup_details))
        )

        # 3. Connectivity Problem (Binary Sensor)
        unique_id_conn = f"{self.device_name}_bridge_connectivity_problem"
        discovery_topic_conn = f"{discovery_prefix}/binary_sensor/{self.device_name}_bridge_connectivity_problem/config"

        config_conn = {
            "name": "Unipi Bridge Connectivity",
            "unique_id": unique_id_conn,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/connectivity_problem",
            "device_class": "connectivity",
            "payload_on": "0",
            "payload_off": "1",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_conn, json.dumps(config_conn))
        )

        # 4. Connectivity Details (Sensor)
        unique_id_conn_details = f"{self.device_name}_bridge_connectivity_details"
        # Use _bridge_connectivity node for details
        discovery_topic_conn_details = f"{discovery_prefix}/sensor/{self.device_name}_bridge_connectivity/connectivity_details/config"

        config_conn_details = {
            "name": "Unipi Bridge Connectivity Details",
            "unique_id": unique_id_conn_details,
            "dev": device_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": f"{device_topic}/connectivity_details",
            "icon": "mdi:network-off-outline",
        }
        self.websocket_to_mqtt_queue.put_nowait(
            (discovery_topic_conn_details, json.dumps(config_conn_details))
        )

    def _enqueue_ws_to_mqtt(self, message: tuple[str, str]) -> None:
        """Non-blocking enqueue to the WS->MQTT queue (drops + logs if full)."""
        try:
            self.websocket_to_mqtt_queue.put_nowait(message)
        except queue.Full:
            self.logger.error(
                f"WS->MQTT queue full! Dropping message for topic: {message[0]}"
            )

    def _build_configuration_url(self) -> str | None:
        """Builds the HA configuration_url from the unipi_http host + web port."""
        try:
            parsed_url = urlparse(str(self.config.unipi_http.url))
            unipi_host = parsed_url.hostname or "localhost"
            web_port = self.config.web_server.port
            return f"http://{unipi_host}:{web_port}"
        except Exception:
            return None

    @log_function
    def publish_discovery_config(
        self, json_data: dict[str, Any], device_info: dict[str, Any]
    ) -> None:
        """
        Publishes Home Assistant MQTT discovery configuration for a device.

        Args:
            json_data: The device data from Unipi.
            device_info: The device information (model, sn, etc.).
        """
        if "dev" not in json_data or "circuit" not in json_data:
            return

        device_model = device_info.get("model", "unknown")
        device_sn = device_info.get("sn", "unknown")
        device_family = device_info.get("family", "unknown")

        dev_type = json_data["dev"]
        circuit = json_data["circuit"]
        device_type_mapped = DEVICE_TYPE_MAPPING.get(dev_type)

        if not device_type_mapped:
            return

        f"{dev_type}_{circuit}"
        unique_id = f"{self.device_name}_{dev_type}_{circuit}"
        active_mode = json_data.get("mode")

        # Build configuration_url from unipi_http host + web_server port
        configuration_url = self._build_configuration_url()

        dev_info = {
            "identifiers": [self.device_name],
            "name": f"Unipi {device_family} {device_model} ({device_sn}){self._shadow_tag()}",
            "manufacturer": "Unipi Technology s.r.o.",
            "model": f"{device_family} {device_model}",
            "sn": device_sn,
        }
        if configuration_url:
            dev_info["configuration_url"] = configuration_url

        config = {
            "name": self.circuits.name(dev_type, circuit, f"{dev_type} {circuit}") + self._shadow_tag(),
            "unique_id": unique_id,
            "dev": dev_info,
            "origin": {
                "name": "Unipi - HomeAssistant",
                "sw": SCRIPT_VERSION,
                "url": "https://github.com/matthijsberg/unipi-homeassistant",
            },
            "state_topic": self.generate_mqtt_topic_update(dev_type, circuit, "state"),
            "qos": 1,
            "availability_topic": f"{self.config.mqtt.topic}/{self.device_name}/status",
            "payload_available": "online",
            "payload_not_available": "offline",
        }

        # Type specific config
        match device_type_mapped:
            case "binary_sensor":
                # NO/NC inversion is applied to the VALUE (logical state), so the payloads are always plain.
                config.update(
                    {
                        "payload_on": "ON",
                        "payload_off": "OFF",
                        "icon": "mdi:electric-switch",
                    }
                )

            case "switch":
                cmd_topic = self.generate_mqtt_topic_update(dev_type, circuit, "set")
                config.update(
                    {
                        "command_topic": cmd_topic,
                        "payload_on": "ON",
                        "payload_off": "OFF",
                        "icon": "mdi:electric-switch",
                        "retain": True,
                    }
                )
                self.mqtt_subscribe_topics.append(cmd_topic)
            case "sensor":
                config["value_template"] = "{{ value_json.value }}"
                if dev_type == "ai":
                    modes = json_data.get("modes", {}).get(active_mode, {})
                    # min/max are not valid MQTT *sensor* options; recent HA
                    # versions reject unknown discovery keys. Only send an
                    # explicit unit when one is known.
                    config["icon"] = "mdi:lightning-bolt-circle"
                    unit = modes.get("unit")
                    if unit is not None:
                        config["unit_of_measurement"] = unit
                elif dev_type == "temp":
                    config.update(
                        {
                            "unit_of_measurement": "°C",
                            "device_class": "temperature",
                            "icon": "mdi:thermometer",
                        }
                    )
                elif dev_type == "1wdevice":
                    # 1wdevice has multiple sensors, we need to publish discovery for each
                    # This requires a different approach than the other devices
                    # We will handle this separately below and return early
                    self._publish_1wdevice_discovery(json_data, device_info)
                    return
            case "light":
                if dev_type == "led":
                    cmd_topic = self.generate_mqtt_topic_update(dev_type, circuit, "set")
                    config.update(
                        {
                            "command_topic": cmd_topic,
                            "payload_on": "ON",
                            "payload_off": "OFF",
                            "icon": "mdi:led-on",
                            "retain": True,
                        }
                    )
                    self.mqtt_subscribe_topics.append(cmd_topic)
                elif dev_type == "ao":
                    modes = json_data.get("modes", {}).get(active_mode, {})
                    if modes.get("unit") == "V":
                        ao_range_max = modes.get("range", [0, 10])[1]
                        scale = int(ao_range_max * 100) if ao_range_max > 0 else 1000
                        cmd_topic = self.generate_mqtt_topic_update(
                            dev_type, circuit, "set"
                        )
                        config.update(
                            {
                                "schema": "json",
                                "brightness_scale": scale,
                                "command_topic": cmd_topic,
                                "brightness": True,
                                "supported_color_modes": ["brightness"],
                                "color_mode": True,
                                "icon": "mdi:lightning-bolt-circle",
                                "retain": True,
                            }
                        )
                        self.mqtt_subscribe_topics.append(cmd_topic)

        if dev_type in ("do", "ro", "led") and self.circuits.get(dev_type, circuit).sequencer_enabled():
            config["json_attributes_topic"] = self.generate_mqtt_topic_update(dev_type, circuit, "attributes")

        if device_type_mapped in ("binary_sensor", "sensor", "switch"):
            device_class = self.circuits.device_class(dev_type, circuit, device_type_mapped)
            if device_class:
                config["device_class"] = device_class

        # Publish Config
        discovery_topic = self.generate_mqtt_topic_discovery(
            dev_type, circuit, "config"
        )
        self._enqueue_ws_to_mqtt((discovery_topic, json.dumps(config)))

        # Initial State
        initial_value = json_data.get("value")
        if initial_value is not None and device_type_mapped == "binary_sensor":
            initial_value = self.circuits.logical(dev_type, circuit, initial_value)
        if initial_value is not None:
            state_topic = config.get("state_topic")
            state_payload = None

            if device_type_mapped in ["binary_sensor", "switch", "led"]:
                state_payload = "ON" if initial_value == 1 else "OFF"
            elif device_type_mapped == "sensor":
                state_payload = json.dumps({"value": initial_value})
            elif dev_type == "ao":
                scale = config.get("brightness_scale", 1000)
                ao_range_max = (
                    json_data.get("modes", {})
                    .get(active_mode, {})
                    .get("range", [0, 10])[1]
                )
                if ao_range_max > 0:
                    bri_val = round((float(initial_value) / ao_range_max) * scale)
                else:
                    bri_val = 0

                state_val = "ON" if bri_val > 0 else "OFF"
                state_payload = json.dumps(
                    {
                        "state": state_val,
                        "brightness": bri_val,
                        "color_mode": "brightness",
                    }
                )

            if state_topic and state_payload is not None:
                self._enqueue_ws_to_mqtt((state_topic, state_payload))

        # T15: the hardware pulse counter of a digital input becomes its own total_increasing sensor
        if dev_type == "di" and self.circuits.get(dev_type, circuit).counter:
            ccfg = self.circuits.get(dev_type, circuit)
            cc = {k: config[k] for k in ("dev", "origin", "qos", "availability_topic",
                                         "payload_available", "payload_not_available")}
            cc.update({
                "name": f"{config['name']} counter",
                "unique_id": f"{unique_id}_counter",
                "state_topic": self.generate_mqtt_topic_update("di", circuit, "counter"),
                "value_template": "{{ value_json.value }}",
                "state_class": "total_increasing",
                "icon": "mdi:counter",
            })
            if ccfg.unit:
                cc["unit_of_measurement"] = ccfg.unit
            self._enqueue_ws_to_mqtt((
                self.generate_mqtt_topic_discovery("di", f"{circuit}_counter", "config", component="sensor"),
                json.dumps(cc),
            ))
            if "counter" in json_data:
                self._counter_update(circuit, json_data["counter"], force=True, source="republish")

    @log_function
    def _publish_1wdevice_discovery(
        self, json_data: dict[str, Any], device_info: dict[str, Any]
    ) -> None:
        """
        Publishes Home Assistant MQTT discovery configuration for a 1-wire device with multiple sensors.
        """
        circuit = json_data.get("circuit")
        if not circuit:
            return

        device_model = device_info.get("model", "unknown")
        device_sn = device_info.get("sn", "unknown")
        device_family = device_info.get("family", "unknown")

        # Build configuration_url from unipi_http host + web_server port
        configuration_url = self._build_configuration_url()

        dev_info = {
            "identifiers": [self.device_name],
            "name": f"Unipi {device_family} {device_model} ({device_sn}){self._shadow_tag()}",
            "manufacturer": "Unipi Technology s.r.o.",
            "model": f"{device_family} {device_model}",
            "sn": device_sn,
        }
        if configuration_url:
            dev_info["configuration_url"] = configuration_url

        # Define the sensors we want to extract from the 1wdevice payload
        sensors = {
            "temp": {
                "name": "Temperature",
                "unit": "°C",
                "class": "temperature",
                "icon": "mdi:thermometer",
            },
            "humidity": {
                "name": "Humidity",
                "unit": "%",
                "class": "humidity",
                "icon": "mdi:water-percent",
            },
            "vdd": {
                "name": "VDD",
                "unit": "V",
                "class": "voltage",
                "icon": "mdi:flash",
            },
            "vad": {
                "name": "VAD",
                "unit": "V",
                "class": "voltage",
                "icon": "mdi:flash",
            },
            "vis": {
                "name": "VIS",
                "unit": "V",
                "class": "voltage",
                "icon": "mdi:flash",
            },
        }

        for sensor_key, sensor_meta in sensors.items():
            if sensor_key in json_data:
                unique_id = f"{self.device_name}_1wdevice_{circuit}_{sensor_key}"
                state_topic = self.generate_mqtt_topic_update(
                    "1wdevice", circuit, sensor_key
                )

                config = {
                    "name": self.circuits.name("1wdevice", circuit, f"1-Wire {circuit} {sensor_meta['name']}", subkey=sensor_key) + self._shadow_tag(),
                    "unique_id": unique_id,
                    "dev": dev_info,
                    "origin": {
                        "name": "Unipi - HomeAssistant",
                        "sw": SCRIPT_VERSION,
                        "url": "https://github.com/matthijsberg/unipi-homeassistant",
                    },
                    "state_topic": state_topic,
                    "qos": 1,
                    "availability_topic": f"{self.config.mqtt.topic}/{self.device_name}/status",
                    "payload_available": "online",
                    "payload_not_available": "offline",
                    "value_template": "{{ value_json.value }}",
                    "unit_of_measurement": sensor_meta["unit"],
                    "device_class": sensor_meta["class"],
                    "icon": sensor_meta["icon"],
                }

                discovery_topic = self.generate_mqtt_topic_discovery(
                    "1wdevice",
                    circuit,
                    "config",
                    component="sensor",
                    sensor_key=sensor_key,
                )
                self._enqueue_ws_to_mqtt((discovery_topic, json.dumps(config)))

                # Initial State
                initial_value = json_data.get(sensor_key)
                if initial_value is not None:
                    state_payload = json.dumps({"value": initial_value})
                    self._enqueue_ws_to_mqtt((state_topic, state_payload))

    @log_function
    def generate_mqtt_topic_discovery(
        self,
        dev: str,
        circuit: str,
        topic_end: str,
        component: str | None = None,
        sensor_key: str | None = None,
    ) -> str:
        """
        Generates an MQTT discovery topic.

        Args:
            dev: Device type.
            circuit: Circuit identifier.
            topic_end: Suffix for the topic.
            component: Optional overriding Home Assistant component.
            sensor_key: Optional sensor key for multi-sensor devices like 1wdevice.

        Returns:
            str: The generated MQTT topic string.
        """
        device_name_part = self.device_name if self.device_name else "unknown_device"
        dev_type_mapped = (
            component if component else DEVICE_TYPE_MAPPING.get(dev, "device")
        )

        if dev == "temp":
            entity_id = f"1-wire_{circuit}_{dev}"
        elif dev == "1wdevice" and sensor_key:
            entity_id = f"1-wire_{circuit}_{sensor_key}"
        else:
            entity_id = f"{dev}_{circuit}"

        return f"{self.config.mqtt.discovery_prefix}/{dev_type_mapped}/{device_name_part}/{entity_id}/{topic_end}"

    @log_function
    def generate_mqtt_topic_update(self, dev: str, circuit: str, topic_end: str) -> str:
        """
        Generates an MQTT state/command topic.

        Args:
            dev: Device type.
            circuit: Circuit identifier.
            topic_end: Suffix for the topic.

        Returns:
            str: The generated MQTT topic string.
        """
        device_name_part = self.device_name if self.device_name else "unknown_device"
        root_topic = self.config.mqtt.topic
        if dev == "temp":
            return f"{root_topic}/{device_name_part}/1-wire/{circuit}/{dev}/{topic_end}"
        elif dev == "1wdevice":
            return f"{root_topic}/{device_name_part}/1-wire/{circuit}/{topic_end}"
        else:
            return f"{root_topic}/{device_name_part}/{dev}/{circuit}/{topic_end}"

    async def websocket_handler(self) -> None:
        """
        Manages the WebSocket connection, including reconnection and message reception.
        """
        reconnect_delay = 1
        max_reconnect_delay = 30
        initial_connection_done = False

        while not self.should_stop.is_set():
            self.websocket_connection = None
            try:
                self.logger.info(
                    f"Attempting to connect to WebSocket: {self.config.websocket.url}"
                )
                async with websockets.connect(
                    self.config.websocket.url,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=15,
                    close_timeout=10,
                ) as websocket:
                    self.websocket_connection = websocket
                    self.logger.info(
                        f"WebSocket connection established: {self.config.websocket.url}"
                    )
                    reconnect_delay = 1

                    # Fail-safe FIRST: after a crash an output may still be energised. Try at once; if evok
                    # is not ready yet the failure is remembered and retried after discovery (below).
                    await self.sequencer.failsafe_all(
                        self.circuits.failsafe_keys(), "fail-safe on WebSocket connect"
                    )

                    if not initial_connection_done:
                        self.logger.info(
                            "Initial WebSocket connection successful. Waiting 5 seconds for Unipi stabilization..."
                        )
                        try:
                            await asyncio.sleep(5)
                            self.logger.info(
                                "Delay complete. Signaling initial WebSocket readiness."
                            )
                            self.initial_websocket_ready.set()
                            initial_connection_done = True
                        except asyncio.CancelledError:
                            self.logger.info(
                                "Delay sleep cancelled during initial connection."
                            )
                            raise

                    self.logger.info(
                        "WebSocket handler: Waiting for initial discovery to complete..."
                    )
                    try:
                        await self.initial_discovery_complete.wait()
                        self.logger.info(
                            "WebSocket handler: Initial discovery complete signal received. Starting message processing."
                        )
                    except asyncio.CancelledError:
                        self.logger.info(
                            "WebSocket handler: Wait for initial discovery cancelled."
                        )
                        break

                    # WebSocket is ready and discovery is done. We are ONLINE.
                    self.publish_availability("online")

                    # Clear any previous bridge errors
                    self.publish_error(False, "OK")

                    # Outputs flagged failsafe_off must not stay on across a bridge/WS interruption
                    await self.sequencer.failsafe_all(
                        self.circuits.failsafe_keys(), "fail-safe after WebSocket (re)connect"
                    )

                    self.logger.debug("Starting WebSocket message handling loop...")
                    while not self.should_stop.is_set():
                        try:
                            message = await websocket.recv()
                            self.logger.debug(
                                f"Received WebSocket message (raw): {message}"
                            )
                            try:
                                json_message = json.loads(message)
                                self.logger.debug(
                                    f"Processing WebSocket JSON: {json_message}"
                                )
                                self.process_websocket_message(json_message)
                                self.logger.debug(
                                    "Finished processing WebSocket message."
                                )
                            except json.JSONDecodeError:
                                self.logger.error(
                                    f"Could not decode JSON from websocket message: {message}",
                                    exc_info=True,
                                )
                            except Exception as proc_err:
                                self.logger.exception(
                                    f"Error processing websocket message: {proc_err}"
                                )
                        except asyncio.CancelledError:
                            self.logger.info("WebSocket receive task cancelled.")
                            break
                        except websockets.exceptions.ConnectionClosedOK:
                            self.logger.info(
                                "WebSocket connection closed normally by peer."
                            )
                            break
                        except websockets.exceptions.ConnectionClosedError as e:
                            self.logger.warning(
                                f"WebSocket connection closed with error: {e}. Breaking inner loop."
                            )
                            break
                        except Exception as e:
                            self.logger.exception(
                                f"Unexpected error in WebSocket receive loop: {e}"
                            )
                            break
            except websockets.exceptions.InvalidURI as e:
                self.logger.critical(
                    f"WebSocket connection failed (Permanent Error): {e}. Exiting handler."
                )
                self.should_stop.set()
                break
            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.InvalidHandshake,
                OSError,
                ConnectionRefusedError,
                asyncio.TimeoutError,
            ) as e:
                self.logger.warning(
                    f"WebSocket connection/establishment failed: {e}. Retrying..."
                )

                # Mark as OFFLINE
                self.publish_availability("offline")

                # Report Error
                self.publish_error(True, f"WebSocket connection failed: {e}")

                if (
                    self.websocket_connection
                    and self.websocket_connection.state == State.CLOSED
                ):
                    self.websocket_connection = None
                self.logger.info(
                    f"Waiting {reconnect_delay} seconds before retrying WebSocket connection..."
                )
                try:
                    await asyncio.sleep(reconnect_delay)
                except asyncio.CancelledError:
                    self.logger.info(
                        "WebSocket handler sleep cancelled during reconnect delay."
                    )
                    self.should_stop.set()
                    break
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            except asyncio.CancelledError:
                self.logger.info("WebSocket handler task cancelled.")
                self.should_stop.set()
                break
            except Exception as handler_exception:
                # Never give up on the WebSocket: log, back off and retry.
                self.logger.exception(
                    f"Unexpected error in websocket_handler outer loop: {handler_exception}"
                )
                try:
                    await asyncio.sleep(reconnect_delay)
                except asyncio.CancelledError:
                    break
                reconnect_delay = min(reconnect_delay * 2, max_reconnect_delay)
            finally:
                self.logger.info("websocket_handler outer loop iteration finished.")
        self.logger.info("websocket_handler exiting.")

    @log_function
    def _publish_1wdevice_values(
        self, json_data: dict[str, Any], dev: str, circuit: str, force: bool = False
    ) -> list[tuple[str, str]]:
        """
        Publishes each present 1-wire sensor value (deadband 0.05, {"value": ...}
        payload) and returns the generated (topic, payload) messages.
        With force=True the deadband is skipped (used for republishing).
        """
        sensors = ["temp", "humidity", "vdd", "vad", "vis"]
        generated_mqtt_messages: list[tuple[str, str]] = []
        for sensor_key in sensors:
            if sensor_key in json_data:
                sensor_value = json_data[sensor_key]

                # Deadband filter for 1wdevice sensors
                sensor_device_key = f"{dev}_{circuit}_{sensor_key}"
                old_value = self.device_states.get(sensor_device_key)

                if old_value is not None and not force:
                    try:
                        # Only publish if value changed by more than 0.05
                        if abs(float(sensor_value) - float(old_value)) < 0.05:
                            continue
                    except (ValueError, TypeError):
                        pass

                self.device_states[sensor_device_key] = sensor_value
                self.events.emit(
                    INPUT_CHANGED,
                    dev=dev,
                    circuit=circuit,
                    value=sensor_value,
                    raw=sensor_value,
                    ts=time.time(),
                    source="republish" if force else "ws",
                    subkey=sensor_key,
                )

                topic = self.generate_mqtt_topic_update(dev, circuit, sensor_key)
                # The discovery config expects {"value": ...}
                payload = json.dumps({"value": sensor_value})
                self.logger.debug(
                    f"WS->MQTT (1wdevice {sensor_key}): Publishing raw JSON value to {topic}"
                )
                self.websocket_to_mqtt_queue.put_nowait((topic, payload))
                generated_mqtt_messages.append((topic, payload))
        return generated_mqtt_messages

    def process_websocket_message(self, json_data: Any, force: bool = False) -> None:
        """
        Converts incoming WebSocket JSON data into MQTT messages.

        Args:
            json_data: The JSON data received from the WebSocket.
            force: Republish mode: skip the change/deadband filters, local
                logic and recording, and just (re)publish the current state.
        """
        if isinstance(json_data, list):
            for item in json_data:
                self.process_websocket_message(item, force=force)
        else:
            # For 1wdevice, the value might not be in the "value" key, but in specific sensor keys
            dev = json_data.get("dev", "unknown")
            circuit = json_data.get("circuit", "unknown")

            if dev == "di" and "counter" in json_data:
                self._counter_update(circuit, json_data["counter"], force=force,
                                     source="republish" if force else "ws")

            if dev == "1wdevice":
                generated_mqtt_messages = self._publish_1wdevice_values(
                    json_data, dev, circuit, force=force
                )

                if self.recorder and generated_mqtt_messages and not force:
                    self.recorder.record_ws_to_mqtt(json_data, generated_mqtt_messages)

            elif "value" in json_data:
                phys_value = json_data["value"]
                # NO/NC: from here on a digital input carries its LOGICAL value (state, rules, events)
                raw_value = self.circuits.logical(dev, circuit, phys_value)
                logic_data = json_data if raw_value is phys_value else {**json_data, "value": raw_value}

                device_key = f"{dev}_{circuit}"
                if dev in ("temp", "humidity", "vdd", "vad", "vis") and f"1wdevice_{circuit}_{dev}" in self.device_states:
                    device_key = f"1wdevice_{circuit}_{dev}"

                old_value = self.device_states.get(device_key)

                # Deadband filter: for analog values, only publish if the
                # change exceeds a threshold
                if old_value is not None and not force:
                    if dev in ("ai", "ao", "temp", "humidity", "vdd", "vad", "vis"):
                        try:
                            # Only publish if value changed by more than 0.05
                            # (0.5% of 10V range)
                            if abs(float(raw_value) - float(old_value)) < 0.05:
                                return
                        except (ValueError, TypeError):
                            pass  # If we can't compare, publish anyway
                    else:
                        # For digital devices, exact match filter
                        if old_value == raw_value:
                            return

                self.logger.debug(
                    f"Processing WebSocket update for {dev}/{circuit}: Value={raw_value} (prev={old_value})"
                )

                # Update State
                self.device_states[device_key] = raw_value
                self.events.emit(
                    INPUT_CHANGED,
                    dev=dev,
                    circuit=circuit,
                    value=raw_value,
                    raw=phys_value,
                    ts=time.time(),
                    source="republish" if force else "ws",
                    subkey=None,
                )

                # --- Local Logic Engine ---
                # Not on republish: nothing changed, so rules must not fire.
                if not force:
                    local_actions = self.local_logic.evaluate(logic_data, self.device_states)
                    for action in local_actions:
                        self.execute_local_action(action)

                # --- Recording Hook ---
                generated_mqtt_messages = []

                match dev:
                    case "temp" | "humidity" | "vdd" | "vad" | "vis":
                        if f"1wdevice_{circuit}_{dev}" in self.device_states:
                            topic = self.generate_mqtt_topic_update("1wdevice", circuit, dev)
                        else:
                            topic = self.generate_mqtt_topic_update(dev, circuit, "state")
                        
                        payload = json.dumps({"value": raw_value})
                        self.logger.debug(
                            f"WS->MQTT ({dev}): Publishing {payload} to {topic}"
                        )
                        self.websocket_to_mqtt_queue.put_nowait((topic, payload))
                        generated_mqtt_messages.append((topic, payload))

                    case "ai":
                        topic = self.generate_mqtt_topic_update(dev, circuit, "state")
                        payload = json.dumps({"value": raw_value})
                        self.logger.debug(
                            f"WS->MQTT (ai): Publishing raw JSON value to {topic}"
                        )
                        self.websocket_to_mqtt_queue.put_nowait((topic, payload))
                        generated_mqtt_messages.append((topic, payload))

                    case "ao":
                        state_topic = self.generate_mqtt_topic_update(dev, circuit, "state")
                        try:
                            # Unipi sends value in Volts (0-10). We map 0-10V to
                            # 0-1000 brightness.
                            brightness_value = int(float(raw_value) * 100)
                            state_value = "ON" if brightness_value > 0 else "OFF"

                            circuit_key = (dev, circuit)
                            if circuit_key in self.active_ao_transitions:
                                # Suppress WS->MQTT echoing during an active
                                # transition to avoid flooding
                                self.logger.debug(
                                    f"WS->MQTT (AO): Suppressing echo for {circuit} due to active transition."
                                )
                            else:
                                state_payload = json.dumps(
                                    {
                                        "state": state_value,
                                        "brightness": brightness_value,
                                        "color_mode": "brightness",
                                    }
                                )
                                self.logger.debug(
                                    f"WS->MQTT (AO): Publishing JSON state to {state_topic}"
                                )
                                self.websocket_to_mqtt_queue.put_nowait(
                                    (state_topic, state_payload)
                                )
                                generated_mqtt_messages.append((state_topic, state_payload))
                        except (ValueError, TypeError) as e:
                            self.logger.error(
                                f"WS->MQTT (AO) Error: Could not process value '{raw_value}' for {dev}/{circuit}: {e}"
                            )

                    case "1wdevice":
                        # 1wdevice has multiple sensors in the payload; publish
                        # each one (shared helper keeps both paths identical).
                        generated_mqtt_messages.extend(
                            self._publish_1wdevice_values(
                                json_data, dev, circuit, force=force
                            )
                        )

                    case "do" | "ro" | "led" if self.sequencer.is_running(dev, circuit):
                        # The sequencer owns the HA state while it runs (ON until it ends)
                        self.logger.debug(f"WS->MQTT ({dev}): suppressing echo for {circuit} during a sequence")

                    case "di" | "do" | "ro" | "led":
                        state_topic = self.generate_mqtt_topic_update(dev, circuit, "state")
                        try:
                            state_value = "ON" if int(float(raw_value)) == 1 else "OFF"
                            self.logger.debug(
                                f"WS->MQTT ({dev}): Publishing state {state_value} to {state_topic}"
                            )
                            self.websocket_to_mqtt_queue.put_nowait(
                                (state_topic, state_value)
                            )
                            generated_mqtt_messages.append((state_topic, state_value))
                        except (ValueError, TypeError) as e:
                            self.logger.error(
                                f"WS->MQTT ({dev}) Error: Could not process value '{raw_value}' for {dev}/{circuit}: {e}"
                            )
                    case _:
                        state_topic = self.generate_mqtt_topic_update(dev, circuit, "state")
                        self.logger.debug(
                            f"WS->MQTT ({dev}): Publishing raw JSON value to {state_topic}"
                        )
                        value_json = json.dumps({"value": raw_value})
                        self.websocket_to_mqtt_queue.put_nowait((state_topic, value_json))
                        generated_mqtt_messages.append((state_topic, value_json))

                if self.recorder and not force:
                    self.recorder.record_ws_to_mqtt(json_data, generated_mqtt_messages)

    @log_function
    async def process_mqtt_message(self, mqtt_topic: str, message_payload: str) -> None:
        """
        Converts an MQTT message to a WebSocket command (async).

        Args:
            mqtt_topic: The MQTT topic.
            message_payload: The message payload.
        """
        self.logger.debug(
            f"mqtt_to_websocket_converter called - Topic: {mqtt_topic}, Payload: {message_payload}"
        )

        parts = self.mqtt_split_items_dataclass(mqtt_topic, message_payload)
        if not parts or not parts.dev or not parts.circuit:
            self.logger.error(f"MQTT topic parsing failed for: {mqtt_topic}")
            return

        ws_state_value: int
        expected_read_value: int
        payload_upper = str(message_payload).upper()

        if payload_upper == "ON":
            ws_state_value = 1
            expected_read_value = 1
        elif payload_upper == "OFF":
            ws_state_value = 0
            expected_read_value = 0
        else:
            self.logger.error(
                f"Invalid payload '{message_payload}' for {parts.dev}/{parts.circuit}"
            )
            return

        if self.sequencer.is_running(parts.dev, parts.circuit):
            await self.sequencer.cancel(parts.dev, parts.circuit, f"plain {payload_upper} command")

        if parts.dev == "ao":
            circuit_key = (parts.dev, parts.circuit)
            if payload_upper == "OFF":
                self.logger.warning(
                    f"AO {circuit_key}: Received OFF command. Interrupting active transition."
                )
                self.active_ao_transitions.pop(circuit_key, None)
            elif payload_upper == "ON":
                self.logger.info(
                    f"AO {circuit_key}: Received ON command but no brightness specified. Defaulting to 100%."
                )
                self.loop.create_task(
                    self.process_ao_transition(mqtt_topic, "100", 0.0, 1000)
                )
                return

        ws_msg_dict = {
            "cmd": "set",
            "dev": parts.dev,
            "circuit": parts.circuit,
            "value": ws_state_value,
        }

        try:
            if self.commands.send_ws(parts.dev, parts.circuit, ws_state_value) is None:
                return  # queue full; send_ws already logged the dropped command

            # Verification with Retry
            verified = False
            for attempt in range(3):
                wait_time = 0.3 if attempt == 0 else 0.5
                await asyncio.sleep(wait_time)

                read_back_data = await self.get_unipi_data(
                    dev=parts.dev, circuit=parts.circuit, scope="value"
                )
                if read_back_data and "value" in read_back_data:
                    try:
                        actual = int(float(read_back_data["value"]))
                        if actual == expected_read_value:
                            verified = True
                            self.logger.info(
                                f"Verification successful for {parts.dev}/{parts.circuit}"
                            )
                            break
                        else:
                            if attempt < 2:
                                self.logger.debug(
                                    f"Verification mismatch for {parts.dev}/{parts.circuit} (Attempt {attempt + 1}). Retrying..."
                                )
                            else:
                                self.logger.warning(
                                    f"Verification mismatch for {parts.dev}/{parts.circuit}. Expected: {expected_read_value}, Actual: {actual}"
                                )
                    except Exception as e:
                        if attempt == 2:
                            self.logger.warning(
                                f"Verification error for {parts.dev}/{parts.circuit}: {e}"
                            )
                else:
                    if attempt == 2:
                        self.logger.warning(
                            f"Verification failed: Could not read data for {parts.dev}/{parts.circuit}"
                        )

            if not verified:
                self.logger.warning(
                    f"Acknowledging MQTT for {parts.dev}/{parts.circuit} despite verification failure."
                )

            ack_messages = self.mqtt_ack(message_payload, parts.dev, parts.circuit)

            if self.recorder:
                self.recorder.record_mqtt_to_ws(
                    mqtt_topic, message_payload, ws_msg_dict, ack_messages
                )

        except queue.Full:
            self.logger.error(f"MQTT->WS Queue Full! Dropping message: {mqtt_topic}")
        except Exception as e:
            self.logger.error(
                f"Error in mqtt_to_websocket_converter: {e}", exc_info=True
            )

    @log_function
    async def process_ao_transition(
        self,
        mqtt_topic: str,
        mqtt_payload: str,
        transition_value: Any,
        brightness_value: Any,
    ) -> None:
        """
        Handles Analog Output (AO) transitions with a specified duration.

        Args:
            mqtt_topic: The MQTT topic.
            mqtt_payload: The message payload.
            transition_value: The transition duration in seconds.
            brightness_value: The target brightness value.
        """
        self.logger.debug(
            f"AO Transition Start - Topic: {mqtt_topic}, Brightness: {brightness_value}, Transition: {transition_value}s"
        )

        parts = self.mqtt_split_items_dataclass(mqtt_topic, mqtt_payload)
        if not parts or not parts.dev or not parts.circuit:
            return

        if parts.dev not in ("ao", "analogoutput"):
            self.logger.error(
                f"AO transition requested for non-AO device {parts.dev}/{parts.circuit}. Ignoring."
            )
            return

        dev = parts.dev
        circuit = parts.circuit
        circuit_key = (dev, circuit)

        try:
            desired_value = int(float(brightness_value))
            transition_time = float(transition_value)
            if not 0 <= desired_value <= 1000:
                raise ValueError("Brightness out of range")
            transition_time = max(0, min(transition_time, 60))
        except Exception as e:
            self.logger.error(f"Invalid input for AO transition: {e}")
            return

        this_instance_desired_value = desired_value

        current_value_dict = await self.get_unipi_data(
            dev=dev, circuit=circuit, scope="value"
        )
        if not current_value_dict or "value" not in current_value_dict:
            self.logger.warning("Could not read current value. Optimistic ACK.")
            self.mqtt_ack(str(desired_value), dev, circuit, origin="fade")
            return

        try:
            current_value = int(float(current_value_dict["value"]) * 100)
        except Exception:
            self.mqtt_ack(str(desired_value), dev, circuit, origin="fade")
            return

        if desired_value == current_value:
            self.logger.info("Already at target value.")
            self.mqtt_ack(str(desired_value), dev, circuit, origin="fade")
            return

        # Start Async Task
        self.active_ao_transitions[circuit_key] = desired_value
        self.loop.create_task(
            self._perform_ao_transition_task(
                dev,
                circuit,
                desired_value,
                current_value,
                transition_time,
                circuit_key,
                this_instance_desired_value,
            ),
            name=f"AOTransition-{dev}-{circuit}",
        )

    async def _perform_ao_transition_task(
        self,
        dev: str,
        circuit: str,
        desired_value: int,
        current_value: int,
        transition_time: float,
        circuit_key: tuple[str, str],
        this_instance_desired_value: int,
    ) -> None:
        """
        Async task to perform smooth AO transitions.

        Args:
            dev: Device type.
            circuit: Circuit identifier.
            desired_value: Target value (0-1000).
            current_value: Starting value (0-1000).
            transition_time: Duration in seconds.
            circuit_key: Tuple identifying the circuit.
            this_instance_desired_value: Unique ID for this transition to handle interruptions.
        """
        try:
            value_diff = desired_value - current_value
            steps = int(abs(value_diff))
            step_time = transition_time / steps if steps > 0 else 0

            MINIMUM_STEP_TIME = 0.1
            if steps > 0 and step_time < MINIMUM_STEP_TIME:
                step_time = MINIMUM_STEP_TIME
                steps = max(1, int(round(transition_time / step_time)))

            step_size = value_diff / steps if steps > 0 else 0
            current_step_value = float(current_value)
            start_time = time.monotonic()

            for step_num in range(steps):
                if (
                    self.active_ao_transitions.get(circuit_key)
                    != this_instance_desired_value
                ):
                    self.logger.warning(f"AO Task ({circuit_key}): Interrupted.")
                    break
                if self.should_stop.is_set():
                    break

                loop_start_time = time.monotonic()
                current_step_value += step_size
                final_step_value = min(max(current_step_value, 0), 1000)
                if step_size > 0:
                    final_step_value = min(final_step_value, desired_value)
                else:
                    final_step_value = max(final_step_value, desired_value)

                ws_value = round(final_step_value / 100, 3)
                self.commands.send_ws(dev, circuit, ws_value, block=True)

                loop_elapsed_time = time.monotonic() - loop_start_time
                total_elapsed_time = time.monotonic() - start_time
                remaining_transition_time = max(0, transition_time - total_elapsed_time)
                sleep_duration = max(
                    0, min(step_time - loop_elapsed_time, remaining_transition_time)
                )
                await asyncio.sleep(sleep_duration)
            else:
                self.logger.info(f"AO Task ({circuit_key}): Loop completed.")

            self.mqtt_ack(str(desired_value), dev, circuit, origin="fade")

        except Exception as e:
            self.logger.exception(f"Error in AO transition task: {e}")
        finally:
            if (
                self.active_ao_transitions.get(circuit_key)
                == this_instance_desired_value
            ):
                self.active_ao_transitions.pop(circuit_key, None)

    @log_function
    def mqtt_ack(
        self, final_value_payload: str, dev: str, circuit: str, origin: str = "mqtt"
    ) -> list[tuple[str, str]]:
        """
        Sends an MQTT acknowledgement (state update) for a command.

        Args:
            final_value_payload: The value to report.
            dev: Device type.
            circuit: Circuit identifier.

        Returns:
            list[tuple[str, str]]: The generated MQTT messages.
        """
        generated_messages = []
        ack_value: Any = None
        state_topic = self.generate_mqtt_topic_update(dev, circuit, "state")

        try:
            match dev:
                case "ao":
                    try:
                        payload_upper = str(final_value_payload).upper()
                        if payload_upper == "OFF":
                            brightness_value = 0
                        else:
                            brightness_value = int(float(final_value_payload))

                        brightness_value = max(0, min(brightness_value, 1000))
                        state_value = "ON" if brightness_value > 0 else "OFF"

                        state_payload = json.dumps(
                            {
                                "state": state_value,
                                "brightness": brightness_value,
                                "color_mode": "brightness",
                            }
                        )

                        self.logger.info(
                            f"MQTT_ack (AO): Publishing JSON state {state_payload} to {state_topic}"
                        )
                        self.websocket_to_mqtt_queue.put_nowait(
                            (state_topic, state_payload)
                        )
                        generated_messages.append((state_topic, state_payload))
                        ack_value = brightness_value
                    except ValueError:
                        self.logger.error(
                            f"MQTT_ack (AO): Invalid value '{final_value_payload}'"
                        )

                case "di" | "do" | "ro" | "led" | "relay":
                    try:
                        payload_upper = str(final_value_payload).upper()
                        state_value = "OFF"
                        if payload_upper == "ON":
                            state_value = "ON"
                        elif payload_upper == "OFF":
                            state_value = "OFF"
                        else:
                            # Try parsing as number
                            if int(float(final_value_payload)) == 1:
                                state_value = "ON"

                        self.logger.info(
                            f"MQTT_ack ({dev}): Publishing state {state_value} to {state_topic}"
                        )
                        self.websocket_to_mqtt_queue.put_nowait((state_topic, state_value))
                        generated_messages.append((state_topic, state_value))
                        ack_value = 1 if state_value == "ON" else 0
                    except ValueError:
                        self.logger.error(
                            f"MQTT_ack ({dev}): Invalid value '{final_value_payload}'"
                        )

                case _:
                    self.logger.info(
                        f"MQTT_ack ({dev}): Publishing raw value {final_value_payload} to {state_topic}"
                    )
                    self.websocket_to_mqtt_queue.put_nowait(
                        (state_topic, str(final_value_payload))
                    )
                    generated_messages.append((state_topic, str(final_value_payload)))
                    ack_value = final_value_payload

            if ack_value is not None:
                self.events.emit(
                    OUTPUT_CHANGED,
                    dev=dev,
                    circuit=circuit,
                    value=ack_value,
                    ts=time.time(),
                    origin=origin,
                )

        except queue.Full:
            self.logger.error(f"MQTT_ack Queue Full! Dropping ACK for {dev}/{circuit}")

        return generated_messages

    @log_function
    def mqtt_split_items_dataclass(
        self, mqtt_topic: str, message_payload: str
    ) -> MqttParts:
        """
        Parses an MQTT topic and payload into a structured MqttParts object.

        Args:
            mqtt_topic: The MQTT topic string.
            message_payload: The message payload.

        Returns:
            MqttParts: A dataclass containing parsed device, circuit, payload, and command.
        """
        try:
            topic_parts = mqtt_topic.split("/")
            if len(topic_parts) < 5:
                return MqttParts()

            try:
                device_name_index = (
                    topic_parts.index(self.device_name)
                    if self.device_name in topic_parts
                    else -1
                )
                if device_name_index == -1:
                    # Fallback if device_name not found (e.g. unknown_device)
                    device_name_index = 1  # unipi/DEVICE_NAME/...

                dev = topic_parts[device_name_index + 1]
                circuit = topic_parts[device_name_index + 2]
                cmd = topic_parts[-1]
            except (ValueError, IndexError):
                return MqttParts()

            processed_payload, _ = self.process_payload(message_payload)
            return MqttParts(dev, circuit, processed_payload, cmd)
        except Exception as e:
            self.logger.debug(f"Error splitting MQTT topic '{mqtt_topic}': {e}")
            return MqttParts()

    @log_function
    @web.middleware
    async def auth_middleware(self, request, handler):
        """
        Middleware to protect sensitive routes.
        """
        # Define protected routes or unprotected logic
        # Unprotected: /, /static, /api/login, /api/status, /api/auth_check
        # Protected: Everything else (POST/PUT/DELETE, /api/rules, /api/inputs)

        path = request.path

        # Periodically sweep expired sessions so self.sessions can't grow
        # unbounded (at most once per 60s). (P1.4)
        now = time.time()
        if now - self._last_session_cleanup > 60:
            expired = [
                sid for sid, s in self.sessions.items() if s["expiry"] < now
            ]
            for sid in expired:
                del self.sessions[sid]
            self._last_session_cleanup = now

        # Always allow Login, Static, Auth Check (needed to load UI and check status)
        # NOTE: We allow "/" (Index) so the HTML can load, but the API calls it makes will be protected.
        # This allows the frontend JS to handle the "not logged in" state
        # gracefully (showing modal).
        if path in [
            "/",
            "/api/login",
            "/api/auth_check",
            "/api/info",
        ] or path.startswith("/static"):
            return await handler(request)

        # Allow reading status without auth? (Dashboard logic)
        # If user wants secure, we should probably protect status too, but let's keep it open for read-only dashboards for now?
        # The prompt implies authorized access to rule editor and input config.
        # Let's verify session.

        session_id = request.cookies.get("unipi_session")
        if not session_id or session_id not in self.sessions:
            return web.json_response({"error": "Unauthorized"}, status=401)

        # Check expiry
        session = self.sessions[session_id]
        if time.time() > session["expiry"]:
            del self.sessions[session_id]
            return web.json_response({"error": "Session expired"}, status=401)

        # Extend session on activity? optional.
        return await handler(request)

    async def web_handler_login(self, request: web.Request) -> web.Response:
        # Per-IP rate limiting (P1.2): bound the attempt rate regardless of the
        # back-off sleep, which parallel attempts can otherwise sidestep.
        peer_ip = request.remote or "unknown"
        now = time.time()
        attempts = [t for t in self.failed_logins.get(peer_ip, []) if now - t < 300]
        self.failed_logins[peer_ip] = attempts
        if len(attempts) >= 5:
            self.logger.warning(f"Login rate limit exceeded for {peer_ip}")
            return web.json_response(
                {"error": "Too many attempts. Try again later."}, status=429
            )

        data = await request.json()
        username = data.get("username")
        password = data.get("password")

        if not username or not password:
            return web.json_response({"error": "Missing credentials"}, status=400)

        # Restrict which users may log in (P1.1). Respond identically to a bad
        # password so we don't reveal that the account is disallowed.
        allowed_users = self.config.web_server.allowed_users
        if allowed_users and username not in allowed_users:
            self.logger.warning(
                f"Login attempt for disallowed user '{username}' from {peer_ip}"
            )
            await asyncio.sleep(random.uniform(2.0, 3.0))
            self.failed_logins[peer_ip].append(now)
            return web.json_response({"error": "Invalid credentials"}, status=401)

        # Authenticate with PAM
        try:
            auth_success = False
            if pam:
                p = pam.pam()
                # Run PAM authentication in thread pool to avoid blocking async
                # loop
                auth_success = await self.loop.run_in_executor(
                    None, p.authenticate, username, password
                )
            else:
                # Should fail if PAM is missing
                return web.json_response(
                    {"error": "Generic Auth Error (PAM missing)"}, status=500
                )

            if auth_success:
                # Successful login clears this IP's failed-attempt history.
                self.failed_logins.pop(peer_ip, None)
                # Create Session
                session_id = secrets.token_urlsafe(32)
                self.sessions[session_id] = {
                    "username": username,
                    "expiry": time.time() + 3600,  # 1 hour
                }

                resp = web.json_response({"status": "ok", "username": username})
                resp.set_cookie(
                    "unipi_session",
                    session_id,
                    max_age=3600,
                    httponly=True,
                    samesite="Strict",
                )
                self.logger.info(f"User '{username}' logged in successfully.")
                return resp
            else:
                self.logger.warning(f"Failed login attempt for user '{username}'")
                # Back-off delay
                await asyncio.sleep(random.uniform(2.0, 3.0))
                self.failed_logins.setdefault(peer_ip, []).append(now)
                return web.json_response({"error": "Invalid credentials"}, status=401)
        except Exception as e:
            self.logger.error(f"Login error: {e}")
            return web.json_response({"error": "Login error"}, status=500)

    async def web_handler_logout(self, request: web.Request) -> web.Response:
        session_id = request.cookies.get("unipi_session")
        if session_id and session_id in self.sessions:
            del self.sessions[session_id]

        resp = web.json_response({"status": "logged_out"})
        resp.del_cookie("unipi_session")
        return resp

    async def web_handler_auth_check(self, request: web.Request) -> web.Response:
        session_id = request.cookies.get("unipi_session")
        if session_id and session_id in self.sessions:
            session = self.sessions[session_id]
            if time.time() <= session["expiry"]:
                return web.json_response(
                    {"authenticated": True, "username": session["username"]}
                )

        return web.json_response({"authenticated": False})

    def process_payload(self, payload: str) -> tuple[Any, str]:
        """
        Processes the raw MQTT payload into a usable value and its type.

        Args:
            payload: The raw payload string.

        Returns:
            tuple[Any, str]: A tuple containing the processed data and its type description
                             ('json', 'onoff', 'int', 'float', 'string').
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

    def on_mqtt_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: dict[str, Any],
        reason_code: int,
        properties: Any = None,
    ) -> None:
        """
        Callback for MQTT disconnection.

        Args:
            client: The MQTT client instance.
            userdata: User data (unused).
            flags: Response flags.
            reason_code: The disconnection reason code.
            properties: MQTT v5 properties.
        """
        self.mqtt_connected = False
        reason_str = (
            mqtt.error_string(reason_code)
            if hasattr(mqtt, "error_string")
            else f"Code {reason_code}"
        )
        self.logger.warning(f"MQTT disconnected. Reason: {reason_str}")

        if reason_code != 0 and not self.should_stop.is_set():
            self.logger.warning(
                "Automatic reconnection will be attempted by the paho-mqtt loop."
            )
            # Intentional: treat the upcoming reconnect like a first connect so
            # retained command messages are skipped for ~2s after reconnecting.
            # This prevents the broker from replaying retained /set commands and
            # re-triggering physical outputs on every reconnect.
            self.is_first_connect = True

    #################################################
    #            --- Main Thread ---                #
    #################################################

    # -------------------------------------------------------------------------
    # Local Logic & Web Server Methods
    # -------------------------------------------------------------------------

    # ---- T17 helpers -----------------------------------------------------------------------------
    def _rec(self, action: dict[str, Any], step: str, ok: bool, detail: str) -> None:
        self.local_logic.record(action.get("rule_id"), action.get("rule_name"), step, ok, detail)

    def _ha_reachable(self) -> bool:
        return self.ha_online is True and self.mqtt_client.is_connected()

    def _toggled_value(self, dev: str, circuit: str) -> int:
        try:
            on = int(float(self.device_states.get(f"{dev}_{circuit}", 0))) == 1
        except (TypeError, ValueError):
            on = False
        return 0 if on else 1

    def _rule_spec(self, dev: str, circuit: str, pulse: Any, preset: Any):
        """Resolve a rule's pulse/preset into a validated sequence spec (raises SequenceRejected)."""
        payload = {"preset": preset} if preset else {"pulse": pulse}
        return parse_command(payload, self.circuits.limits(dev, circuit), self.circuits.get(dev, circuit).presets)

    async def _rule_pulse(self, action: dict[str, Any]) -> None:
        dev, circuit = normalize_dev(action["dev"]), action["circuit"]
        try:
            spec = self._rule_spec(dev, circuit, action.get("pulse"), action.get("preset"))
            await self.sequencer.start(dev, circuit, spec, origin="rule")
            self._rec(action, "executed", True, f"pulse sequence started on {dev}/{circuit}: {spec}")
        except SequenceRejected as e:
            self._rec(action, "rejected", False, f"pulse refused: {e}")
            self.logger.warning(f"Rule pulse on {dev}/{circuit} rejected: {e}")
            self._publish_attributes(dev, circuit, {"last_error": f"rule: {e}"})

    def validate_rule(self, rule: "LocalLogicRule") -> str | None:
        """None if the rule is usable, else a human-readable reason (it will be disabled, not deleted)."""
        t = rule.action_type
        if t not in ("set", "dimmer", "toggle", "pulse"):
            return f"unknown action_type '{t}'"
        if rule.when not in ("always", "ha_offline"):
            return f"'when' must be 'always' or 'ha_offline', got '{rule.when}'"
        if not rule.trigger_dev or not rule.trigger_circuit:
            return "the trigger needs a device and a circuit"
        known = {"di", "ai", "ao", "ro", "do", "led", "temp", "humidity", "vdd", "vad", "vis", "1wdevice"}
        if normalize_dev(rule.trigger_dev) not in known:
            return f"unknown trigger device '{rule.trigger_dev}' (known: {sorted(known)})"
        for c in rule.conditions:
            if normalize_dev(c.dev) not in known:
                return f"unknown device '{c.dev}' in a condition (known: {sorted(known)})"
        dev = normalize_dev(rule.action_dev or "")
        if t == "set":
            if dev not in ("ro", "do", "led", "ao"):
                return f"'set' needs an output device (relay/digital output/LED/analog output), got '{rule.action_dev}'"
            if dev == "ao":
                try:
                    if not 0 <= float(rule.action_value) <= 10:
                        raise ValueError
                except (TypeError, ValueError):
                    return f"an analog output takes a voltage between 0 and 10, got {rule.action_value!r}"
            elif str(rule.action_value).strip().lower() not in ("1", "0", "on", "off", "true", "false"):
                return f"a {dev} output takes 1/0 or ON/OFF, got {rule.action_value!r}"
        if t in ("toggle", "pulse") and dev not in ("do", "ro", "led"):
            return f"'{t}' needs a digital output (do/ro/led), got '{rule.action_dev}'"
        if t == "pulse":
            if (rule.action_pulse is None) == (rule.action_preset is None):
                return "pulse needs exactly one of action_pulse / action_preset"
            try:
                self._rule_spec(dev, rule.action_circuit, rule.action_pulse, rule.action_preset)
            except SequenceRejected as e:
                return f"pulse not allowed on {dev}/{rule.action_circuit}: {e}"
        if t == "dimmer":
            if dev != "ao":
                return "dimmer needs an analog output (ao)"
            if rule.action_value not in (None, ""):
                try:
                    if not 0 < float(rule.action_value) <= 10:
                        raise ValueError
                except (TypeError, ValueError):
                    return f"dimmer level (action_value) must be a voltage in (0, 10], got {rule.action_value!r}"
            if not 200 <= rule.dimmer_hold_ms <= 5000:
                return f"dimmer hold time must be 200-5000 ms, got {rule.dimmer_hold_ms}"
            if not 0.2 <= rule.dimmer_speed <= 10:
                return f"dimmer speed must be 0.2-10 volts per second, got {rule.dimmer_speed}"
            if not 0 <= rule.dimmer_min <= 5:
                return f"dimmer minimum level must be 0-5 V, got {rule.dimmer_min}"
        return None

    def _load_dimmer_state(self) -> dict[str, dict[str, Any]]:
        try:
            with open(self._dimmer_state_file) as f:
                return json.load(f)
        except (OSError, ValueError):
            return {}

    def _save_dimmer_state(self) -> None:
        data = {rid: {"previous_level": st["previous_level"], "last_direction": st["last_direction"]}
                for rid, st in self.dimmer_states.items()}
        tmp = self._dimmer_state_file + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump({**self.dimmer_persist, **data}, f)
            os.replace(tmp, self._dimmer_state_file)
        except OSError as e:
            self.logger.warning(f"Could not save dimmer state: {e}")

    def execute_local_action(self, action: dict[str, Any]) -> None:
        """Executes a local action generated by the logic engine."""
        if self.shadow:
            self.logger.info(f"SHADOW: rule action would run: {action}")
            self._rec(action, "shadow", False, "shadow mode: action logged, not executed")
            return
        try:
            dev = action.get("dev")
            circuit = action.get("circuit")
            value = action.get("value")
            transition = action.get("transition")

            if not dev or not circuit:
                return

            # "Only when Home Assistant is unreachable": HA is in charge while it is up
            if action.get("when", "always") == "ha_offline" and self._ha_reachable():
                self.logger.debug(f"Rule action for {dev}/{circuit} skipped: Home Assistant is reachable")
                self._rec(action, "gated", False, "skipped: Home Assistant is reachable and this rule only runs when it is not")
                return

            # Handle action delay
            delay = action.get("delay")
            if delay is not None and float(delay) > 0:
                action_copy = action.copy()
                if "delay" in action_copy:
                    del action_copy["delay"]
                
                task_key = (dev, circuit)
                
                async def delayed_execution():
                    try:
                        await asyncio.sleep(float(delay))
                        self.execute_local_action(action_copy)
                    except asyncio.CancelledError:
                        self.logger.debug(f"Delayed action for {task_key} cancelled.")
                    finally:
                        if self.active_delayed_actions.get(task_key) == asyncio.current_task():
                            self.active_delayed_actions.pop(task_key, None)
                
                # Cancel existing delayed action for this target to prevent overlap
                old_task = self.active_delayed_actions.get(task_key)
                if old_task:
                    old_task.cancel()
                
                new_task = self.loop.create_task(delayed_execution(), name=f"DelayedAction-{dev}-{circuit}")
                self.active_delayed_actions[task_key] = new_task
                self._rec(action, "delayed", True, f"waiting {delay} s before {action.get('type', 'set')} {dev}/{circuit}")
                return

            action_type = action.get("type", "set")
            if action_type == "dimmer":
                self.handle_dimmer_action(action)
                return
            if action_type == "pulse":
                self.loop.create_task(self._rule_pulse(action), name=f"RulePulse-{dev}-{circuit}")
                return
            if action_type == "toggle":
                value = self._toggled_value(dev, circuit)

            if value is None:
                self._rec(action, "rejected", False, "the rule has no value to set")
                return

            self.logger.info(
                f"Executing Local Action: {dev} {circuit} -> {value} (Transition: {transition})"
            )

            # Construct WebSocket command
            cmd_data = {}

            match dev:
                case "relay" | "output" | "led" | "ro" | "do":
                    # Expecting 1/0 or ON/OFF
                    cmd_value = 1 if str(value).upper() == "ON" or str(value) == "1" else 0
                    cmd_data = {
                        "cmd": "set",
                        "dev": dev,
                        "circuit": circuit,
                        "value": cmd_value,
                    }

                    # Send MQTT ACK (Optimistic)
                    self.mqtt_ack(str(cmd_value), dev, circuit, origin="rule")

                case "analogoutput" | "ao":
                    # Handle AO
                    try:
                        target_value = float(value)
                        cmd_data = {
                            "cmd": "set",
                            "dev": dev,
                            "circuit": circuit,
                            "value": target_value,
                        }

                        # Send MQTT ACK (Optimistic)
                        # Assume 0-10V -> 0-1000 brightness scale
                        ack_value = int(target_value * 100)
                        self.mqtt_ack(str(ack_value), dev, circuit, origin="rule")

                        # Handle Transition if present
                        if transition is not None and float(transition) > 0:
                            transition_time_ms = float(transition)
                            transition_time_s = transition_time_ms / 1000.0

                            circuit_key = f"{dev}_{circuit}"
                            current_val_raw = self.device_states.get(circuit_key, 0)
                            try:
                                current_value = int(float(current_val_raw) * 100)
                            except (ValueError, TypeError):
                                current_value = 0

                            desired_value = int(target_value * 100)

                            this_instance_desired_value = desired_value
                            self.active_ao_transitions[(dev, circuit)] = desired_value

                            self.loop.create_task(
                                self._perform_ao_transition_task(
                                    dev,
                                    circuit,
                                    desired_value,
                                    current_value,
                                    transition_time_s,
                                    (dev, circuit),
                                    this_instance_desired_value,
                                ),
                                name=f"AOTransitionLocal-{dev}-{circuit}",
                            )
                            self._rec(action, "executed", True,
                                      f"fading {dev}/{circuit} to {target_value:g} V over {transition_time_ms:g} ms")
                            return  # Skip sending immediate command
                    except ValueError:
                        self.logger.error(f"Invalid value for AO action: {value}")
                        self._rec(action, "rejected", False, f"{value!r} is not a valid voltage")
                        return

            if cmd_data:
                if self.sequencer.is_running(dev, circuit):  # a rule overrides a running pulse/timed sequence
                    self.loop.create_task(self.sequencer.cancel(dev, circuit, "rule action"))
                self.commands.send_ws(cmd_data["dev"], cmd_data["circuit"], cmd_data["value"])
                self._rec(action, "executed", True,
                          f"{'toggle' if action_type == 'toggle' else 'set'} {dev}/{circuit} = {cmd_data['value']} sent to the Unipi")

        except Exception as e:
            self.logger.error(f"Error executing local action: {e}")
            self._rec(action, "error", False, f"error while executing: {e}")

    def handle_dimmer_action(self, action: dict[str, Any]) -> None:
        """Dimmer rule. hold=True: short press toggles on release, long press dims while held.
        hold=False: toggles on the press edge only (legacy wall-switch behaviour)."""
        rule_id = action.get("rule_id")
        dev = action.get("dev")
        circuit = action.get("circuit")
        trigger_value = action.get("trigger_value")

        if not rule_id or not dev or not circuit or trigger_value is None:
            return

        if dev not in ("ao", "analogoutput"):
            self.logger.error("Dimmer action is only supported for Analog Outputs (ao).")
            return

        try:
            level = float(action.get("value")) if action.get("value") not in (None, "") else 10.0
        except (TypeError, ValueError):
            level = 10.0

        if rule_id not in self.dimmer_states:
            saved = self.dimmer_persist.get(rule_id, {})
            self.dimmer_states[rule_id] = {
                "last_direction": saved.get("last_direction", 1),
                "previous_level": saved.get("previous_level", level),   # survives restarts
                "is_dimming": False,
                "dimming_task": None,
            }

        state = self.dimmer_states[rule_id]

        try:
            trigger_val_int = int(float(trigger_value))
        except (ValueError, TypeError):
            return

        if not action.get("hold", True):
            if trigger_val_int == 1:       # press edge only; the release is ignored
                self._dimmer_toggle(state, dev, circuit, level, action)
            return

        if trigger_val_int == 1:
            # Button Pressed
            state["pressed"] = True
            state["is_dimming"] = False
            # Cancel any existing task just in case
            if state["dimming_task"]:
                state["dimming_task"].cancel()

            state["dimming_task"] = self.loop.create_task(
                self._dimmer_hold_task(rule_id, dev, circuit, int(action.get("hold_ms", 500)),
                                       float(action.get("speed", 2.5)), float(action.get("min_v", 1.0)), level),
                name=f"DimmerHold-{rule_id}"
            )
        elif trigger_val_int == 0:
            # Button Released. A release whose press was not accepted (e.g. a condition said no) must do nothing:
            # it is let through the engine so that dimming always stops, not so that it can toggle the lamp.
            if not state.get("pressed"):
                return
            state["pressed"] = False
            task = state.get("dimming_task")
            if task:
                task.cancel()
                state["dimming_task"] = None

            if state.get("is_dimming"):
                # Was dimming, stop and save state
                try:
                    current_val = float(self.device_states.get(f"{dev}_{circuit}", 0))
                except (ValueError, TypeError):
                    current_val = 0.0
                state["last_direction"] = -state.get("dim_direction", state.get("last_direction", 1))
                state["previous_level"] = current_val
                state["is_dimming"] = False
                self._save_dimmer_state()
                self.mqtt_ack(str(int(current_val * 100)), dev, circuit, origin="rule")
                self._rec(action, "executed", True, f"dimming stopped at {current_val:g} V")
            else:
                self._dimmer_toggle(state, dev, circuit, level, action)

    def _dimmer_toggle(self, state: dict[str, Any], dev: str, circuit: str, level: float, action: dict[str, Any] | None = None) -> None:
        try:
            current_val = float(self.device_states.get(f"{dev}_{circuit}", 0))
        except (ValueError, TypeError):
            current_val = 0.0
        if current_val > 0.0:
            state["previous_level"] = current_val   # remember where it was, then turn OFF
            target_val = 0.0
        else:
            target_val = state.get("previous_level") or level
            if target_val <= 0.0:
                target_val = level
        self.commands.send_ws(dev, circuit, target_val)
        self.mqtt_ack(str(int(target_val * 100)), dev, circuit, origin="rule")
        self._save_dimmer_state()
        if action:
            self._rec(action, "executed", True, f"dimmer {dev}/{circuit} -> {target_val:g} V")

    async def _dimmer_hold_task(self, rule_id: str, dev: str, circuit: str, hold_ms: int = 500,
                                speed: float = 2.5, min_v: float = 1.0, level: float = 10.0) -> None:
        """While the button is held (after hold_ms): dim like a Z-Wave dimmer.
        Direction alternates between holds; at the top it goes down, at the bottom (or when off) it goes up.
        Dimming down stops at min_v and keeps the lamp ON; only a short press switches it off."""
        key = f"{dev}_{circuit}"
        try:
            await self._dim_sleep(hold_ms / 1000.0)
            state = self.dimmer_states.get(rule_id)
            if not state:
                return
            state["is_dimming"] = True
            try:
                cur = float(self.device_states.get(key, 0))
            except (ValueError, TypeError):
                cur = 0.0
            direction = state.get("last_direction", 1)
            if cur <= 0.05:                       # off: switch on at the lowest level, then brighten
                cur, direction = min_v, 1
                self.commands.send_ws(dev, circuit, round(cur, 2))
                self.device_states[key] = cur
            elif cur >= 10.0 - 0.05:
                direction = -1
            elif cur <= min_v + 0.05:
                direction = 1
            state["dim_direction"] = direction

            step_time, n = 0.1, 0
            step = speed * step_time * direction
            while True:
                new = max(min_v, min(10.0, cur + step))
                if new != cur:
                    cur = new
                    self.commands.send_ws(dev, circuit, round(cur, 2))
                    self.device_states[key] = cur       # optimistic; evok confirms shortly after
                    n += 1
                    if n % 5 == 0:                      # keep HA in step without a message every 100 ms
                        self.mqtt_ack(str(int(cur * 100)), dev, circuit, origin="rule")
                await self._dim_sleep(step_time)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            self.logger.error(f"Error in dimmer hold task: {e}")

    def publish_availability(self, status: str) -> None:
        """
        Publishes the availability status (online/offline) to the MQTT broker.
        """
        if not self.device_name:
            return

        availability_topic = f"{self.config.mqtt.topic}/{self.device_name}/status"
        self.logger.info(
            f"Publishing '{status}' status to availability topic: {availability_topic}"
        )
        try:
            self.mqtt_client.publish(
                availability_topic, payload=status, qos=1, retain=True
            )
        except Exception as e:
            self.logger.error(
                f"Error publishing availability status: {e}", exc_info=True
            )
        self.events.emit(AVAILABILITY, online=(status == "online"), ts=time.time())

    def publish_error(self, is_error: bool, details: str = "") -> None:
        """
        Publishes error status and details to MQTT.
        """
        if not self.device_name:
            return

        root_topic = self.config.mqtt.topic
        device_topic = f"{root_topic}/{self.device_name}_bridge"
        status_topic = f"{device_topic}/connectivity_problem"
        details_topic = f"{device_topic}/connectivity_details"

        try:
            self.mqtt_client.publish(
                status_topic, "1" if is_error else "0", retain=True
            )
            self.mqtt_client.publish(details_topic, details, retain=True)
        except Exception as e:
            self.logger.error(f"Error publishing error status: {e}", exc_info=True)

    async def setup_web_server(self) -> None:
        """Sets up and starts the local web server."""
        try:
            app = web.Application(middlewares=[self.auth_middleware])
            if os.path.exists(self.web_static_path):
                app.router.add_static("/static", self.web_static_path)
            else:
                self.logger.warning(
                    f"Static web directory not found at {self.web_static_path}, /static route disabled."
                )
            app.router.add_get("/", self.web_handler_index)
            app.router.add_get("/api/rules", self.web_handler_get_rules)
            app.router.add_post("/api/rules", self.web_handler_add_rule)
            app.router.add_put("/api/rules", self.web_handler_update_rules)
            # app.router.add_put('/api/rules/{id}',
            # self.web_handler_update_rule) # Not implemented yet
            app.router.add_delete("/api/rules/{rule_id}", self.web_handler_delete_rule)
            app.router.add_get("/api/status", self.web_handler_get_status)
            app.router.add_get("/api/rule_trace", self.web_handler_rule_trace)
            app.router.add_get("/api/inputs", self.web_handler_get_inputs)
            app.router.add_post("/api/inputs/{circuit}", self.web_handler_update_input)

            # Auth API
            app.router.add_post("/api/login", self.web_handler_login)
            app.router.add_post("/api/logout", self.web_handler_logout)
            app.router.add_get("/api/auth_check", self.web_handler_auth_check)
            app.router.add_get("/api/info", self.web_handler_get_info)

            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(
                runner, self.config.web_server.host, self.config.web_server.port
            )
            await site.start()

            self.web_runner = runner
            self.web_site = site

            self.logger.info(
                f"Web Server started on port {self.config.web_server.port}"
            )
        except Exception as e:
            self.logger.error(f"Failed to start Web Server: {e}")

    async def web_handler_index(self, request: web.Request) -> web.StreamResponse:
        """Serves the index.html page."""
        try:
            # Serve from web/index.html if exists, else serve simple string
            index_path = os.path.join(os.path.dirname(__file__), "web", "index.html")
            if os.path.exists(index_path):
                return web.FileResponse(index_path)
            else:
                return web.Response(
                    text="<h1>Unipi Local Logic Engine</h1><p>web/index.html not found.</p>",
                    content_type="text/html",
                )
        except Exception as e:
            self.logger.error(f"Error serving index: {e}")
            return web.Response(status=500)

    async def web_handler_get_info(self, request: web.Request) -> web.Response:
        """Returns public device info (safe for unauthenticated users)."""
        import socket

        ip_address = "Unknown"
        try:
            # Try to get the IP that can reach an external address (Google DNS)
            # This is usually the primary interface IP
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.settimeout(0)
            try:
                # doesn't even have to be reachable
                s.connect(("10.255.255.255", 1))
                ip_address = s.getsockname()[0]
            except Exception:
                ip_address = "127.0.0.1"
            finally:
                s.close()
        except Exception:
            pass

        model = "Unknown"
        sn = "Unknown"
        if self.device_info:
            model = self.device_info.get("model", "Unknown")
            sn = self.device_info.get("sn", "Unknown")
            family = self.device_info.get("family", "")
            if family:
                model = f"{family} {model}"

        return web.json_response(
            {"name": self.device_name, "model": model, "sn": sn, "ip": ip_address}
        )

    async def web_handler_get_rules(self, request: web.Request) -> web.Response:
        """Returns the list of rules."""
        rules_data = []
        for rule in self.local_logic.rules:
            d = rule.model_dump()
            if rule.id in self.local_logic.disabled:
                d["disabled_reason"] = self.local_logic.disabled[rule.id]   # shown as a warning on the block
            rules_data.append(d)
        return web.json_response(rules_data)

    async def web_handler_rule_trace(self, request: web.Request) -> web.Response:
        """Recent rule activity (what the engine saw and decided). ?since=<seq> returns only newer events."""
        try:
            since = int(request.query.get("since", "0"))
        except ValueError:
            since = 0
        return web.json_response(self.local_logic.trace_since(since))

    async def web_handler_get_status(self, request: web.Request) -> web.Response:
        """Returns the current device states."""
        return web.json_response(self.device_states)

    async def web_handler_add_rule(self, request: web.Request) -> web.Response:
        """Adds a new rule."""
        try:
            data = await request.json()
            # Remove ID if present to ensure new ID is generated, or allow update if ID matches?
            # Let's assume add/update based on ID presence?
            # For now, just add new.
            if "id" in data and not data["id"]:
                del data["id"]

            rule = LocalLogicRule(**data)
            err = self.validate_rule(rule)
            if err:
                return web.json_response({"error": err}, status=400)
            self.local_logic.rules.append(rule)
            self.local_logic.revalidate()
            self.local_logic.save_rules()
            return web.json_response(rule.model_dump())
        except ValidationError as e:
            return web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            return web.json_response({"error": str(e)}, status=500)

    async def web_handler_update_rules(self, request: web.Request) -> web.Response:
        """Handles PUT /api/rules to replace all rules."""
        try:
            data = await request.json()
            if not isinstance(data, list):
                return web.json_response(
                    {"error": "Expected a list of rules"}, status=400
                )

            new_rules = []
            errors = []
            for item in data:
                rule = LocalLogicRule(**item)
                err = self.validate_rule(rule)
                if err:
                    errors.append(f"rule '{rule.name}': {err}")
                new_rules.append(rule)
            if errors:  # all or nothing: a broken list never replaces a working one
                return web.json_response({"error": "; ".join(errors)}, status=400)

            self.local_logic.replace_rules(new_rules)
            return web.json_response({"status": "ok", "count": len(new_rules)})
        except Exception as e:
            self.logger.error(f"Error updating rules: {e}")
            return web.json_response({"error": str(e)}, status=400)

    async def web_handler_delete_rule(self, request: web.Request) -> web.Response:
        """Deletes a rule by ID."""
        rule_id = request.match_info["rule_id"]
        initial_len = len(self.local_logic.rules)
        self.local_logic.rules = [r for r in self.local_logic.rules if r.id != rule_id]

        if len(self.local_logic.rules) < initial_len:
            self.local_logic.save_rules()
            return web.json_response({"status": "deleted"})
        else:
            return web.json_response({"error": "Rule not found"}, status=404)

    async def web_handler_get_inputs(self, request: web.Request) -> web.Response:
        """Returns list of Digital Inputs with their config."""
        inputs = []
        for item in self.discovered_devices:
            dev = item.get("dev")
            circuit = str(item.get("circuit", ""))
            # Check for both "input" and "di" device types
            if dev in ["input", "di"] and circuit:
                # Check current config
                inverted = self.circuits.is_inverted(dev, circuit)

                # Get current value (state), trying both prefixes
                current_val = self.device_states.get(f"{dev}_{circuit}")
                if current_val is None and dev == "input":
                    current_val = self.device_states.get(f"di_{circuit}")
                # device_states holds the logical value; this screen shows the physical contact state
                current_val = self.circuits.logical(dev, circuit, current_val, inverted=inverted)

                inputs.append(
                    {
                        "circuit": circuit,
                        "dev": dev,
                        "value": current_val,
                        "inverted": inverted,
                    }
                )

        self.logger.info(
            f"API /api/inputs returning {len(inputs)} items. Total discovered: {len(self.discovered_devices)}"
        )
        if len(inputs) == 0 and len(self.discovered_devices) > 0:
            # Log the dev types we found to debug
            found_types = set(d.get("dev") for d in self.discovered_devices)
            self.logger.info(f"Debug: Found device types: {found_types}")

        return web.json_response(inputs)

    async def web_handler_update_input(self, request: web.Request) -> web.Response:
        """Updates input configuration (NO/NC)."""
        circuit = request.match_info["circuit"]
        try:
            data = await request.json()
            inverted = data.get("inverted")
            if not isinstance(inverted, bool):
                return web.json_response(
                    {"error": "Invalid 'inverted' value"}, status=400
                )

            old_inverted = self.circuits.is_inverted("di", circuit)

            # Update config
            if circuit not in self.config.inputs:
                self.config.inputs[circuit] = {}
            self.config.inputs[circuit]["inverted"] = inverted

            # Save to file (P1.5): read-merge-write only the changed key so we
            # never persist secrets (e.g. MQTT_PASSWORD from env), never drop
            # unknown keys/comments, and don't reformat the user's file. Do NOT
            # serialize the pydantic model to disk.
            try:
                try:
                    with open(self.config_path, "r") as f:
                        file_data = json.load(f)
                except (FileNotFoundError, json.JSONDecodeError):
                    file_data = {}
                file_data.setdefault("inputs", {})[circuit] = {"inverted": inverted}
                with open(self.config_path, "w") as f:
                    json.dump(file_data, f, indent=2)
                os.chmod(self.config_path, 0o600)
            except Exception as e:
                self.logger.error(f"Failed to save config to {self.config_path}: {e}")
                return web.json_response({"error": "Failed to save config"}, status=500)

            # Trigger Re-discovery for this circuit
            # We need to find the device_info item again
            target_item = None
            device_info = None

            for item in self.discovered_devices:
                if item.get("dev") == "device_info":
                    device_info = item
                # Match circuit AND device type (input or di)
                # Matches get_inputs filter
                if item.get("circuit") == circuit and item.get("dev") in [
                    "input",
                    "di",
                ]:
                    target_item = item

            if target_item and device_info:
                # Inject correct current value from device_states to ensure instant update
                # Target item has stale startup 'value'.
                dev = target_item.get("dev")
                current_val = self.device_states.get(f"{dev}_{circuit}")
                if current_val is None and dev == "input":
                    current_val = self.device_states.get(f"di_{circuit}")

                # Create a copy so we don't mutate the original discovery
                # record permanently (though it might be fine)
                item_to_publish = target_item.copy()
                if current_val is not None:
                    # device_states holds the LOGICAL value (old setting); discovery expects the physical one
                    item_to_publish["value"] = self.circuits.logical(
                        dev, circuit, current_val, inverted=old_inverted
                    )

                # HA Update Strategy:
                # To force HA to accept the payload inversion (NO->NC) on an existing entity,
                # we momentarily mark the device as "offline", then publish correct config,
                # then come back "online", then publish state.

                avail_topic = f"{self.config.mqtt.topic}/{self.device_name}/status"
                self._enqueue_ws_to_mqtt((avail_topic, "offline"))

                # Publish Config (with new NO/NC payloads)
                self.publish_discovery_config(item_to_publish, device_info)

                # Wait a moment for HA to process offline + new config
                await asyncio.sleep(0.2)

                # Come back Online
                self._enqueue_ws_to_mqtt((avail_topic, "online"))

                # Publish State
                dev_type = item_to_publish.get("dev")
                state_topic = self.generate_mqtt_topic_update(
                    dev_type, circuit, "state"
                )
                final_val = self.circuits.logical(dev_type, circuit, item_to_publish.get("value"))
                if final_val is not None:
                    self.device_states[f"{dev_type}_{circuit}"] = final_val
                try:
                    state_payload = (
                        "ON"
                        if final_val is not None and int(float(final_val)) == 1
                        else "OFF"
                    )
                except (ValueError, TypeError):
                    state_payload = "OFF"
                self._enqueue_ws_to_mqtt((state_topic, state_payload))

                return web.json_response({"status": "updated", "inverted": inverted})
            elif not target_item:
                return web.json_response({"error": "Device not found"}, status=404)
            else:
                return web.json_response({"error": "Device info not found"}, status=500)

        except Exception as e:
            self.logger.error(f"Error updating input config: {e}")
            return web.json_response({"error": str(e)}, status=500)


if __name__ == "__main__":
    # Argument Parsing
    parser = argparse.ArgumentParser(description="Unipi to Home Assistant Bridge")
    parser.add_argument(
        "--config", type=str, default="config.json", help="Path to configuration file"
    )
    parser.add_argument(
        "--record", type=str, help="Enable traffic recording to the specified JSON file"
    )
    args = parser.parse_args()

    config_file = args.config
    if not os.path.exists(config_file):
        # Fallback to default location if not found in current dir
        default_config = os.path.expanduser("~/.config/unipi-homeassistant/config.json")
        if os.path.exists(default_config):
            config_file = default_config

    # Load Config
    try:
        config = AppConfig.load_from_env_and_file(config_file)
    except Exception as e:
        print(f"Critical Error: Failed to load configuration: {e}")
        exit(1)

    # Warn if the config file (which may hold the MQTT password) is readable by
    # other users. (P1.5)
    try:
        mode = os.stat(config_file).st_mode
        if mode & 0o077:
            print(
                f"WARNING: {config_file} is readable by other users. "
                f"Run: chmod 600 {config_file}"
            )
    except OSError:
        pass

    # Initialize Recorder
    recorder = None
    if args.record:
        recorder = TrafficRecorder(args.record)

    # Initialize Bridge
    bridge = UnipiBridge(config, config_file, recorder=recorder)

    # Run
    try:
        bridge.run()
    except Exception as e:
        print(f"Critical Error during execution: {e}")
        exit(1)
    exit(bridge.exit_code)
