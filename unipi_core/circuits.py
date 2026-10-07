"""Per-circuit configuration (config.json -> "circuits").

Keys are "<dev>/<circuit>" (or "<dev>/<circuit>/<subkey>" for the values of a multi-value 1-wire
device), e.g. "di/1_01", "ro/xS51_01", "1wdevice/28D1EFA708000052/temp". Device aliases of older
evok versions are accepted and canonicalised (input=di, relay=ro, output=do, analogoutput=ao).

Only options that are IMPLEMENTED are accepted. Options planned for later tasks are rejected with
an explicit message so that a setting can never be silently ignored.
"""
from __future__ import annotations

import logging
import re
from typing import Any

from pydantic import BaseModel, ConfigDict, model_validator

DEV_ALIASES = {"input": "di", "relay": "ro", "output": "do", "analogoutput": "ao"}
KNOWN_DEVS = {"di", "do", "ro", "led", "ai", "ao", "temp", "1wdevice"}
ONEWIRE_SUBKEYS = {"temp", "humidity", "vdd", "vad", "vis"}

# HA binary_sensor / sensor / switch device classes we know of. Unknown values only warn:
# Home Assistant may add classes later and a config typo must not take the bridge down.
BINARY_SENSOR_CLASSES = {
    "battery", "battery_charging", "carbon_monoxide", "cold", "connectivity", "door", "garage_door",
    "gas", "heat", "light", "lock", "moisture", "motion", "moving", "occupancy", "opening", "plug",
    "power", "presence", "problem", "running", "safety", "smoke", "sound", "tamper", "update",
    "vibration", "window",
}

PLANNED_KEYS = {
    "off_delay_s": "T14", "counter": "T15", "counter_interval_s": "T15", "failsafe_off": "T12",
    "max_on_s": "T12", "pulse_defaults": "T12", "max_count": "T12", "max_pulse_ms": "T12",
    "presets": "T12/T13", "unit": "T16", "transform": "T16", "sampling": "T16", "valid_range": "T16",
    "reject_values": "T16", "ha_component": "T19",
}


class CircuitConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    device_class: str | None = None
    inverted: bool | None = None  # None = not set here (legacy "inputs" section may still apply)

    @model_validator(mode="before")
    @classmethod
    def _explain_unsupported(cls, data: Any) -> Any:
        if isinstance(data, dict):
            if "area" in data:
                raise ValueError(
                    "'area' is not supported: Home Assistant MQTT discovery only has a per-DEVICE "
                    "suggested_area, no per-entity area. Assign areas to entities in Home Assistant."
                )
            for k in data:
                if k in PLANNED_KEYS:
                    raise ValueError(f"option '{k}' is planned ({PLANNED_KEYS[k]}) but not supported in this version")
        return data


def normalize_dev(dev: str) -> str:
    return DEV_ALIASES.get(dev, dev)


def canonical_key(key: str) -> str:
    """'input/1_01' -> 'di/1_01'. Raises ValueError (naming the key) on a malformed key."""
    parts = key.split("/")
    if len(parts) not in (2, 3) or not all(parts):
        raise ValueError(f"circuit key '{key}' must look like '<dev>/<circuit>' (or '<dev>/<circuit>/<subkey>' for 1-wire)")
    dev = normalize_dev(parts[0])
    if dev not in KNOWN_DEVS:
        raise ValueError(f"circuit key '{key}': unknown device type '{parts[0]}' (known: {sorted(KNOWN_DEVS | set(DEV_ALIASES))})")
    if len(parts) == 3 and not (dev == "1wdevice" and parts[2] in ONEWIRE_SUBKEYS):
        raise ValueError(f"circuit key '{key}': a third part is only valid for 1wdevice and one of {sorted(ONEWIRE_SUBKEYS)}")
    return "/".join([dev, *parts[1:]])


def canonicalize_circuits(raw: Any) -> Any:
    """Pre-validation hook for AppConfig.circuits: canonical keys, duplicates are an error."""
    if not isinstance(raw, dict):
        return raw
    out: dict[str, Any] = {}
    for k, v in raw.items():
        ck = canonical_key(k)
        if ck in out:
            raise ValueError(f"circuit '{ck}' is configured twice (aliases of the same device type)")
        dev = ck.split("/")[0]
        if isinstance(v, dict):
            if "inverted" in v and dev != "di":
                raise ValueError(f"circuit '{ck}': 'inverted' only applies to digital inputs (di)")
            if "device_class" in v and dev in ("led", "ao", "1wdevice"):
                raise ValueError(f"circuit '{ck}': 'device_class' is not supported for {dev} entities")
        out[ck] = v
    return out


_EMPTY = CircuitConfig()


class CircuitRegistry:
    """Read access to the circuit settings, with fallback to the legacy `inputs` section."""

    def __init__(self, config: Any, logger: logging.Logger | None = None):
        self._config = config
        self._log = logger or logging.getLogger("CircuitRegistry")
        self._warned: set[str] = set()

    def get(self, dev: str, circuit: str, subkey: str | None = None) -> CircuitConfig:
        base = f"{normalize_dev(dev)}/{circuit}"
        circuits = self._config.circuits
        if subkey and f"{base}/{subkey}" in circuits:
            return circuits[f"{base}/{subkey}"]
        return circuits.get(base, _EMPTY)

    def name(self, dev: str, circuit: str, default: str, subkey: str | None = None) -> str:
        return self.get(dev, circuit, subkey).name or default

    def is_inverted(self, dev: str, circuit: str) -> bool:
        """Logical inversion applies to digital inputs only."""
        if normalize_dev(dev) != "di":
            return False
        # The web UI's NO/NC switch writes the `inputs` section at runtime; an explicit user action
        # there wins over the static `circuits` setting.
        ui = self._config.inputs.get(circuit)
        if ui and "inverted" in ui:
            return bool(ui["inverted"])
        return bool(self.get(dev, circuit).inverted)

    def device_class(self, dev: str, circuit: str, component: str, subkey: str | None = None) -> str | None:
        dc = self.get(dev, circuit, subkey).device_class
        if dc and component == "binary_sensor" and dc not in BINARY_SENSOR_CLASSES and (circuit, dc) not in self._warned:
            self._warned.add((circuit, dc))
            self._log.warning(f"{dev}/{circuit}: device_class '{dc}' is not a known binary_sensor class; passing it to Home Assistant anyway")
        return dc

    def logical(self, dev: str, circuit: str, value: Any, inverted: bool | None = None) -> Any:
        """Apply (or, being an involution, remove) the NO/NC inversion on a digital-input value."""
        inv = self.is_inverted(dev, circuit) if inverted is None else inverted
        if not inv:
            return value
        try:
            return 1 - int(float(value))
        except (TypeError, ValueError):
            return value
