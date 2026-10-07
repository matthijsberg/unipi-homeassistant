"""CommandService: the single place the bridge (and later the sequencer / legacy adapter / local
rules) uses to drive Unipi outputs. In T10 it only wraps existing code paths."""
from __future__ import annotations

import json
import logging
import queue
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover
    from typing import Protocol

    class _Bridge(Protocol):
        mqtt_to_websocket_queue: queue.Queue
        logger: logging.Logger

        def generate_mqtt_topic_update(self, dev: str, circuit: str, topic_end: str) -> str: ...


class CommandService:
    def __init__(self, bridge: "_Bridge"):
        self.bridge = bridge

    # -- low level -------------------------------------------------------------------------
    def send_ws(self, dev: str, circuit: str, value: Any, *, block: bool = False) -> dict[str, Any] | None:
        """Queue one `set` command for the evok WebSocket. Returns the command dict, or None if the
        queue was full (command dropped, error logged). `block=True` keeps the legacy behaviour of
        AO fades (blocking put) until T12 replaces them."""
        cmd = {"cmd": "set", "dev": dev, "circuit": circuit, "value": value}
        q = self.bridge.mqtt_to_websocket_queue
        try:
            if block:
                q.put(json.dumps(cmd))
            else:
                q.put_nowait(json.dumps(cmd))
        except queue.Full:
            self.bridge.logger.error(f"MQTT->WS queue full! Dropping command for {dev}/{circuit}")
            return None
        return cmd

    # -- high level (verified / acked paths of the bridge) ---------------------------------
    async def set_digital(self, dev: str, circuit: str, on: bool) -> None:
        """Same path as an MQTT ON/OFF command: WS write, read-back verification, ack to HA."""
        topic = self.bridge.generate_mqtt_topic_update(dev, circuit, "set")
        await self.bridge.process_mqtt_message(topic, "ON" if on else "OFF")

    async def transition(self, dev: str, circuit: str, target: int, seconds: float = 0.0) -> None:
        """Fade an analog output to `target` (0-1000) over `seconds`, as an MQTT JSON command would."""
        topic = self.bridge.generate_mqtt_topic_update(dev, circuit, "set")
        await self.bridge.process_ao_transition(topic, "", seconds, target)
