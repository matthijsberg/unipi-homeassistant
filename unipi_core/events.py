"""In-process event bus: lets features (legacy adapter, sequencer, hold filters) observe state
changes without being wired into the bridge's message handlers.

Event kinds and their keyword payloads:
  INPUT_CHANGED   dev, circuit, value, raw, ts, source ("ws" | "republish"), subkey (1-wire, or None)
  OUTPUT_CHANGED  dev, circuit, value, ts, origin ("mqtt" | "rule" | "fade" | "sequence")
  AVAILABILITY    online (bool), ts

Subscribers run on the asyncio loop thread, never inline in a foreign thread, and an exception in
a subscriber is logged and never reaches the emitter.
"""
from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from typing import Any, Callable

INPUT_CHANGED = "input_changed"
OUTPUT_CHANGED = "output_changed"
AVAILABILITY = "availability"
KINDS = (INPUT_CHANGED, OUTPUT_CHANGED, AVAILABILITY)

Callback = Callable[..., None]


class EventBus:
    def __init__(self, loop: asyncio.AbstractEventLoop, logger: logging.Logger | None = None):
        self.loop = loop
        self.logger = logger or logging.getLogger("EventBus")
        self._subs: dict[str, list[Callback]] = defaultdict(list)

    def subscribe(self, kind: str, callback: Callback) -> None:
        if kind not in KINDS:
            raise ValueError(f"unknown event kind {kind!r}; expected one of {KINDS}")
        self._subs[kind].append(callback)

    def unsubscribe(self, kind: str, callback: Callback) -> None:
        if callback in self._subs.get(kind, []):
            self._subs[kind].remove(callback)

    def emit(self, kind: str, **data: Any) -> None:
        if not self._subs.get(kind):
            return  # nothing subscribed: zero cost on the hot path
        try:
            running = asyncio.get_running_loop()
        except RuntimeError:
            running = None
        # On the loop thread (or with the loop idle, as in synchronous tests) dispatch now;
        # from any other thread (e.g. paho's) hop onto the loop.
        if running is self.loop or (running is None and not self.loop.is_running()):
            self._dispatch(kind, data)
        else:
            self.loop.call_soon_threadsafe(self._dispatch, kind, data)

    def _dispatch(self, kind: str, data: dict[str, Any]) -> None:
        for cb in list(self._subs.get(kind, [])):
            try:
                cb(**data)
            except Exception:
                self.logger.exception("EventBus subscriber %r failed on %s", cb, kind)
