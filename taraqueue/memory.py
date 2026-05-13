"""Memory queue implementation."""

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field

from yarl import URL

from taraqueue import Queue, QueueEmpty

_channels: dict[str, list[asyncio.Queue[str]]] = {}


@dataclass
class MemoryQueue(Queue):
    """In-process queue with fan-out semantics."""

    queue: asyncio.Queue[str] = field(default_factory=asyncio.Queue)

    @classmethod
    def from_url(cls, url: URL) -> MemoryQueue:
        return cls()

    async def subscribe(self, topic: str) -> None:
        """See `Queue.subscribe`."""
        subscribers = _channels.setdefault(topic, [])
        if self.queue in subscribers:
            return
        subscribers.append(self.queue)

    async def unsubscribe(self, topic: str) -> None:
        """See `Queue.unsubscribe`."""
        subscribers = _channels.get(topic)
        if subscribers is None:
            return
        with suppress(ValueError):
            subscribers.remove(self.queue)
        if not subscribers:
            _channels.pop(topic, None)

    async def receive(self, timeout=None) -> str:
        """See `Queue.receive`."""
        if not timeout:
            try:
                return self.queue.get_nowait()
            except asyncio.QueueEmpty as e:
                raise QueueEmpty("Queue is empty") from e
        try:
            return await asyncio.wait_for(self.queue.get(), timeout=float(timeout))
        except TimeoutError as e:
            raise QueueEmpty("Timed out waiting for message") from e

    async def publish(self, topic: str, message: str) -> None:
        """See `Queue.publish`."""
        for q in list(_channels.get(topic, [])):
            await q.put(message)
