"""Queue abstraction."""

from abc import ABC, abstractmethod
from contextlib import asynccontextmanager
from dataclasses import dataclass

from yarl import URL

from taraqueue.registry import registry_load


class QueueEmpty(Exception):
    """Raised when the queue is empty."""


@dataclass
class Queue(ABC):
    """Base queue class."""

    @classmethod
    def from_url(cls, url: URL | str, registry=None) -> "Queue":
        if registry is None:
            registry = registry_load("taraqueue")
        scheme = URL(url).scheme
        queue_cls = registry["taraqueue"][scheme]
        return queue_cls.from_url(url)

    @abstractmethod
    async def subscribe(self, topic: str) -> None:
        """Subscribe to a topic before receiving messages."""

    @abstractmethod
    async def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic after receiving messages."""

    @abstractmethod
    async def receive(self, timeout=None) -> str:
        """Listen for messages on the subscribed topics."""

    @abstractmethod
    async def publish(self, topic: str, message: str) -> None:
        """Publish a message to a topic."""

    @asynccontextmanager
    async def connect(self, topic: str):
        """Context manager that subscribes on entry and unsubscribes on exit."""
        await self.subscribe(topic)
        try:
            yield self
        finally:
            await self.unsubscribe(topic)
