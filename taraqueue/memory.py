"""Memory queue implementation."""

from collections import defaultdict
from contextlib import suppress

from attrs import define, field
from yarl import URL

from taraqueue import Queue, QueueEmpty

_global_memory_queues = defaultdict(list)


@define
class MemoryQueue(Queue):

    topics = field(factory=list)
    queues = field(default=_global_memory_queues)

    @classmethod
    def from_url(cls, url: URL) -> "MemoryQueue":
        return cls()

    async def subscribe(self, topic: str) -> None:
        """See `Queue.subscribe`."""
        self.topics.append(topic)

    async def unsubscribe(self, topic: str) -> None:
        """See `Queue.unsubscribe`."""
        with suppress(ValueError):
            self.topics.remove(topic)

    async def receive(self, timeout=None) -> str:
        """See `Queue.receive`."""
        for topic in self.topics[:]:
            # Cycle through topics.
            self.topics.append(self.topics.pop(0))
            queue = self.queues[topic]
            with suppress(IndexError):
                return queue.pop(0)

        raise QueueEmpty("Queue is empty")

    async def publish(self, topic: str, message: str) -> None:
        """See `Queue.publish`."""
        queue = self.queues[topic]
        queue.append(message)



