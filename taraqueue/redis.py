"""Redis queue implementation."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING

from yarl import URL

from taraqueue import Queue, QueueEmpty

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub


@dataclass
class RedisQueue(Queue):

    client: Redis = field()
    pubsub: PubSub = field()

    @classmethod
    def from_env(cls, env=os.environ) -> RedisQueue:
        host = env.get("REDIS_SLAVEOF_IP", "") or env.get("IPV4_NETWORK", "172.22.1") + ".249"
        port = int(env.get("REDIS_SLAVEOF_PORT", "") or "6379")
        password = env.get("REDIS_PASSWORD")
        return cls.from_host(host, port, password=password)

    @classmethod
    def from_host(cls, host: str, port: int = 6379, password: str | None = None) -> RedisQueue:
        from redis.asyncio import StrictRedis

        client = StrictRedis(
            host=host,
            port=port,
            decode_responses=True,
            db=0,
            password=password,
        )
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        return cls(client, pubsub)

    @classmethod
    def from_url(cls, url: URL | str) -> RedisQueue:
        url = URL(url)
        return cls.from_host(url.host, url.port, password=url.password)

    async def subscribe(self, topic: str) -> None:
        """See `Queue.subscribe`."""
        await self.pubsub.subscribe(topic)

    async def unsubscribe(self, topic: str) -> None:
        """See `Queue.unsubscribe`."""
        await self.pubsub.unsubscribe(topic)

    async def receive(self, timeout=0) -> str:
        """See `Queue.receive`."""
        stop_time = time() + timeout
        while True:
            remaining_timeout = max(0.0, stop_time - time())
            message = await self.pubsub.get_message(ignore_subscribe_messages=True, timeout=remaining_timeout)
            if message:
                return message["data"]
            if time() >= stop_time:
                raise QueueEmpty("Queue is empty")

    async def publish(self, topic: str, message: str) -> None:
        """See `Queue.publish`."""
        await self.client.publish(topic, message)
