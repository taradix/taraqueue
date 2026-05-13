"""Integration tests for the queue module."""


import pytest

from taraqueue import Queue, QueueEmpty


async def test_queue_send_receive(queue, unique):
    """Sending a message to a queue should be the next received message."""
    topic = unique("text")
    async with queue.connect(topic) as session:
        await session.publish(topic, "test")
        result = await session.receive(5)
    assert result == "test"


async def test_queue_receive_empty(queue, unique):
    """Receiving from an empty queue should raise a QueueEmpty error."""
    topic = unique("text")
    async with queue.connect(topic) as session:
        with pytest.raises(QueueEmpty):
            await session.receive()


async def test_queue_two_subscribers_both_receive(unique):
    """Every active subscriber should independently receive each published message.

    This verifies fan-out semantics: two separate Queue instances subscribed to
    the same channel both receive the message, rather than one consuming it and
    the other seeing nothing.
    """
    topic = unique("text")
    sub1 = Queue.from_url("memory://")
    sub2 = Queue.from_url("memory://")
    publisher = Queue.from_url("memory://")

    async with sub1.connect(topic), sub2.connect(topic):
        await publisher.publish(topic, "hello")
        msg1 = await sub1.receive(timeout=5)
        msg2 = await sub2.receive(timeout=5)

    assert msg1 == "hello"
    assert msg2 == "hello"


async def test_queue_subscribe_two_topics_receives_from_both(unique):
    """A single subscriber connected to two topics should receive from either."""
    topic_a = unique("text")
    topic_b = unique("text")
    sub = Queue.from_url("memory://")
    publisher = Queue.from_url("memory://")

    async with sub.connect(topic_a), sub.connect(topic_b):
        await publisher.publish(topic_a, "from-a")
        await publisher.publish(topic_b, "from-b")
        msg1 = await sub.receive(timeout=5)
        msg2 = await sub.receive(timeout=5)

    assert {msg1, msg2} == {"from-a", "from-b"}

    """A subscriber that connects after a publish should not receive that message.

    This matches Redis pub/sub behaviour: messages are only delivered to
    subscribers that are active at the moment of publishing.
    """
    topic = unique("text")
    publisher = Queue.from_url("memory://")
    late_sub = Queue.from_url("memory://")

    await publisher.publish(topic, "early")

    async with late_sub.connect(topic):
        with pytest.raises(QueueEmpty):
            await late_sub.receive()
