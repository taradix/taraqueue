"""Integration tests for the SQS queue."""

import pytest

from taraqueue import Queue, QueueEmpty


async def test_sqs_sns_topic_created_on_subscribe(sqs_queue, unique):
    """Subscribing should create the SNS topic and SQS queue in LocalStack."""
    topic = unique("text")
    await sqs_queue.subscribe(topic)
    try:
        assert topic in sqs_queue._subscriptions
        sub = sqs_queue._subscriptions[topic]
        assert sub.topic_arn
        assert sub.queue_url
        assert sub.subscription_arn
    finally:
        await sqs_queue.unsubscribe(topic)


async def test_sqs_unsubscribe_cleans_up(sqs_queue, unique):
    """Unsubscribing should remove the internal subscription state."""
    topic = unique("text")
    await sqs_queue.subscribe(topic)
    await sqs_queue.unsubscribe(topic)
    assert topic not in sqs_queue._subscriptions


async def test_sqs_subscribe_idempotent(sqs_queue, unique):
    """Subscribing twice to the same topic should not create a duplicate."""
    topic = unique("text")
    await sqs_queue.subscribe(topic)
    try:
        sub1 = sqs_queue._subscriptions[topic]
        await sqs_queue.subscribe(topic)
        sub2 = sqs_queue._subscriptions[topic]
        assert sub1 is sub2
    finally:
        await sqs_queue.unsubscribe(topic)


async def test_sqs_receive_empty_no_subscriptions(sqs_queue):
    """Receiving with no subscriptions should raise QueueEmpty."""
    with pytest.raises(QueueEmpty):
        await sqs_queue.receive()


async def test_sqs_fan_out(localstack_service, unique):
    """Two SQS subscribers should both receive the same published message."""
    from yarl import URL

    url = URL.build(scheme="sqs", host=localstack_service.ip, port=4566)
    sub1 = Queue.from_url(url)
    sub2 = Queue.from_url(url)
    publisher = Queue.from_url(url)

    topic = unique("text")
    async with sub1.connect(topic), sub2.connect(topic):
        await publisher.publish(topic, "fanout-msg")
        msg1 = await sub1.receive(timeout=10)
        msg2 = await sub2.receive(timeout=10)

    assert msg1 == "fanout-msg"
    assert msg2 == "fanout-msg"


async def test_sqs_late_subscriber_misses_message(localstack_service, unique):
    """A subscriber that connects after publish should not receive that message."""
    from yarl import URL

    url = URL.build(scheme="sqs", host=localstack_service.ip, port=4566)
    publisher = Queue.from_url(url)
    late_sub = Queue.from_url(url)

    topic = unique("text")
    await publisher.publish(topic, "early")

    async with late_sub.connect(topic):
        with pytest.raises(QueueEmpty):
            await late_sub.receive()


async def test_sqs_from_url_with_credentials():
    """from_url should parse credentials and endpoint from the URL."""
    q = Queue.from_url("sqs://mykey:mysecret@localhost:4566/eu-west-1")
    assert q.region == "eu-west-1"
    assert q.endpoint_url == "http://localhost:4566"
