"""Amazon SQS queue implementation using SNS for pub/sub fan-out."""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from time import time
from typing import TYPE_CHECKING

from yarl import URL

from taraqueue import Queue, QueueEmpty

if TYPE_CHECKING:
    from types import ModuleType


@dataclass
class SQSQueue(Queue):
    """Queue backed by Amazon SNS (publish) and SQS (subscribe/receive)."""

    session: object = field(repr=False)
    region: str = "us-east-1"
    endpoint_url: str | None = None
    _subscriptions: dict[str, _Subscription] = field(default_factory=dict, repr=False)
    _aiobotocore: ModuleType | None = field(default=None, repr=False)

    @classmethod
    def from_url(cls, url: URL | str) -> SQSQueue:
        """Create an SQSQueue from a URL."""
        import aiobotocore.session

        url = URL(url)
        region = url.path.lstrip("/") or "us-east-1"

        endpoint_url = None
        if url.host:
            scheme = "https" if url.port == 443 else "http"
            port = url.port or 4566
            endpoint_url = f"{scheme}://{url.host}:{port}"

        session = aiobotocore.session.get_session()
        if url.user:
            session.set_credentials(url.user, url.password or "")

        return cls(
            session=session,
            region=region,
            endpoint_url=endpoint_url,
        )

    def _create_client(self, service: str):
        return self.session.create_client(
            service,
            region_name=self.region,
            endpoint_url=self.endpoint_url,
        )

    async def subscribe(self, topic: str) -> None:
        """See `Queue.subscribe`."""
        if topic in self._subscriptions:
            return

        async with self._create_client("sns") as sns:
            resp = await sns.create_topic(Name=topic)
            topic_arn = resp["TopicArn"]

        queue_name = f"taraqueue-{topic}-{uuid.uuid4().hex[:8]}"
        async with self._create_client("sqs") as sqs:
            resp = await sqs.create_queue(QueueName=queue_name)
            queue_url = resp["QueueUrl"]
            attrs = await sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])
            queue_arn = attrs["Attributes"]["QueueArn"]

            # Allow SNS to send messages to this SQS queue.
            policy = json.dumps({
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": "sqs:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                }],
            })
            await sqs.set_queue_attributes(QueueUrl=queue_url, Attributes={"Policy": policy})

        async with self._create_client("sns") as sns:
            resp = await sns.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=queue_arn,
            )
            subscription_arn = resp["SubscriptionArn"]
            # Request raw message delivery so we don't have to unwrap the SNS envelope.
            await sns.set_subscription_attributes(
                SubscriptionArn=subscription_arn,
                AttributeName="RawMessageDelivery",
                AttributeValue="true",
            )

        self._subscriptions[topic] = _Subscription(
            topic_arn=topic_arn,
            queue_url=queue_url,
            queue_name=queue_name,
            subscription_arn=subscription_arn,
        )

    async def unsubscribe(self, topic: str) -> None:
        """See `Queue.unsubscribe`."""
        sub = self._subscriptions.pop(topic, None)
        if sub is None:
            return

        async with self._create_client("sns") as sns:
            await sns.unsubscribe(SubscriptionArn=sub.subscription_arn)

        async with self._create_client("sqs") as sqs:
            await sqs.delete_queue(QueueUrl=sub.queue_url)

    async def receive(self, timeout=None) -> str:
        """See `Queue.receive`."""
        if not self._subscriptions:
            raise QueueEmpty("Queue is empty")

        wait_time = int(timeout) if timeout and int(timeout) > 0 else 0
        stop_time = time() + (float(timeout) if timeout else 0)

        while True:
            for sub in list(self._subscriptions.values()):
                async with self._create_client("sqs") as sqs:
                    resp = await sqs.receive_message(
                        QueueUrl=sub.queue_url,
                        MaxNumberOfMessages=1,
                        WaitTimeSeconds=min(wait_time, 20),
                    )
                messages = resp.get("Messages", [])
                if messages:
                    msg = messages[0]
                    async with self._create_client("sqs") as sqs:
                        await sqs.delete_message(
                            QueueUrl=sub.queue_url,
                            ReceiptHandle=msg["ReceiptHandle"],
                        )
                    return msg["Body"]

            if time() >= stop_time:
                raise QueueEmpty("Queue is empty")

    async def publish(self, topic: str, message: str) -> None:
        """See `Queue.publish`."""
        async with self._create_client("sns") as sns:
            resp = await sns.create_topic(Name=topic)
            topic_arn = resp["TopicArn"]
            await sns.publish(TopicArn=topic_arn, Message=message)


@dataclass(frozen=True)
class _Subscription:
    """Internal state for a single topic subscription."""

    topic_arn: str
    queue_url: str
    queue_name: str
    subscription_arn: str
