"""Queue fixtures."""

import pytest
from yarl import URL

from taraqueue import Queue


@pytest.fixture
def memory_queue():
    """Memory queue fixture."""
    url = URL.build(scheme="memory")
    return Queue.from_url(url)


@pytest.fixture
def redis_queue(redis_service, taraqueue_env_vars):
    """Redis queue fixture."""
    url = URL.build(
        scheme="redis",
        host=redis_service.ip,
        port=6379,
        password=taraqueue_env_vars["REDIS_PASSWORD"],
    )
    return Queue.from_url(url)


@pytest.fixture
def sqs_queue(localstack_service):
    """SQS queue fixture backed by LocalStack."""
    url = URL.build(
        scheme="sqs",
        host=localstack_service.ip,
        port=4566,
    )
    return Queue.from_url(url)


@pytest.fixture(
    params=[
        "memory_queue",
        "redis_queue",
        "sqs_queue",
    ],
)
def queue(request):
    """Queue fixture."""
    return request.getfixturevalue(request.param)
