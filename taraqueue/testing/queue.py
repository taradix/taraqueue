"""Queue fixtures."""

import subprocess
import sys
import time

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
def sqs_queue(moto_server):
    """SQS queue fixture backed by moto server."""
    url = URL.build(
        scheme="sqs",
        host="127.0.0.1",
        port=moto_server,
        user="testing",
        password="testing",
    )
    return Queue.from_url(url)


@pytest.fixture(scope="session")
def moto_server():
    """Start a moto standalone server for AWS service mocking."""
    import socket

    # Find a free port
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]

    proc = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-p", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the server to be ready
    for _ in range(50):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.connect(("127.0.0.1", port))
                break
        except OSError:
            time.sleep(0.1)
    else:
        proc.terminate()
        raise RuntimeError("moto server failed to start")

    yield port

    proc.terminate()
    proc.wait()


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
