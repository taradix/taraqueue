"""Integration tests for the LocalStack service."""


def test_localstack_service(localstack_service):
    """The LocalStack service should be reachable."""
    assert localstack_service.ip
