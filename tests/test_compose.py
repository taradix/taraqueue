"""Unit tests for the testing.compose module."""

from datetime import datetime
from unittest.mock import Mock, patch

from pytest_xdocker.process import ProcessData

from taraqueue.testing.compose import ComposeServer, ComposeService

INSPECT_DATA = {
    "Id": "abc123def456",
    "Config": {"Env": ["KEY=value", "OTHER=data"]},
    "NetworkSettings": {"Networks": {"bridge": {"IPAddress": "172.17.0.2"}}},
    "State": {"StartedAt": "2024-06-01T12:00:00+00:00"},
}


def test_compose_service_properties():
    """ComposeService properties should read values from docker inspect data."""
    service = ComposeService("test-redis-1")
    with patch("taraqueue.testing.compose.DockerContainer") as MockContainer:
        mock = Mock(inspect=INSPECT_DATA)
        MockContainer.return_value = mock

        assert service.container_id == "abc123def456"
        assert service.env == {"KEY": "value", "OTHER": "data"}
        assert service.ip == "172.17.0.2"
        assert isinstance(service.started_at, datetime)
        assert service.started_at.year == 2024


def test_compose_server_prepare_func():
    """ComposeServer repr and prepare_func should produce correct output."""
    server = ComposeServer(pattern="ready", project="myproj", process=Mock())
    controldir = Mock(basename="redis")
    result = server.prepare_func(controldir)

    assert isinstance(result, ProcessData)
    assert result.pattern == "ready"
    assert result.timeout == 180
