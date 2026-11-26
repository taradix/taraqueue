"""Service fixtures."""

import pytest
from pytest_xdocker.compose import ComposeServer, find_compose_files


@pytest.fixture(scope="session")
def taraqueue_env_vars():
    """Environment variables for the services."""
    return {
        "COMPOSE_PROJECT_NAME": "test",
        "REDIS_PASSWORD": "test",
    }


@pytest.fixture(scope="session")
def taraqueue_env_file(taraqueue_env_vars, request):
    """Environment file containing `taraqueue_env_vars`.

    Cached for troubleshooting purposes.
    """
    env_file = request.config.cache.makedir("taraqueue") / "env"
    with env_file.open("w") as f:
        for k, v in taraqueue_env_vars.items():
            f.write(f"{k}={v}\n")

    return env_file


@pytest.fixture(scope="session")
def redis_service(process, taraqueue_env_file, request):
    """Redis service fixture."""
    server = ComposeServer(
        pattern="Ready to accept connections tcp",
        env_file=taraqueue_env_file,
        compose_files=find_compose_files(request.config.rootdir),
        process=process,
    )
    with server.run("redis") as service:
        yield service


@pytest.fixture(scope="session")
def redis_client(redis_service, taraqueue_env_vars):
    """Redis client to the service fixture."""
    from redis import StrictRedis

    return StrictRedis(
        host=redis_service.ip,
        port=6379,
        decode_responses=True,
        db=0,
        password=taraqueue_env_vars["REDIS_PASSWORD"],
    )
