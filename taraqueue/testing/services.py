"""Service fixtures."""

from functools import partial
from pathlib import Path

import pytest

from taraqueue.testing.compose import ComposeServer


@pytest.fixture(scope="session")
def project():
    return "test"


@pytest.fixture(scope="session")
def env_vars(project):
    """Environment variables for the services."""
    return {
        "COMPOSE_PROJECT_NAME": project,
        "REDISPASS": "test",
    }


@pytest.fixture(scope="session")
def env_file(env_vars, request):
    """Environment file containing `env_vars`.

    Cached for troubleshooting purposes.
    """
    env_file = request.config.cache.makedir("compose") / "env"
    with env_file.open("w") as f:
        for k, v in env_vars.items():
            f.write(f"{k}={v}\n")

    return env_file


@pytest.fixture(scope="session")
def compose_files(request):
    directory = Path(request.config.rootdir)
    filenames = ["docker-compose.yml", "compose.yaml", "compose.yml"]
    while True:
        for filename in filenames:
            path = directory / filename
            if path.exists():
                all_files = directory.glob(f"{path.stem}.*")
                ordered_files = sorted(all_files, key=lambda p: len(p.name))
                return list(ordered_files)

        if directory == directory.parent:
            raise FileNotFoundError("Docker compose file not found")

        directory = directory.parent


@pytest.fixture(scope="session")
def compose_server(project, env_file, compose_files, process):
    return partial(
        ComposeServer,
        project=project,
        env_file=env_file,
        compose_files=compose_files,
        process=process,
    )


@pytest.fixture(scope="session")
def redis_service(compose_server):
    """Redis service fixture."""
    server = compose_server("Ready to accept connections tcp")
    with server.run("redis") as service:
        yield service


@pytest.fixture(scope="session")
def redis_client(redis_service, env_vars):
    """Redis client to the service fixture."""
    from redis import StrictRedis

    return StrictRedis(
        host=redis_service.ip,
        port=6379,
        decode_responses=True,
        db=0,
        password=env_vars["REDISPASS"],
    )
