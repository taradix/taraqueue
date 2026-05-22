"""Service fixtures."""

from pathlib import Path

import pytest

from taraqueue.testing.compose import ComposeServer


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
    env_file = request.config.cache.makedir("compose") / "env"
    with env_file.open("w") as f:
        for k, v in taraqueue_env_vars.items():
            f.write(f"{k}={v}\n")

    return env_file


@pytest.fixture(scope="session")
def taraqueue_compose_files(request):
    """Use the compose files from the project - not this library."""
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
def redis_service(process, taraqueue_env_file, taraqueue_compose_files):
    """Redis service fixture."""
    server = ComposeServer(
        pattern="Ready to accept connections tcp",
        env_file=taraqueue_env_file,
        compose_files=taraqueue_compose_files,
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


@pytest.fixture(scope="session")
def localstack_service(process, taraqueue_env_file, taraqueue_compose_files):
    """LocalStack service fixture."""
    server = ComposeServer(
        pattern="Ready.",
        env_file=taraqueue_env_file,
        compose_files=taraqueue_compose_files,
        process=process,
    )
    with server.run("localstack") as service:
        yield service
