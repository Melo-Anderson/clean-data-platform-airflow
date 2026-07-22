from unittest.mock import MagicMock

from app.domain.endpoints.endpoint import DatabaseEndpoint, RestApiEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.discovery.database_runner import DatabaseRunner
from app.infrastructure.discovery.discovery_runner_factory import DiscoveryRunnerFactoryImpl


def make_database_endpoint() -> DatabaseEndpoint:
    return DatabaseEndpoint(
        id="ep-db-1",
        name="prod-db",
        credential_ref=CredentialReference(path="vault/prod-db"),
    )


def make_rest_api_endpoint() -> RestApiEndpoint:
    return RestApiEndpoint(
        id="ep-api-1",
        name="some-api",
        credential_ref=CredentialReference(path="vault/some-api"),
    )


def test_factory_creates_database_runner() -> None:
    secret_manager = MagicMock()
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    endpoint = make_database_endpoint()

    runner = factory.create(endpoint)

    assert isinstance(runner, DatabaseRunner)


def test_factory_creates_mongodb_runner() -> None:
    from app.domain.endpoints.endpoint import NoSqlEndpoint
    from app.infrastructure.discovery.mongodb_runner import MongoDbRunner

    secret_manager = MagicMock()
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    endpoint = NoSqlEndpoint(
        id="ep-mongo-1",
        name="prod-mongo",
        credential_ref=CredentialReference(path="vault/prod-mongo"),
    )

    runner = factory.create(endpoint)

    assert isinstance(runner, MongoDbRunner)


def test_factory_creates_rest_api_runner() -> None:
    from app.infrastructure.discovery.rest_api_runner import RestApiRunner

    secret_manager = MagicMock()
    factory = DiscoveryRunnerFactoryImpl(secret_manager=secret_manager)
    endpoint = make_rest_api_endpoint()

    runner = factory.create(endpoint)

    assert isinstance(runner, RestApiRunner)
