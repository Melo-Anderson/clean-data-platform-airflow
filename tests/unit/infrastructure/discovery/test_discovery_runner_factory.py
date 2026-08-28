from __future__ import annotations

from unittest.mock import MagicMock

from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.discovery.discovery_runner_factory import DiscoveryRunnerFactoryImpl
from app.infrastructure.discovery.filesystem_runner import FileSystemDiscoveryRunner


def test_factory_creates_filesystem_runner() -> None:
    mock_secrets = MagicMock()
    factory = DiscoveryRunnerFactoryImpl(secret_manager=mock_secrets)
    endpoint = FileSystemEndpoint(
        id="ep-1",
        name="local-files",
        credential_ref=CredentialReference("vault/storage"),
        root_path="/tmp/landing",
    )
    runner = factory.create(endpoint)
    assert isinstance(runner, FileSystemDiscoveryRunner)
