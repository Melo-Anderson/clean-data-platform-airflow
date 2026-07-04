# app/infrastructure/discovery/discovery_runner_factory.py
from __future__ import annotations

from app.application.discovery.discovery_runner import DiscoveryRunner, DiscoveryRunnerFactory
from app.application.shared.secret_manager_port import SecretManagerPort
from app.domain.endpoints.endpoint import DatabaseEndpoint, Endpoint
from app.infrastructure.discovery.database_runner import DatabaseRunner


class DiscoveryRunnerFactoryImpl(DiscoveryRunnerFactory):
    """Creates the appropriate DiscoveryRunner for the given Endpoint type."""

    def __init__(self, secret_manager: SecretManagerPort) -> None:
        self._secret_manager = secret_manager

    def create(self, endpoint: Endpoint) -> DiscoveryRunner:
        if isinstance(endpoint, DatabaseEndpoint):
            return DatabaseRunner(secret_manager=self._secret_manager)
        raise NotImplementedError(
            f"No DiscoveryRunner registered for endpoint type: {type(endpoint).__name__!r}"
        )
