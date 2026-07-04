from typing import Protocol

from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import Endpoint
from app.domain.objects.data_object import DataObject


class DiscoveryRunner(Protocol):
    """
    Protocol for runners that execute discovery on specific types of endpoints.
    """

    async def run(
        self,
        asset_id: str,
        objects: list[DataObject],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        """
        Connects to the endpoint, reflects metadata, returns one SchemaSnapshot per object.
        """
        ...


class DiscoveryRunnerFactory(Protocol):
    """Factory to create the appropriate DiscoveryRunner for an endpoint."""

    def create(self, endpoint: Endpoint) -> DiscoveryRunner:
        ...
