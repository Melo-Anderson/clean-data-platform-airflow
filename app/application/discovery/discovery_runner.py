from typing import Protocol

from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import Endpoint


class DiscoveryRunner(Protocol):
    """Protocol for runners that execute metadata discovery on target physical systems.

    Implementors connect to the target endpoint, inspect schemas, and map physical columns/types
    into the platform's agnostic SchemaSnapshot domain objects.
    """

    async def run(
        self,
        asset_id: str,
        scope_include: list[str],
        scope_exclude: list[str],
        endpoint: Endpoint,
    ) -> list[SchemaSnapshot]:
        """Inspects target endpoint objects, reflecting schema definitions.

        Args:
            asset_id: ID of the DataAsset being scanned.
            scope_include: Glob patterns of table/file names to include in reflection.
            scope_exclude: Glob patterns of table/file names to explicitly exclude.
            endpoint: Physical connection configuration and subtype info.

        Returns:
            A list of SchemaSnapshots, one for each reflected data object.
        """
        ...


class DiscoveryRunnerFactory(Protocol):
    """Factory interface resolving the correct DiscoveryRunner subtype for a given Endpoint."""

    def create(self, endpoint: Endpoint) -> DiscoveryRunner:
        """Instantiate a runner matching the endpoint's type (e.g. database, s3, sftp).

        Args:
            endpoint: Connection definition containing type attribute.

        Returns:
            An active DiscoveryRunner instance matching the connection type.
        """
        ...
