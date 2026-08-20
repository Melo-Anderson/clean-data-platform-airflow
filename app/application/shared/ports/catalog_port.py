from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping
from app.domain.shared.exceptions import DomainException


class CatalogPublishError(DomainException):
    """Raised when metadata or lineage publication fails."""


@runtime_checkable
class CatalogPort(Protocol):
    """Interface/Port for metadata and lineage synchronization to external catalogs."""

    async def publish_asset(
        self, asset_id: str, name: str, state: str, metadata: dict[str, Any]
    ) -> None:
        """Publishes the high-level asset (e.g. Dataset) to the catalog."""
        ...

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        """Publishes the column structure and types of the DataObject to the catalog."""
        ...

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        """Creates lineage edges (upstream -> downstream) in the catalog's graph."""
        ...

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        """Updates sensitivity/governance tags for columns in the catalog."""
        ...


# Backward compatibility alias
CatalogAdapter = CatalogPort
