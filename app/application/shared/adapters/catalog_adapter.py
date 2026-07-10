from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping


class CatalogPublishError(Exception):
    """
    Raised by a CatalogAdapter when metadata or lineage publication fails.
    Provides explicit error wrapping independent of the underlying implementation.
    """


@runtime_checkable
class CatalogAdapter(Protocol):
    """
    Interface/Protocol for metadata and lineage synchronization to external catalogs.
    Implementations (DataHub, OpenMetadata) reside in the infrastructure layer.
    """

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict) -> None:
        """
        Publishes the high-level asset (e.g. Dataset) to the catalog.
        """
        ...

    async def publish_schema(
        self,
        asset: DataAsset,
        snapshot: SchemaSnapshot,
    ) -> None:
        """
        Publishes the column structure and types of the DataObject to the catalog.
        Must be idempotent.
        """
        ...

    async def publish_lineage(
        self,
        mapping: LineageMapping,
    ) -> None:
        """
        Creates lineage edges (upstream -> downstream) in the catalog's graph.
        """
        ...

    async def update_policy_tags(
        self,
        object_id: str,
        policy_tags: dict[str, str],  # field_name -> policy_tag string
    ) -> None:
        """
        Updates sensitivity/governance tags for columns in the catalog.
        """
        ...
