from __future__ import annotations

import logging

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping
from app.application.shared.adapters.catalog_adapter import CatalogPublishError

logger = logging.getLogger(__name__)


class NoopCatalogAdapter:
    """
    Silent CatalogAdapter. Used in unit and integration tests
    or in development environments where no external catalog is active.
    """

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict) -> None:
        logger.info(
            f"[Catalog NOOP] publish_asset asset_id={asset_id!r} name={name!r} state={state!r}"
        )

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        logger.info(
            f"[Catalog NOOP] publish_schema for asset={asset.name!r} "
            f"object={snapshot.object_name!r} with {len(snapshot.fields)} fields."
        )

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        logger.info(
            f"[Catalog NOOP] publish_lineage pipeline_id={mapping.pipeline_id!r} "
            f"mappings={len(mapping.column_mappings)} items."
        )

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        logger.info(
            f"[Catalog NOOP] update_policy_tags object_id={object_id!r} tags={policy_tags}."
        )
