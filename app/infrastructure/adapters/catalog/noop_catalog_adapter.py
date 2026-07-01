from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


class NoopCatalogAdapter:
    async def publish_asset(
        self, asset_id: str, name: str, state: str, metadata: dict[str, Any]
    ) -> None:
        logger.debug("NoopCatalogAdapter.publish_asset: %s state=%s", asset_id, state)

    async def publish_lineage(
        self, source_object_id: str, destination_object_id: str, pipeline_id: str
    ) -> None:
        logger.debug(
            "NoopCatalogAdapter.publish_lineage: %s -> %s", source_object_id, destination_object_id
        )

    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None:
        logger.debug("NoopCatalogAdapter.publish_schema_drift: %s", asset_id)
