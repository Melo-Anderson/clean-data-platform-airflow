from __future__ import annotations

from typing import Any


class OpenMetadataCatalogAdapter:
    """OpenMetadata SDK integration. Configure via PLATFORM_OPENMETADATA_URL and PLATFORM_OPENMETADATA_TOKEN."""

    def __init__(self, base_url: str, token: str) -> None:
        self._base_url = base_url
        self._token = token

    async def publish_asset(
        self, asset_id: str, name: str, state: str, metadata: dict[str, Any]
    ) -> None:
        raise NotImplementedError("OpenMetadataCatalogAdapter.publish_asset not yet implemented")

    async def publish_lineage(
        self, source_object_id: str, destination_object_id: str, pipeline_id: str
    ) -> None:
        raise NotImplementedError

    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None:
        raise NotImplementedError
