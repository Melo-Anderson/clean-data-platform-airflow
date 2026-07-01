from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class CatalogAdapter(Protocol):
    """Protocol for external catalog integration. Selected via PLATFORM_CATALOG_ADAPTER env var."""

    async def publish_asset(
        self, asset_id: str, name: str, state: str, metadata: dict[str, Any]
    ) -> None: ...
    async def publish_lineage(
        self, source_object_id: str, destination_object_id: str, pipeline_id: str
    ) -> None: ...
    async def publish_schema_drift(self, asset_id: str, drift_event: dict[str, Any]) -> None: ...
