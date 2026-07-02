from __future__ import annotations

from datetime import datetime
from functools import lru_cache
from typing import Any


class PlatformApiClient:
    """Stub for Task 10"""

    def pipeline_succeeded_on(
        self,
        pipeline_id: str,
        require_same_day: bool,
        logical_date: datetime,
        dependency_type: str,
    ) -> bool:
        return True

    def emit_raw_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        pass

    def update_freshness_status(
        self,
        pipeline_id: str,
        destination_object_ids: list[str],
    ) -> None:
        pass

    def emit_etl_lineage(
        self,
        pipeline_id: str,
        transform_ref: str,
        schema_path: str | None,
    ) -> None:
        pass

    def emit_export_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        pass

    def execute_sensor_query(
        self,
        asset_id: str,
        query: str,
    ) -> Any:
        return [{"sensor": "ok"}]


@lru_cache(maxsize=1)
def get_platform_client() -> PlatformApiClient:
    return PlatformApiClient()
