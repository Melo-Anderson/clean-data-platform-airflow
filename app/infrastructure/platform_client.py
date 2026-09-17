from __future__ import annotations

import logging
from datetime import datetime
from functools import lru_cache
from typing import Any

import httpx

from app.config import get_settings
from app.domain.pipelines.pipeline_run import PipelineRun
from app.infrastructure.mappers.pipeline_run_mapper import serialize_pipeline_run

logger = logging.getLogger(__name__)


class PlatformApiClient:
    """HTTP Client adapter for Airflow callbacks and platform integration."""

    def __init__(
        self,
        base_url: str | None = None,
        vault_url: str | None = None,
        vault_token: str | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._base_url = (base_url or "").rstrip("/")
        self._vault_url = vault_url
        self._vault_token = vault_token
        self._timeout = timeout

    def _get_client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base_url, timeout=self._timeout)

    def pipeline_succeeded_on(
        self,
        pipeline_id: str,
        require_same_day: bool,
        logical_date: datetime,
        dependency_type: str,
    ) -> bool:
        """Check if upstream pipeline succeeded on or before the execution date."""
        try:
            with self._get_client() as client:
                params = {
                    "require_same_day": str(require_same_day).lower(),
                    "logical_date": logical_date.isoformat(),
                    "dependency_type": dependency_type,
                }
                res = client.get(f"/v1/pipelines/{pipeline_id}/runs/latest", params=params)
                if res.status_code == 200:
                    data = res.json()
                    return bool(data.get("success", False))
                logger.warning(
                    "Failed querying pipeline status (%s): %s", res.status_code, res.text
                )
                return False
        except Exception as exc:
            logger.warning("Error checking pipeline status for %s: %s", pipeline_id, exc)
            return False

    def emit_raw_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        """Emit raw ingestion lineage record."""
        payload = {
            "pipeline_id": pipeline_id,
            "source_object_ids": source_object_ids,
            "destination_object_ids": destination_object_ids,
            "schema_path": schema_path,
        }
        try:
            with self._get_client() as client:
                res = client.post("/v1/lineage/raw", json=payload)
                if res.status_code not in (200, 201):
                    logger.warning(
                        "Failed emitting raw lineage (%s): %s", res.status_code, res.text
                    )
        except Exception as exc:
            logger.warning("Could not emit raw lineage: %s", exc)

    def update_freshness_status(
        self,
        pipeline_id: str,
        destination_object_ids: list[str],
    ) -> None:
        """Update freshness tracking metadata for destination objects."""
        payload = {
            "pipeline_id": pipeline_id,
            "destination_object_ids": destination_object_ids,
        }
        try:
            with self._get_client() as client:
                res = client.post("/v1/lineage/freshness", json=payload)
                if res.status_code not in (200, 201):
                    logger.warning(
                        "Failed updating freshness status (%s): %s", res.status_code, res.text
                    )
        except Exception as exc:
            logger.warning("Could not update freshness status: %s", exc)

    def emit_etl_lineage(
        self,
        pipeline_id: str,
        transform_ref: str,
        schema_path: str | None,
    ) -> None:
        """Emit ETL transformation lineage."""
        payload = {
            "pipeline_id": pipeline_id,
            "transform_ref": transform_ref,
            "schema_path": schema_path,
        }
        try:
            with self._get_client() as client:
                res = client.post("/v1/lineage/etl", json=payload)
                if res.status_code not in (200, 201):
                    logger.warning(
                        "Failed emitting ETL lineage (%s): %s", res.status_code, res.text
                    )
        except Exception as exc:
            logger.warning("Could not emit ETL lineage: %s", exc)

    def emit_export_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        """Emit export lineage record."""
        payload = {
            "pipeline_id": pipeline_id,
            "source_object_ids": source_object_ids,
            "destination_object_ids": destination_object_ids,
            "schema_path": schema_path,
        }
        try:
            with self._get_client() as client:
                res = client.post("/v1/lineage/export", json=payload)
                if res.status_code not in (200, 201):
                    logger.warning(
                        "Failed emitting export lineage (%s): %s", res.status_code, res.text
                    )
        except Exception as exc:
            logger.warning("Could not emit export lineage: %s", exc)

    def execute_sensor_query(
        self,
        asset_id: str,
        query: str,
    ) -> Any:
        """Execute a sensor availability query against an asset."""
        try:
            with self._get_client() as client:
                res = client.post(f"/v1/assets/{asset_id}/sensors/query", json={"query": query})
                if res.status_code in (200, 201):
                    data = res.json()
                    return data.get("result", [])
                logger.warning("Sensor query failed (%s): %s", res.status_code, res.text)
                return []
        except Exception as exc:
            logger.warning("Could not execute sensor query for %s: %s", asset_id, exc)
            return []

    def upsert_pipeline_run(self, run: dict[str, Any] | PipelineRun) -> None:
        """Persist a PipelineRun execution record and its physical files via REST API."""
        payload = serialize_pipeline_run(run)
        pipeline_id = payload.get("pipeline_id")
        if not pipeline_id:
            logger.warning("Cannot upsert pipeline run without pipeline_id")
            return

        try:
            with self._get_client() as client:
                res = client.post(f"/v1/pipelines/{pipeline_id}/runs/record", json=payload)
                if res.status_code in (200, 201):
                    return
                logger.warning("Platform API responded %s: %s", res.status_code, res.text)
        except Exception as exc:
            logger.warning("Could not reach API at %s: %s", self._base_url, exc)

    def notify_failure(
        self,
        pipeline_id: str,
        failed_task: str,
        error_message: str | None = None,
    ) -> None:
        """Notify platform of pipeline execution failure."""
        payload = {"failed_task": failed_task, "error_message": error_message}
        try:
            with self._get_client() as client:
                res = client.post(
                    f"/v1/pipelines/{pipeline_id}/notifications/failure", json=payload
                )
                if res.status_code not in (200, 201):
                    logger.warning("Failed notifying failure (%s): %s", res.status_code, res.text)
        except Exception as exc:
            logger.warning("Could not notify failure: %s", exc)

    def get_latest_discovery_snapshot(
        self, asset_id: str, object_name: str | None = None
    ) -> dict[str, Any]:
        """Fetch latest discovered schema snapshot for an asset."""
        try:
            with self._get_client() as client:
                res = client.get(f"/v1/discovery/assets/{asset_id}/snapshot")
                if res.status_code == 200:
                    data = res.json()
                    if object_name and isinstance(data, dict) and "objects" in data:
                        obj_data = data["objects"].get(object_name, {})
                        return dict(obj_data) if isinstance(obj_data, dict) else {}
                    return data if isinstance(data, dict) else {}
        except Exception as exc:
            logger.warning("Could not fetch discovery snapshot for %s: %s", asset_id, exc)
        return {}

    def get_processed_hashes(self, pipeline_id: str) -> set[str]:
        """Fetch set of processed file MD5 checksums for a pipeline."""
        try:
            with self._get_client() as client:
                res = client.get(f"/v1/pipelines/{pipeline_id}/processed_hashes")
                if res.status_code == 200:
                    return set(res.json())
        except Exception as exc:
            logger.warning("Could not fetch processed hashes for %s: %s", pipeline_id, exc)
        return set()


@lru_cache(maxsize=1)
def get_platform_client() -> PlatformApiClient:
    settings = get_settings()
    return PlatformApiClient(
        base_url=settings.platform_api_url,
        vault_url=settings.vault_url,
        vault_token=settings.vault_token,
    )
