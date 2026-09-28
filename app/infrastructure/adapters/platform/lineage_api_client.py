# app/infrastructure/adapters/platform/lineage_api_client.py
from __future__ import annotations

import logging

from app.infrastructure.adapters.platform._base_client import BasePlatformClient
from app.infrastructure.resilience.http_resilience import resilient_http

logger = logging.getLogger(__name__)


class LineageApiClient(BasePlatformClient):
    """Operacoes de lineage e freshness contra a Platform API."""

    @resilient_http(fallback=None, log_prefix="emit_raw_lineage")
    def emit_raw_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        payload = {
            "pipeline_id": pipeline_id,
            "source_object_ids": source_object_ids,
            "destination_object_ids": destination_object_ids,
            "schema_path": schema_path,
        }
        with self._get_client() as client:
            res = client.post("/v1/lineage/raw", json=payload)
            if res.status_code not in (200, 201):
                logger.warning("Failed emitting raw lineage (%s): %s", res.status_code, res.text)

    @resilient_http(fallback=None, log_prefix="update_freshness_status")
    def update_freshness_status(
        self,
        pipeline_id: str,
        destination_object_ids: list[str],
    ) -> None:
        payload = {
            "pipeline_id": pipeline_id,
            "destination_object_ids": destination_object_ids,
        }
        with self._get_client() as client:
            res = client.post("/v1/lineage/freshness", json=payload)
            if res.status_code not in (200, 201):
                logger.warning(
                    "Failed updating freshness status (%s): %s", res.status_code, res.text
                )

    @resilient_http(fallback=None, log_prefix="emit_etl_lineage")
    def emit_etl_lineage(
        self,
        pipeline_id: str,
        transform_ref: str,
        schema_path: str | None,
    ) -> None:
        payload = {
            "pipeline_id": pipeline_id,
            "transform_ref": transform_ref,
            "schema_path": schema_path,
        }
        with self._get_client() as client:
            res = client.post("/v1/lineage/etl", json=payload)
            if res.status_code not in (200, 201):
                logger.warning("Failed emitting ETL lineage (%s): %s", res.status_code, res.text)

    @resilient_http(fallback=None, log_prefix="emit_export_lineage")
    def emit_export_lineage(
        self,
        pipeline_id: str,
        source_object_ids: list[str],
        destination_object_ids: list[str],
        schema_path: str | None,
    ) -> None:
        payload = {
            "pipeline_id": pipeline_id,
            "source_object_ids": source_object_ids,
            "destination_object_ids": destination_object_ids,
            "schema_path": schema_path,
        }
        with self._get_client() as client:
            res = client.post("/v1/lineage/export", json=payload)
            if res.status_code not in (200, 201):
                logger.warning("Failed emitting export lineage (%s): %s", res.status_code, res.text)
