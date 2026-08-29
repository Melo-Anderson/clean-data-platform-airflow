from __future__ import annotations

import logging
import uuid
from datetime import UTC, datetime
from functools import lru_cache
from typing import Any

import httpx

from app.config import get_settings
from app.domain.pipelines.pipeline_run import PipelineRun

logger = logging.getLogger(__name__)


def _format_datetime(value: Any) -> str | None:
    """Format a datetime to ISO 8601 string."""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, str):
        return value
    return None


def _serialize_pipeline_run_file(f: Any) -> dict[str, Any]:
    """Serialize a physical file record for HTTP transport."""
    if isinstance(f, dict):
        return {
            "id": f.get("id") or str(uuid.uuid4()),
            "file_path": f.get("file_path", ""),
            "file_name": f.get("file_name", ""),
            "file_size_bytes": f.get("file_size_bytes", 0),
            "mtime": _format_datetime(f.get("mtime")) or datetime.now(tz=UTC).isoformat(),
            "hash_md5": f.get("hash_md5", ""),
            "status": f.get("status", "PROCESSED"),
            "processed_at": _format_datetime(f.get("processed_at")),
        }

    return {
        "id": getattr(f, "id", None) or str(uuid.uuid4()),
        "file_path": getattr(f, "file_path", ""),
        "file_name": getattr(f, "file_name", ""),
        "file_size_bytes": getattr(f, "file_size_bytes", 0),
        "mtime": _format_datetime(getattr(f, "mtime", None)) or datetime.now(tz=UTC).isoformat(),
        "hash_md5": getattr(f, "hash_md5", ""),
        "status": getattr(f, "status", "PROCESSED"),
        "processed_at": _format_datetime(getattr(f, "processed_at", None)),
    }


def _serialize_pipeline_run(run: dict[str, Any] | PipelineRun) -> dict[str, Any]:
    """Serialize a PipelineRun entity or dictionary for HTTP transport."""
    if isinstance(run, dict):
        raw_files = run.get("files") or []
        files_data = [_serialize_pipeline_run_file(f) for f in raw_files]
        return {
            "id": run.get("id") or str(uuid.uuid4()),
            "pipeline_id": run.get("pipeline_id", ""),
            "pipeline_name": run.get("pipeline_name", ""),
            "pipeline_type": run.get("pipeline_type", "ingestion"),
            "dag_run_id": run.get("dag_run_id", "unknown"),
            "status": run.get("status", "success"),
            "started_at": _format_datetime(run.get("started_at"))
            or datetime.now(tz=UTC).isoformat(),
            "finished_at": _format_datetime(run.get("finished_at")),
            "failed_task": run.get("failed_task"),
            "optional_failures": run.get("optional_failures") or [],
            "quality_violations": run.get("quality_violations") or [],
            "metrics": run.get("metrics") or {},
            "sla_minutes": run.get("sla_minutes", 90),
            "sla_breached": run.get("sla_breached", False),
            "files": files_data,
        }

    files = getattr(run, "files", []) or []
    return {
        "id": run.id,
        "pipeline_id": run.pipeline_id,
        "pipeline_name": run.pipeline_name,
        "pipeline_type": run.pipeline_type,
        "dag_run_id": run.dag_run_id,
        "status": run.status.value,
        "started_at": _format_datetime(run.started_at),
        "finished_at": _format_datetime(run.finished_at),
        "failed_task": run.failed_task,
        "optional_failures": run.optional_failures or [],
        "quality_violations": run.quality_violations or [],
        "metrics": run.metrics or {},
        "sla_minutes": run.sla_minutes,
        "sla_breached": run.sla_breached,
        "files": [_serialize_pipeline_run_file(f) for f in files],
    }


class PlatformApiClient:
    """HTTP Client adapter for Airflow callbacks and platform integration."""

    def __init__(self, base_url: str | None = None, timeout: float = 10.0) -> None:
        settings = get_settings()
        self._base_url = (base_url or settings.platform_api_url).rstrip("/")
        self._timeout = timeout

    def _get_client(self) -> httpx.Client:
        return httpx.Client(base_url=self._base_url, timeout=self._timeout)

    def resolve_vault_secrets(self, credential_ref: str) -> dict[str, Any]:
        """Resolve credentials from Vault or return empty dict for unauthenticated endpoints."""
        if not credential_ref or credential_ref in ("vault/none", "none"):
            return {}
        try:
            import asyncio
            import concurrent.futures

            from app.infrastructure.adapters.secrets.bao_secret_manager_adapter import (
                BaoSecretManagerAdapter,
            )

            settings = get_settings()
            adapter = BaoSecretManagerAdapter(
                vault_url=settings.vault_url,
                vault_token=settings.vault_token,
            )
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(asyncio.run, adapter.resolve(credential_ref)).result()
            return asyncio.run(adapter.resolve(credential_ref))
        except Exception as exc:
            logger.warning("Could not resolve secret %s from Vault: %s", credential_ref, exc)
            return {"token": "stub-token"}

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
        payload = _serialize_pipeline_run(run)
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
    return PlatformApiClient()
