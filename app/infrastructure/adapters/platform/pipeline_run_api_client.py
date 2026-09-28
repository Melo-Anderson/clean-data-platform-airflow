# app/infrastructure/adapters/platform/pipeline_run_api_client.py
from __future__ import annotations

import logging
from typing import Any

from app.domain.pipelines.pipeline_run import PipelineRun
from app.infrastructure.adapters.platform._base_client import BasePlatformClient
from app.infrastructure.mappers.pipeline_run_mapper import serialize_pipeline_run
from app.infrastructure.resilience.http_resilience import resilient_http

logger = logging.getLogger(__name__)


class PipelineRunApiClient(BasePlatformClient):
    """Operacoes de pipeline runs, monitoramento e notificacao de falhas."""

    @resilient_http(fallback=None, log_prefix="upsert_pipeline_run")
    def upsert_pipeline_run(self, run: dict[str, Any] | PipelineRun) -> None:
        payload = serialize_pipeline_run(run)
        pipeline_id = payload.get("pipeline_id")
        if not pipeline_id:
            logger.warning("Cannot upsert pipeline run without pipeline_id")
            return

        with self._get_client() as client:
            res = client.post(f"/v1/pipelines/{pipeline_id}/runs/record", json=payload)
            if res.status_code not in (200, 201):
                logger.warning("Platform API responded %s: %s", res.status_code, res.text)

    @resilient_http(fallback=None, log_prefix="notify_failure")
    def notify_failure(
        self,
        pipeline_id: str,
        failed_task: str,
        error_message: str | None = None,
    ) -> None:
        payload = {"failed_task": failed_task, "error_message": error_message}
        with self._get_client() as client:
            res = client.post(f"/v1/pipelines/{pipeline_id}/notifications/failure", json=payload)
            if res.status_code not in (200, 201):
                logger.warning("Failed notifying failure (%s): %s", res.status_code, res.text)

    @resilient_http(fallback=set(), log_prefix="get_processed_hashes")
    def get_processed_hashes(self, pipeline_id: str) -> set[str]:
        with self._get_client() as client:
            res = client.get(f"/v1/pipelines/{pipeline_id}/processed_hashes")
            if res.status_code == 200:
                return set(res.json())
        return set()
