# app/infrastructure/adapters/platform/sensor_api_client.py
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from app.infrastructure.adapters.platform._base_client import BasePlatformClient
from app.infrastructure.resilience.http_resilience import resilient_http

logger = logging.getLogger(__name__)


class SensorApiClient(BasePlatformClient):
    """Operacoes de checagem de dependencias e sensores contra a Platform API."""

    @resilient_http(fallback=False, log_prefix="pipeline_succeeded_on")
    def pipeline_succeeded_on(
        self,
        pipeline_id: str,
        require_same_day: bool,
        logical_date: datetime,
        dependency_type: str,
    ) -> bool:
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
            logger.warning("Failed querying pipeline status (%s): %s", res.status_code, res.text)
            return False

    @resilient_http(fallback=[], log_prefix="execute_sensor_query")
    def execute_sensor_query(
        self,
        asset_id: str,
        query: str,
    ) -> Any:
        with self._get_client() as client:
            res = client.post(f"/v1/assets/{asset_id}/sensors/query", json={"query": query})
            if res.status_code in (200, 201):
                data = res.json()
                return data.get("result", [])
            logger.warning("Sensor query failed (%s): %s", res.status_code, res.text)
            return []
