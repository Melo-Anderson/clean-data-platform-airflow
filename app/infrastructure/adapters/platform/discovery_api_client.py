# app/infrastructure/adapters/platform/discovery_api_client.py
from __future__ import annotations

import logging
from typing import Any

from app.infrastructure.adapters.platform._base_client import BasePlatformClient
from app.infrastructure.resilience.http_resilience import resilient_http

logger = logging.getLogger(__name__)


class DiscoveryApiClient(BasePlatformClient):
    """Operacoes de discovery de schemas contra a Platform API."""

    @resilient_http(fallback={}, log_prefix="get_latest_discovery_snapshot")
    def get_latest_discovery_snapshot(
        self, asset_name: str, object_name: str | None = None
    ) -> dict[str, Any]:
        with self._get_client() as client:
            res = client.get(f"/v1/discovery/assets/{asset_name}/snapshot")
            if res.status_code == 200:
                data = res.json()
                if object_name and isinstance(data, dict) and "objects" in data:
                    obj_data = data["objects"].get(object_name, {})
                    return dict(obj_data) if isinstance(obj_data, dict) else {}
                return data if isinstance(data, dict) else {}
        return {}
