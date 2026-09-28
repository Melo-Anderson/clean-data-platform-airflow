# app/infrastructure/adapters/platform/__init__.py
from __future__ import annotations

from functools import cache

from app.config import get_settings
from app.infrastructure.adapters.platform._base_client import BasePlatformClient
from app.infrastructure.adapters.platform.discovery_api_client import DiscoveryApiClient
from app.infrastructure.adapters.platform.lineage_api_client import LineageApiClient
from app.infrastructure.adapters.platform.pipeline_run_api_client import PipelineRunApiClient
from app.infrastructure.adapters.platform.sensor_api_client import SensorApiClient


@cache
def get_lineage_client() -> LineageApiClient:
    settings = get_settings()
    return LineageApiClient(base_url=settings.platform_api_url)


@cache
def get_pipeline_run_client() -> PipelineRunApiClient:
    settings = get_settings()
    return PipelineRunApiClient(base_url=settings.platform_api_url)


@cache
def get_sensor_client() -> SensorApiClient:
    settings = get_settings()
    return SensorApiClient(base_url=settings.platform_api_url)


@cache
def get_discovery_client() -> DiscoveryApiClient:
    settings = get_settings()
    return DiscoveryApiClient(base_url=settings.platform_api_url)


__all__ = [
    "BasePlatformClient",
    "DiscoveryApiClient",
    "LineageApiClient",
    "PipelineRunApiClient",
    "SensorApiClient",
    "get_discovery_client",
    "get_lineage_client",
    "get_pipeline_run_client",
    "get_sensor_client",
]
