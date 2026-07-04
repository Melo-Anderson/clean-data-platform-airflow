from __future__ import annotations

from app.application.shared.adapters.catalog_adapter import CatalogAdapter
from app.config import Settings
from app.infrastructure.adapters.catalog.database_catalog_adapter import DatabaseCatalogAdapter
from app.infrastructure.adapters.catalog.datahub_adapter import DataHubCatalogAdapter
from app.infrastructure.adapters.catalog.noop_adapter import NoopCatalogAdapter
from app.infrastructure.adapters.catalog.openmetadata_adapter import OpenMetadataCatalogAdapter
from app.infrastructure.persistence.database import get_session_factory


def get_catalog_adapter(settings: Settings) -> CatalogAdapter:
    """
    Factory that resolves the active CatalogAdapter from environment configuration.

    Follows the same pattern as get_secret_manager: zero hardcoded values,
    all configuration delegated to Settings (read from .env or environment variables).
    """
    adapter_name = settings.catalog_adapter.lower()

    if adapter_name == "database":
        return DatabaseCatalogAdapter(get_session_factory())

    if adapter_name == "datahub":
        if not settings.datahub_url:
            raise ValueError("PLATFORM_DATAHUB_URL must be set when using datahub adapter")
        return DataHubCatalogAdapter(gms_url=settings.datahub_url, token=settings.datahub_token or None)

    if adapter_name == "openmetadata":
        if not settings.openmetadata_url:
            raise ValueError("PLATFORM_OPENMETADATA_URL must be set when using openmetadata adapter")
        return OpenMetadataCatalogAdapter(
            server_url=settings.openmetadata_url,
            api_key=settings.openmetadata_api_key or None,
        )

    # Default: noop (safe for local dev and tests)
    return NoopCatalogAdapter()
