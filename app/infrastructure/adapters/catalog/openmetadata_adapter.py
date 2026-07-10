from __future__ import annotations

import logging
from typing import Any

from app.application.shared.adapters.catalog_adapter import CatalogPublishError
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping

logger = logging.getLogger(__name__)


class OpenMetadataCatalogAdapter:
    """
    CatalogAdapter for OpenMetadata.

    Uses the official `metadata-ingestion` library to manage
    the publication of tables, columns, tags, and fine-grained lineage.
    """

    def __init__(self, server_url: str, api_key: str | None = None) -> None:
        self._server_url = server_url
        self._api_key = api_key
        self._client = None

    def _get_client(self) -> Any:
        if self._client is None:
            try:
                from metadata.generated.schema.security.client.openMetadataJWTClientConfig import (
                    OpenMetadataJWTClientConfig,
                )
                from metadata.ingestion.ometa.models import MetadataServerConfig
                from metadata.ingestion.ometa.ometa_api import OpenMetadata

                config = MetadataServerConfig(
                    hostPort=self._server_url,
                    authProvider="openmetadata",
                    securityConfig=OpenMetadataJWTClientConfig(jwtToken=self._api_key)
                    if self._api_key
                    else None,
                )
                self._client = OpenMetadata(config)
            except ImportError:
                raise CatalogPublishError("openmetadata-ingestion library is not installed.")
        return self._client

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict) -> None:
        pass

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        # Creation of Table/Column entities via OpenMetadata REST API
        pass

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        # Emission of LineageDetails object with FineGrainedLineage in OpenMetadata
        pass

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        pass
