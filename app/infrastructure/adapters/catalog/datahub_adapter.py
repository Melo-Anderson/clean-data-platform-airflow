from __future__ import annotations

import logging
from typing import Any

from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.lineage.lineage_mapping import LineageMapping
from app.application.shared.adapters.catalog_adapter import CatalogPublishError

logger = logging.getLogger(__name__)


class DataHubCatalogAdapter:
    """
    CatalogAdapter for LinkedIn DataHub.

    Uses the `datahub-rest` library via metadata ingestion, emitting
    Metadata Change Proposals (MCP) through the DataHub GMS REST API.
    """

    def __init__(self, gms_url: str, token: str | None = None) -> None:
        self._gms_url = gms_url
        self._token = token
        # Lazy import of DataHub SDK to avoid delaying app startup
        self._emitter = None

    def _get_emitter(self) -> Any:
        if self._emitter is None:
            try:
                from datahub.emitter.rest_emitter import DatahubRestEmitter
                self._emitter = DatahubRestEmitter(gms_server=self._gms_url, token=self._token)
            except ImportError:
                raise CatalogPublishError("datahub library is not installed.")
        return self._emitter

    def _map_field_type(self, normalized_type: str) -> Any:
        from datahub.metadata.schema_classes import (
            StringTypeClass, NumberTypeClass, BooleanTypeClass, DateTypeClass
        )
        if normalized_type in ("integer", "bigint", "decimal", "float"):
            return NumberTypeClass()
        if normalized_type == "boolean":
            return BooleanTypeClass()
        if normalized_type in ("date", "timestamp"):
            return DateTypeClass()
        return StringTypeClass()

    def _build_schema_fields(self, snapshot: SchemaSnapshot) -> list[Any]:
        from datahub.metadata.schema_classes import SchemaFieldClass
        return [
            SchemaFieldClass(
                fieldPath=field.name,
                type=self._map_field_type(field.normalized_type),
                nativeDataType=field.source_type,
                nullable=field.nullable,
                description=field.description,
            )
            for field in snapshot.fields
        ]

    async def publish_asset(self, asset_id: str, name: str, state: str, metadata: dict) -> None:
        pass  # TODO: implement publishing Dataset to DataHub

    async def publish_schema(self, asset: DataAsset, snapshot: SchemaSnapshot) -> None:
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import SchemaMetadataClass

            urn = f"urn:li:dataset:(urn:li:dataPlatform:{snapshot.runner_type},{snapshot.object_id},PROD)"
            
            schema_metadata = SchemaMetadataClass(
                schemaName=snapshot.object_name,
                platform=f"urn:li:dataPlatform:{snapshot.runner_type}",
                version=0,
                hash="",
                fields=self._build_schema_fields(snapshot),
            )
            
            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType="UPSERT",
                entityUrn=urn,
                aspectName="schemaMetadata",
                aspect=schema_metadata,
            )
            self._get_emitter().emit(mcp)
            logger.info(f"Published schema to DataHub for dataset {urn}")
        except Exception as exc:
            logger.error(f"Failed to publish schema to DataHub for asset {asset.name}. Expected valid MCP generation. Error: {exc}", exc_info=True)
            raise CatalogPublishError(f"DataHub schema publish failed for asset_id={asset.id}") from exc

    def _build_fine_lineages(self, mapping: LineageMapping, src_urn: str, dest_urn: str) -> list[Any]:
        from datahub.metadata.schema_classes import FineGrainedLineageClass
        return [
            FineGrainedLineageClass(
                upstreamPeople=[],
                upstreams=[f"urn:li:schemaField:({src_urn},{col_map.source_column})"],
                downstreams=[f"urn:li:schemaField:({dest_urn},{col_map.destination_column})"],
                confidenceScore=1.0,
                transformationOperation=col_map.transformation_expression,
            )
            for col_map in mapping.column_mappings
        ]

    async def publish_lineage(self, mapping: LineageMapping) -> None:
        # Fine-grained column lineage in DataHub is emitted via UpstreamLineageClass aspect.
        try:
            from datahub.emitter.mcp import MetadataChangeProposalWrapper
            from datahub.metadata.schema_classes import UpstreamLineageClass, UpstreamClass

            dest_urn = f"urn:li:dataset:(urn:li:dataPlatform:platform,{mapping.destination_object_id},PROD)"
            src_urn = f"urn:li:dataset:(urn:li:dataPlatform:platform,{mapping.source_object_id},PROD)"

            upstream = UpstreamClass(dataset=src_urn, type="TRANSFORMED")
            
            upstream_lineage = UpstreamLineageClass(
                upstreams=[upstream],
                fineGrainedLineages=self._build_fine_lineages(mapping, src_urn, dest_urn),
            )

            mcp = MetadataChangeProposalWrapper(
                entityType="dataset",
                changeType="UPSERT",
                entityUrn=dest_urn,
                aspectName="upstreamLineage",
                aspect=upstream_lineage,
            )
            self._get_emitter().emit(mcp)
            logger.info(f"Published fine-grained column lineage to DataHub for destination {dest_urn}")
        except Exception as exc:
            logger.error(f"Failed to publish lineage to DataHub for pipeline {mapping.pipeline_id}. Error: {exc}", exc_info=True)
            raise CatalogPublishError(f"DataHub lineage publish failed for pipeline_id={mapping.pipeline_id}") from exc

    async def update_policy_tags(self, object_id: str, policy_tags: dict[str, str]) -> None:
        # DataHub uses Glossaries or Tags to represent Policy Tags / Classifications.
        # Production implementation sends MCPs of schemaFieldEditableProperties to add terms.
        pass
