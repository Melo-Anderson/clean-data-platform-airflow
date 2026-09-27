# app/infrastructure/adapters/compute/source_translators.py
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from app.infrastructure.adapters.compute.source_dtos import (
    DatabaseSourceDTO,
    MongoSourceDTO,
    RestApiSourceDTO,
    StorageSourceDTO,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_builder import OmniBeamManifestBuilder
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import SourceConfigUnion


class SourceManifestTranslator(ABC):
    """Abstract strategy for translating strongly typed source DTOs into OmniBeam source configs."""

    @abstractmethod
    def translate(
        self,
        *,
        object_name: str = "",
        creds: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        snapshot_fields: list[dict[str, Any]] | None = None,
        builder: OmniBeamManifestBuilder,
        pipeline_id: str = "",
        dto: Any = None,
    ) -> SourceConfigUnion:
        """Translate source metadata into a strongly-typed OmniBeam SourceConfig."""
        pass


class DatabaseManifestTranslator(SourceManifestTranslator):
    """Translates database source configuration where object_name represents a database table."""

    def translate(
        self,
        *,
        object_name: str = "",
        creds: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        snapshot_fields: list[dict[str, Any]] | None = None,
        builder: OmniBeamManifestBuilder,
        pipeline_id: str = "",
        dto: DatabaseSourceDTO | None = None,
    ) -> SourceConfigUnion:
        if dto is None:
            dto = DatabaseSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        return builder.build_database_source(
            driver=dto.driver,
            table=dto.table,
            credential_ref=dto.credential_ref,
            connection_uri=dto.connection_uri,
            database=dto.database,
            snapshot=dto.snapshot_fields,
            query=dto.query,
            partition_column=dto.partition_column,
            num_partitions=dto.num_partitions,
            watermark_column=dto.watermark_column,
            watermark_value=dto.watermark_value,
        )


class MongoManifestTranslator(SourceManifestTranslator):
    """Translates NoSQL configuration where object_name represents a MongoDB collection."""

    def translate(
        self,
        *,
        object_name: str = "",
        creds: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        snapshot_fields: list[dict[str, Any]] | None = None,
        builder: OmniBeamManifestBuilder,
        pipeline_id: str = "",
        dto: MongoSourceDTO | None = None,
    ) -> SourceConfigUnion:
        if dto is None:
            dto = MongoSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        return builder.build_mongo_source(
            collection=dto.collection,
            credential_ref=dto.credential_ref,
            database=dto.database,
            connection_uri=dto.connection_uri,
            snapshot=dto.snapshot_fields,
            filter_json=dto.filter_json,
        )


class RestApiManifestTranslator(SourceManifestTranslator):
    """Translates REST API configuration where object_name represents an API endpoint/resource."""

    def translate(
        self,
        *,
        object_name: str = "",
        creds: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        snapshot_fields: list[dict[str, Any]] | None = None,
        builder: OmniBeamManifestBuilder,
        pipeline_id: str = "",
        dto: RestApiSourceDTO | None = None,
    ) -> SourceConfigUnion:
        if dto is None:
            dto = RestApiSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        return builder.build_rest_api_source(
            base_url=dto.base_url,
            path=dto.endpoint,
            endpoint=dto.endpoint,
            snapshot=dto.snapshot_fields,
            auth_type=dto.auth_type,
            pagination_strategy=dto.pagination_strategy,
            records_path=dto.records_path,
        )


class StorageManifestTranslator(SourceManifestTranslator):
    """Translates file storage configuration where object_name represents a dataset/file pattern."""

    def translate(
        self,
        *,
        object_name: str = "",
        creds: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        snapshot_fields: list[dict[str, Any]] | None = None,
        builder: OmniBeamManifestBuilder,
        pipeline_id: str = "",
        dto: StorageSourceDTO | None = None,
    ) -> SourceConfigUnion:
        if dto is None:
            dto = StorageSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        return builder.build_storage_source(
            paths=dto.paths,
            snapshot=dto.snapshot_fields,
            format=dto.format,
            delimiter=dto.delimiter,
            quote_char=dto.quote_char,
            compression=dto.compression,
        )


MANIFEST_TRANSLATORS: dict[str, SourceManifestTranslator] = {
    "database": DatabaseManifestTranslator(),
    "mongodb": MongoManifestTranslator(),
    "rest_api": RestApiManifestTranslator(),
    "storage": StorageManifestTranslator(),
}
