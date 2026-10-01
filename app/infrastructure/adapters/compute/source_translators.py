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
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
    DatabaseSourceConfig,
    OmniBeamApiAuth,
    OmniBeamApiPagination,
    OmniBeamPartitionConfig,
    OmniBeamPoolConfig,
    RestApiSourceConfig,
    SourceConfigUnion,
    StorageSourceConfig,
)


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
    """Translates relational database source configuration matching OmniBeam §4.2."""

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
    ) -> DatabaseSourceConfig:
        if dto is None:
            dto = DatabaseSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        partition_cfg = OmniBeamPartitionConfig(
            partition_column=dto.partition_column,
            batch_size=dto.batch_size,
            num_partitions=dto.num_partitions,
        )
        pool_cfg = OmniBeamPoolConfig(
            max_open_conns=dto.max_open_conns,
            max_idle_conns=dto.max_idle_conns,
            conn_max_lifetime_s=dto.conn_max_lifetime_s,
        )

        return builder.build_database_source(
            driver=dto.driver,  # type: ignore[arg-type]
            table=dto.table,
            query=dto.query,
            credential_ref=dto.credential_ref,
            connection_uri=dto.connection_uri,
            database=dto.database,
            query_filter=dto.query_filter,
            columns=dto.columns,
            flatten_nested=dto.flatten_nested,
            partition_config=partition_cfg,
            pool_config=pool_cfg,
            snapshot=dto.snapshot_fields,
        )


class MongoManifestTranslator(SourceManifestTranslator):
    """Translates NoSQL MongoDB configuration matching OmniBeam §4.2."""

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
    ) -> DatabaseSourceConfig:
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
            flatten_nested=dto.flatten_nested,
            filter_json=dto.filter_json,
            snapshot=dto.snapshot_fields,
        )


class RestApiManifestTranslator(SourceManifestTranslator):
    """Translates REST API configuration matching OmniBeam §4.3."""

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
    ) -> RestApiSourceConfig:
        if dto is None:
            dto = RestApiSourceDTO.from_inputs(
                object_name=object_name,
                config=config or {},
                creds=creds or {},
                snapshot_fields=snapshot_fields or [],
                pipeline_id=pipeline_id,
            )

        pagination = OmniBeamApiPagination(
            type=dto.pagination_type,  # type: ignore[arg-type]
            page_param=dto.page_param,
            size_param=dto.size_param,
            page_size=dto.page_size,
            initial_page=dto.initial_page,
            max_pages_limit=dto.max_pages_limit,
            cursor_param=dto.cursor_param,
            next_cursor_path=dto.next_cursor_path,
            total_count_path=dto.total_count_path,
            has_more_path=dto.has_more_path,
        )

        auth = (
            OmniBeamApiAuth(
                type=dto.auth_type,  # type: ignore[arg-type]
                token_ref=dto.token_ref,
                api_key_header=dto.api_key_header,
                api_key_query=dto.api_key_query,
                username_ref=dto.username_ref,
                password_ref=dto.password_ref,
                token_url=dto.token_url,
                client_id_ref=dto.client_id_ref,
                client_secret_ref=dto.client_secret_ref,
                scopes=dto.scopes,
            )
            if dto.auth_type
            else None
        )

        return builder.build_rest_api_source(
            base_url=dto.base_url,
            endpoint=dto.endpoint,
            pagination=pagination,
            auth=auth,
            http_method=dto.http_method,  # type: ignore[arg-type]
            headers=dto.headers,
            query_params=dto.query_params,
            skip_tls_verify=dto.skip_tls_verify,
            records_path=dto.records_path,
            field_mapping=dto.field_mapping,
            snapshot=dto.snapshot_fields,
        )


class StorageManifestTranslator(SourceManifestTranslator):
    """Translates file storage configuration matching OmniBeam §4.1."""

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
    ) -> StorageSourceConfig:
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
            format=dto.format,  # type: ignore[arg-type]
            delimiter=dto.delimiter,
            quote_char=dto.quote_char,
            multiline=dto.multiline,
            charset=dto.charset,
            compression=dto.compression,
            chunk_size_bytes=dto.chunk_size_bytes,
        )


MANIFEST_TRANSLATORS: dict[str, SourceManifestTranslator] = {
    "database": DatabaseManifestTranslator(),
    "mongodb": MongoManifestTranslator(),
    "rest_api": RestApiManifestTranslator(),
    "storage": StorageManifestTranslator(),
}
