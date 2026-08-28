from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.shared.file_formats import normalize_file_format
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
    DatabaseSourceConfig,
    MongoSourceConfig,
    OmniBeamDestinationConfig,
    OmniBeamDlqConfig,
    OmniBeamFieldSchema,
    OmniBeamManifest,
    OmniBeamQualityConfig,
    OmniBeamQualityRule,
    OmniBeamSchemaWrapper,
    OmniBeamSecurityConfig,
    RestApiSourceConfig,
    SourceConfigUnion,
    StorageSourceConfig,
)

_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "integer": "int64",
    "bigint": "int64",
    "float": "float64",
    "double": "float64",
    "decimal": "decimal",
    "boolean": "bool",
    "date": "date",
    "timestamp": "timestamp",
    "json": "json",
}


def _map_to_omnibeam_type(normalized_type: str) -> str:
    return _TYPE_MAP.get(normalized_type.lower(), "string")


class OmniBeamManifestBuilder:
    """Compiles platform metadata entities and Discovery SchemaSnapshots

    into canonical OmniBeam JSON manifests for any endpoint type (Storage, Database, REST API, Mongo).
    """

    def build_fields_schema(
        self,
        snapshot: SchemaSnapshot
        | Sequence[SchemaField]
        | Sequence[dict[str, Any]]
        | dict[str, Any]
        | None,
    ) -> OmniBeamSchemaWrapper:
        """Converts Discovery SchemaSnapshot or field lists into OmniBeamFieldSchema wrapper."""
        if isinstance(snapshot, dict) and "fields" in snapshot:
            snapshot = snapshot["fields"]

        if isinstance(snapshot, SchemaSnapshot):
            snapshot = snapshot.fields

        fields: list[OmniBeamFieldSchema] = []
        if isinstance(snapshot, (list, tuple)):
            fields = [self._map_field(item) for item in snapshot]

        if not fields:
            fields.append(OmniBeamFieldSchema(name="id", type="string", nullable=True))

        return OmniBeamSchemaWrapper(fields=fields)

    def _map_field(self, item: Any) -> OmniBeamFieldSchema:
        """Map a single SchemaField or field dictionary to OmniBeamFieldSchema."""
        if isinstance(item, SchemaField):
            return OmniBeamFieldSchema(
                name=item.name,
                type=_map_to_omnibeam_type(item.normalized_type),
                nullable=item.nullable,
                scale=2 if item.normalized_type == "decimal" else None,
            )
        if isinstance(item, dict):
            norm_type = item.get("normalized_type") or item.get("type", "string")
            return OmniBeamFieldSchema(
                name=str(item.get("name", "")),
                type=_map_to_omnibeam_type(str(norm_type)),
                nullable=bool(item.get("nullable", True)),
                scale=item.get("scale", 2 if str(norm_type).lower() == "decimal" else None),
            )
        return OmniBeamFieldSchema(name="id", type="string", nullable=True)

    def build_storage_source(
        self,
        *,
        paths: list[str],
        snapshot: SchemaSnapshot | list[SchemaField] | list[dict[str, Any]],
        format: str = "csv",
        delimiter: str = ",",
        quote_char: str = '"',
        compression: str = "none",
    ) -> StorageSourceConfig:
        return StorageSourceConfig(
            type="storage",
            paths=paths,
            format=format,
            delimiter=delimiter,
            quote_char=quote_char,
            compression=compression,
            schema=self.build_fields_schema(snapshot),
        )

    def build_database_source(
        self,
        *,
        credential_ref: str,
        snapshot: SchemaSnapshot | list[SchemaField] | list[dict[str, Any]],
        table: str | None = None,
        query: str | None = None,
        partition_column: str | None = None,
        num_partitions: int = 1,
        watermark_column: str | None = None,
        watermark_value: str | None = None,
    ) -> DatabaseSourceConfig:
        return DatabaseSourceConfig(
            type="database",
            credential_ref=credential_ref,
            table=table,
            query=query,
            partition_column=partition_column,
            num_partitions=num_partitions,
            watermark_column=watermark_column,
            watermark_value=watermark_value,
            schema=self.build_fields_schema(snapshot),
        )

    def build_rest_api_source(
        self,
        *,
        base_url: str,
        path: str,
        snapshot: SchemaSnapshot | list[SchemaField] | list[dict[str, Any]],
        auth_type: str = "",
        pagination_strategy: str = "page_number",
    ) -> RestApiSourceConfig:
        return RestApiSourceConfig(
            type="rest_api",
            base_url=base_url,
            path=path,
            auth_type=auth_type,
            pagination_strategy=pagination_strategy,
            schema=self.build_fields_schema(snapshot),
        )

    def build_mongo_source(
        self,
        *,
        credential_ref: str,
        database: str,
        collection: str,
        snapshot: SchemaSnapshot | list[SchemaField] | list[dict[str, Any]],
        filter_json: str | None = None,
    ) -> MongoSourceConfig:
        return MongoSourceConfig(
            type="mongodb",
            credential_ref=credential_ref,
            database=database,
            collection=collection,
            filter_json=filter_json,
            schema=self.build_fields_schema(snapshot),
        )

    def build(
        self,
        pipeline_id: str,
        run_id: str,
        files: list[PipelineRunFile] | None = None,
        snapshot: SchemaSnapshot | list[SchemaField] | list[dict[str, Any]] | None = None,
        output_path: str = "",
        quarantine_path: str = "",
        runner: str = "direct",
        delimiter: str = ",",
        quote_char: str = '"',
        compression: str = "none",
        sensitive_fields: list[str] | None = None,
        quality_rules: list[dict[str, Any]] | None = None,
        source_config: SourceConfigUnion | None = None,
    ) -> OmniBeamManifest:
        """Constructs a canonical OmniBeam manifest from source configuration and Discovery metadata."""
        if source_config is not None:
            source_cfg = source_config
        else:
            file_paths = [f.file_path for f in (files or [])]
            fmt = normalize_file_format(file_paths[0]) if file_paths else "csv"
            source_cfg = self.build_storage_source(
                paths=file_paths,
                snapshot=snapshot or [],
                format=fmt,
                delimiter=delimiter,
                quote_char=quote_char,
                compression=compression,
            )

        dest_cfg = self._build_destination_config(output_path)
        dlq_cfg = self._build_dlq_config(quarantine_path)
        rules = [OmniBeamQualityRule(**r) for r in (quality_rules or [])]

        return OmniBeamManifest(
            pipeline_id=pipeline_id,
            run_id=run_id,
            pipeline_type="ingestion",
            runner=runner,
            source=source_cfg,
            destination=dest_cfg,
            dlq_config=dlq_cfg,
            quality_config=OmniBeamQualityConfig(rules=rules),
            security=OmniBeamSecurityConfig(sensitive_fields=sensitive_fields or []),
        )

    def _build_destination_config(self, output_path: str) -> OmniBeamDestinationConfig:
        return OmniBeamDestinationConfig(
            type="storage",
            output_path=output_path,
            output_format="parquet",
            compression="zstd",
            single_file=False,
            include_audit_columns=True,
        )

    def _build_dlq_config(self, quarantine_path: str) -> OmniBeamDlqConfig:
        return OmniBeamDlqConfig(
            enabled=True,
            quarantine_path=quarantine_path,
            max_error_percentage=5.0,
        )
