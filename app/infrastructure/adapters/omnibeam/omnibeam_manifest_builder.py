from __future__ import annotations

from collections.abc import Sequence
from typing import Any, cast

from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.pipelines.pipeline_run_file import PipelineRunFile
from app.domain.shared.file_formats import normalize_file_format
from app.domain.shared.platform_defaults import (
    DatabaseDefaults,
    DestinationDefaults,
    DlqDefaults,
    ExtractionDefaults,
    OmniBeamDefaults,
    RestApiDefaults,
    StorageDefaults,
)
from app.infrastructure.adapters.omnibeam.omnibeam_manifest_schema import (
    DatabaseSourceConfig,
    DestinationConfigUnion,
    OmniBeamApiAuth,
    OmniBeamApiAuthType,
    OmniBeamApiEndpointConfig,
    OmniBeamApiOptions,
    OmniBeamApiPagination,
    OmniBeamApiPaginationType,
    OmniBeamBigQueryDestinationConfig,
    OmniBeamBigQueryOptions,
    OmniBeamBigQueryWriteDisposition,
    OmniBeamDatabaseDriver,
    OmniBeamDecimalOverflow,
    OmniBeamDestinationFormat,
    OmniBeamDlqConfig,
    OmniBeamEncryptionConfig,
    OmniBeamFieldDataType,
    OmniBeamFieldSchema,
    OmniBeamFormatOptions,
    OmniBeamHttpMethod,
    OmniBeamManifest,
    OmniBeamPartitionConfig,
    OmniBeamPipelineType,
    OmniBeamPoolConfig,
    OmniBeamQualityConfig,
    OmniBeamQualityRule,
    OmniBeamResilienceConfig,
    OmniBeamRestApiDestinationConfig,
    OmniBeamRunnerType,
    OmniBeamSchemaWrapper,
    OmniBeamSecretsConfig,
    OmniBeamSecurityConfig,
    OmniBeamStorageDestinationConfig,
    OmniBeamStorageFormat,
    RestApiSourceConfig,
    SourceConfigUnion,
    StorageSourceConfig,
)

_TYPE_MAP: dict[str, OmniBeamFieldDataType] = {
    "string": "string",
    "integer": "int64",
    "int": "int64",
    "int32": "int64",
    "int64": "int64",
    "bigint": "int64",
    "float": "float64",
    "float32": "float64",
    "float64": "float64",
    "double": "float64",
    "number": "float64",
    "decimal": "decimal",
    "boolean": "bool",
    "bool": "bool",
    "date": "date",
    "timestamp": "timestamp",
    "json": "json",
    "bytes": "bytes",
}


def _map_to_omnibeam_type(normalized_type: str) -> OmniBeamFieldDataType:
    mapped = _TYPE_MAP.get(normalized_type.lower())
    if not mapped:
        raise ValueError(
            f"Unsupported type '{normalized_type}'. Allowed types: {sorted(_TYPE_MAP.keys())}"
        )
    return mapped


class OmniBeamManifestBuilder:
    """Compiles platform metadata and Discovery SchemaSnapshots into canonical

    OmniBeam JSON manifests strictly conforming to OmniBeam-Go (§2, §4, §5, §6, §7).
    Follows Clean Code principles: no silent fallbacks, no dummy schema fields,
    single source of truth for platform defaults.
    """

    def build_fields_schema(
        self,
        snapshot: SchemaSnapshot
        | Sequence[SchemaField]
        | Sequence[dict[str, Any]]
        | dict[str, Any]
        | None,
    ) -> OmniBeamSchemaWrapper | None:
        """Converts Discovery SchemaSnapshot or field lists into OmniBeamFieldSchema wrapper."""
        if snapshot is None:
            return None

        if isinstance(snapshot, dict) and "fields" in snapshot:
            snapshot = snapshot["fields"]

        if isinstance(snapshot, SchemaSnapshot):
            snapshot = snapshot.fields

        if not isinstance(snapshot, (list, tuple)):
            raise TypeError(
                f"Expected snapshot to be a SchemaSnapshot or Sequence of fields, got: {type(snapshot)}"
            )

        if not snapshot:
            return None

        fields = [self._map_field(item) for item in snapshot]
        return OmniBeamSchemaWrapper(fields=fields)

    def _map_field(self, item: Any) -> OmniBeamFieldSchema:
        """Maps a SchemaField or field dictionary to a strongly typed OmniBeamFieldSchema."""
        if isinstance(item, SchemaField):
            norm_type = _map_to_omnibeam_type(item.normalized_type)
            scale = item.extra.get("scale") if item.extra else None
            if norm_type == "decimal" and scale is None:
                scale = ExtractionDefaults.decimal_scale
            return OmniBeamFieldSchema(
                name=item.name,
                type=norm_type,
                nullable=item.nullable,
                scale=int(scale) if scale is not None else None,
                on_overflow=ExtractionDefaults.decimal_overflow,
            )
        if isinstance(item, dict):
            name = item.get("name")
            if not name:
                raise ValueError("Field dictionary must contain a non-empty 'name'.")
            raw_type = item.get("normalized_type")
            if not raw_type:
                raw_type = item.get("type")
            if not raw_type:
                raise ValueError(f"Field '{name}' must specify 'normalized_type' or 'type'.")
            norm_type = _map_to_omnibeam_type(str(raw_type))
            scale = item.get("scale")
            if norm_type == "decimal" and scale is None:
                scale = ExtractionDefaults.decimal_scale
            return OmniBeamFieldSchema(
                name=str(name),
                type=norm_type,
                nullable=bool(item.get("nullable", True)),
                scale=int(scale) if scale is not None else None,
                on_overflow=cast(
                    OmniBeamDecimalOverflow,
                    item.get("on_overflow", ExtractionDefaults.decimal_overflow),
                ),
            )
        raise TypeError(f"Unsupported schema field representation: {type(item)}")

    def build_storage_source(
        self,
        *,
        paths: list[str],
        snapshot: SchemaSnapshot | Sequence[SchemaField] | Sequence[dict[str, Any]] | None = None,
        format: OmniBeamStorageFormat = StorageDefaults.format,  # type: ignore[assignment]
        delimiter: str = StorageDefaults.delimiter,
        quote_char: str = StorageDefaults.quote_char,
        multiline: bool = StorageDefaults.multiline,
        charset: str = ExtractionDefaults.encoding,
        compression: str = StorageDefaults.compression,
        chunk_size_bytes: int = StorageDefaults.chunk_size_bytes,
    ) -> StorageSourceConfig:
        """Builds StorageSourceConfig for file / object storage stream ingestion (§4.1)."""
        if not isinstance(paths, list):
            raise TypeError("Argument 'paths' must be a list of file path strings.")

        return StorageSourceConfig(
            type="storage",
            paths=paths,
            format=format,
            delimiter=delimiter,
            quote_char=quote_char,
            multiline=multiline,
            charset=charset,
            compression=compression,
            chunk_size_bytes=chunk_size_bytes,
            schema=self.build_fields_schema(snapshot),
        )

    def build_database_source(
        self,
        *,
        driver: OmniBeamDatabaseDriver,
        snapshot: SchemaSnapshot | Sequence[SchemaField] | Sequence[dict[str, Any]] | None = None,
        table: str | None = None,
        query: str | None = None,
        credential_ref: str | None = None,
        connection_uri: str | None = None,
        host: str | None = None,
        port: int | None = None,
        database: str | None = None,
        username: str | None = None,
        password_ref: str | None = None,
        query_filter: str | None = None,
        columns: list[str] | None = None,
        flatten_nested: bool = DatabaseDefaults.flatten_nested,
        partition_config: OmniBeamPartitionConfig | None = None,
        pool_config: OmniBeamPoolConfig | None = None,
        resilience: OmniBeamResilienceConfig | None = None,
        # Direct partition/pool parameters when not passing pre-built config objects:
        partition_column: str | None = None,
        batch_size: int = DatabaseDefaults.batch_size,
        num_partitions: int = DatabaseDefaults.num_partitions,
        max_open_conns: int = DatabaseDefaults.max_open_conns,
        max_idle_conns: int = DatabaseDefaults.max_idle_conns,
        conn_max_lifetime_s: int = DatabaseDefaults.conn_max_lifetime_s,
    ) -> DatabaseSourceConfig:
        """Builds DatabaseSourceConfig for SQL & MongoDB bounded partitioned queries (§4.2)."""
        if not table and not query:
            raise ValueError("Database source requires either 'table' or 'query' to be specified.")

        resolved_partition_cfg = (
            partition_config
            if partition_config is not None
            else OmniBeamPartitionConfig(
                partition_column=partition_column,
                batch_size=batch_size,
                num_partitions=num_partitions,
            )
        )
        resolved_pool_cfg = (
            pool_config
            if pool_config is not None
            else OmniBeamPoolConfig(
                max_open_conns=max_open_conns,
                max_idle_conns=max_idle_conns,
                conn_max_lifetime_s=conn_max_lifetime_s,
            )
        )

        return DatabaseSourceConfig(
            driver=driver,
            table=table,
            query=query,
            credential_ref=credential_ref,
            connection_uri=connection_uri,
            host=host,
            port=port,
            database=database,
            username=username,
            password_ref=password_ref,
            query_filter=query_filter,
            columns=columns if columns is not None else [],
            flatten_nested=flatten_nested,
            partition_config=resolved_partition_cfg,
            pool_config=resolved_pool_cfg,
            resilience=resilience if resilience is not None else OmniBeamResilienceConfig(),
            schema=self.build_fields_schema(snapshot),
        )

    def build_mongo_source(
        self,
        *,
        collection: str,
        credential_ref: str,
        database: str,
        snapshot: SchemaSnapshot | Sequence[SchemaField] | Sequence[dict[str, Any]] | None = None,
        connection_uri: str | None = None,
        flatten_nested: bool = True,
        filter_json: str | None = None,
    ) -> DatabaseSourceConfig:
        """Convenience method building MongoDB source conforming to DatabaseSourceConfig (§4.2)."""
        return self.build_database_source(
            driver="mongodb",
            table=collection,
            credential_ref=credential_ref,
            connection_uri=connection_uri,
            database=database,
            flatten_nested=flatten_nested,
            query_filter=filter_json,
            snapshot=snapshot,
        )

    def build_rest_api_source(
        self,
        *,
        base_url: str,
        endpoint: str,
        pagination: OmniBeamApiPagination | None = None,
        snapshot: SchemaSnapshot | Sequence[SchemaField] | Sequence[dict[str, Any]] | None = None,
        http_method: OmniBeamHttpMethod = RestApiDefaults.http_method,  # type: ignore[assignment]
        headers: dict[str, str] | None = None,
        query_params: dict[str, str] | None = None,
        auth: OmniBeamApiAuth | None = None,
        retry: OmniBeamResilienceConfig | None = None,
        skip_tls_verify: bool = False,
        records_path: str = "",
        field_mapping: dict[str, str] | None = None,
        # Direct pagination/auth parameters when not passing pre-built config objects:
        auth_type: OmniBeamApiAuthType | None = None,
        token_ref: str | None = None,
        pagination_type: OmniBeamApiPaginationType = RestApiDefaults.pagination_type,  # type: ignore[assignment]
        page_size: int = ExtractionDefaults.page_size,
        page_param: str = RestApiDefaults.page_param,
        size_param: str = RestApiDefaults.size_param,
    ) -> RestApiSourceConfig:
        """Builds RestApiSourceConfig for paginated REST API endpoints (§4.3)."""
        if not base_url:
            raise ValueError("RestApi source requires a non-empty 'base_url'.")
        if not endpoint:
            raise ValueError("RestApi source requires a non-empty 'endpoint'.")

        resolved_pagination = (
            pagination
            if pagination is not None
            else OmniBeamApiPagination(
                type=pagination_type,
                page_param=page_param,
                size_param=size_param,
                page_size=page_size,
            )
        )
        resolved_auth = (
            auth
            if auth is not None
            else (OmniBeamApiAuth(type=auth_type, token_ref=token_ref) if auth_type else None)
        )

        return RestApiSourceConfig(
            base_url=base_url,
            endpoint=endpoint,
            http_method=http_method,
            headers=headers if headers is not None else {},
            query_params=query_params if query_params is not None else {},
            auth=resolved_auth,
            pagination=resolved_pagination,
            retry=retry if retry is not None else OmniBeamResilienceConfig(),
            skip_tls_verify=skip_tls_verify,
            records_path=records_path,
            field_mapping=field_mapping if field_mapping is not None else {},
            schema=self.build_fields_schema(snapshot),
        )

    # =========================================================================
    # Destination Sink Builders (§5)
    # =========================================================================

    def build_storage_destination(
        self,
        output_path: str,
        output_format: OmniBeamDestinationFormat = DestinationDefaults.output_format,  # type: ignore[assignment]
        compression: str = ExtractionDefaults.compression,
        single_file: bool = DestinationDefaults.single_file,
        include_audit_columns: bool = DestinationDefaults.include_audit_columns,
        metrics_dir: str | None = None,
        encryption: OmniBeamEncryptionConfig | None = None,
        format_options: OmniBeamFormatOptions | None = None,
    ) -> OmniBeamStorageDestinationConfig:
        """Builds Storage/File destination sink matching OmniBeam §5.1."""
        if not output_path:
            raise ValueError("Storage destination requires a non-empty 'output_path'.")

        return OmniBeamStorageDestinationConfig(
            type="storage",
            output_path=output_path,
            output_format=output_format,
            compression=compression,
            single_file=single_file,
            include_audit_columns=include_audit_columns,
            metrics_dir=metrics_dir,
            encryption=encryption if encryption is not None else OmniBeamEncryptionConfig(),
            format_options=format_options
            if format_options is not None
            else OmniBeamFormatOptions(),
        )

    def build_bigquery_destination(
        self,
        dataset_id: str,
        table_id: str,
        project_id: str | None = None,
        write_disposition: OmniBeamBigQueryWriteDisposition = DestinationDefaults.bigquery_write_disposition,  # type: ignore[assignment]
        batch_size: int = DestinationDefaults.bigquery_batch_size,
    ) -> OmniBeamBigQueryDestinationConfig:
        """Builds Google BigQuery destination sink matching OmniBeam §5.2."""
        if not dataset_id or not table_id:
            raise ValueError("BigQuery destination requires both 'dataset_id' and 'table_id'.")

        return OmniBeamBigQueryDestinationConfig(
            type="bigquery",
            bigquery_options=OmniBeamBigQueryOptions(
                project_id=project_id,
                dataset_id=dataset_id,
                table_id=table_id,
                write_disposition=write_disposition,
                batch_size=batch_size,
            ),
        )

    def build_rest_api_destination(
        self,
        base_url: str,
        resource_path: str,
        method: OmniBeamHttpMethod = DestinationDefaults.rest_api_method,  # type: ignore[assignment]
        batch_size: int = DestinationDefaults.rest_api_batch_size,
        credential_ref: str | None = None,
        auth_type: OmniBeamApiAuthType | None = None,
        headers: dict[str, str] | None = None,
        body_envelope: str = "",
        rate_limit_rps: int = DestinationDefaults.rest_api_rate_limit_rps,
        timeout_ms: int = DestinationDefaults.rest_api_timeout_ms,
        max_retries: int = DestinationDefaults.rest_api_max_retries,
    ) -> OmniBeamRestApiDestinationConfig:
        """Builds REST HTTP API export destination sink matching OmniBeam §5.3."""
        if not base_url:
            raise ValueError("REST API destination requires a non-empty 'base_url'.")
        if not resource_path:
            raise ValueError("REST API destination requires a non-empty 'resource_path'.")

        return OmniBeamRestApiDestinationConfig(
            type="rest_api",
            endpoint=OmniBeamApiEndpointConfig(
                base_url=base_url,
                credential_ref=credential_ref,
                auth_type=auth_type,
                headers=headers if headers is not None else {},
            ),
            api_options=OmniBeamApiOptions(
                resource_path=resource_path,
                method=method,
                batch_size=batch_size,
                body_envelope=body_envelope,
                rate_limit_rps=rate_limit_rps,
                timeout_ms=timeout_ms,
                max_retries=max_retries,
            ),
        )

    # =========================================================================
    # Canonical Manifest Construction (§2)
    # =========================================================================

    def build(
        self,
        pipeline_id: str,
        run_id: str,
        quarantine_path: str,
        source_config: SourceConfigUnion | None = None,
        destination_config: DestinationConfigUnion | None = None,
        files: list[PipelineRunFile] | None = None,
        snapshot: SchemaSnapshot | Sequence[SchemaField] | Sequence[dict[str, Any]] | None = None,
        output_path: str = "",
        max_error_percentage: float = DlqDefaults.max_error_percentage,
        runner: OmniBeamRunnerType = OmniBeamDefaults.runner,  # type: ignore[assignment]
        pipeline_type: OmniBeamPipelineType = OmniBeamDefaults.pipeline_type,  # type: ignore[assignment]
        secrets_config: OmniBeamSecretsConfig | None = None,
        sensitive_fields: list[str] | None = None,
        quality_rules: list[dict[str, Any] | OmniBeamQualityRule] | None = None,
    ) -> OmniBeamManifest:
        """Constructs a canonical OmniBeam manifest ensuring strict mutual exclusivity

        between source connectors (OmniBeam §2, §4) and proper sink configuration (§5).
        Fails fast without silent format guessing or dummy schema creation.
        """
        if not pipeline_id:
            raise ValueError("Argument 'pipeline_id' is required.")
        if not run_id:
            raise ValueError("Argument 'run_id' is required.")
        if not quarantine_path:
            raise ValueError(
                "Argument 'quarantine_path' is required for DLQ quarantine configuration."
            )

        # Source resolution
        if source_config is not None:
            resolved_source = source_config
        elif files:
            file_paths = [f.file_path for f in files]
            if not file_paths:
                raise ValueError("Argument 'files' cannot be empty.")
            fmt_str = normalize_file_format(file_paths[0])
            if fmt_str not in ("csv", "txt", "tsv", "json", "jsonl", "jsonlines", "parquet"):
                raise ValueError(
                    f"Unsupported file format '{fmt_str}' detected for files {file_paths}. "
                    "Must be one of: csv, txt, tsv, json, jsonl, jsonlines, parquet."
                )
            resolved_source = self.build_storage_source(
                paths=file_paths,
                snapshot=snapshot,
                format=fmt_str,  # type: ignore[arg-type]
            )
        else:
            raise ValueError(
                "Either 'source_config' or a non-empty 'files' list must be provided to build an OmniBeam manifest."
            )

        storage_src: StorageSourceConfig | None = None
        database_src: DatabaseSourceConfig | None = None
        api_src: RestApiSourceConfig | None = None

        if isinstance(resolved_source, StorageSourceConfig):
            storage_src = resolved_source
        elif isinstance(resolved_source, DatabaseSourceConfig):
            database_src = resolved_source
        elif isinstance(resolved_source, RestApiSourceConfig):
            api_src = resolved_source
        else:
            raise TypeError(f"Unsupported source configuration type: {type(resolved_source)}")

        # Destination resolution
        if destination_config is not None:
            dest_cfg = destination_config
        elif output_path:
            dest_cfg = self.build_storage_destination(output_path=output_path)
        else:
            raise ValueError(
                "Either 'destination_config' or 'output_path' must be provided to build an OmniBeam manifest."
            )

        dlq_cfg = OmniBeamDlqConfig(
            enabled=True,
            quarantine_path=quarantine_path,
            max_error_percentage=max_error_percentage,
        )

        parsed_rules: list[OmniBeamQualityRule] = []
        if quality_rules:
            for r in quality_rules:
                if isinstance(r, OmniBeamQualityRule):
                    parsed_rules.append(r)
                elif isinstance(r, dict):
                    parsed_rules.append(OmniBeamQualityRule(**r))
                else:
                    raise TypeError(f"Unsupported quality rule item: {type(r)}")

        quality_cfg = OmniBeamQualityConfig(rules=parsed_rules) if parsed_rules else None
        security_cfg = (
            OmniBeamSecurityConfig(sensitive_fields=sensitive_fields) if sensitive_fields else None
        )

        return OmniBeamManifest(
            pipeline_id=pipeline_id,
            run_id=run_id,
            pipeline_type=pipeline_type,
            runner=runner,
            secrets_config=secrets_config,
            source=storage_src,
            database_source=database_src,
            api_source=api_src,
            destination=dest_cfg,
            dlq_config=dlq_cfg,
            quality_config=quality_cfg,
            security=security_cfg,
        )
