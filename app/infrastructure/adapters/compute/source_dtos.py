# app/infrastructure/adapters/compute/source_dtos.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from app.domain.shared.platform_defaults import (
    DatabaseDefaults,
    ExtractionDefaults,
    RestApiDefaults,
    StorageDefaults,
)
from app.infrastructure.adapters.compute.rest_api_helpers import normalize_resource_path


def _extract_file_path(item: Any) -> str:
    if isinstance(item, str):
        return item.replace("\\", "/")
    if isinstance(item, dict):
        return str(item.get("file_path", "")).replace("\\", "/")
    return str(getattr(item, "file_path", "")).replace("\\", "/")


@dataclass(frozen=True)
class PipelineExecutionTargetDTO:
    """Canonical representation of the pipeline target object and credentials pointer.

    Single point of extraction for pipeline metadata across compute adapters.
    """

    source_type: str
    object_name: str
    credential_ref: str
    extraction_query: str | None = None

    @classmethod
    def from_config(
        cls, config: dict[str, Any], default_source_type: str | None = None
    ) -> PipelineExecutionTargetDTO:
        source_type = config.get("source_type")
        object_name = config.get("object_name")
        credential_ref = config.get("credential_ref")
        extraction_query = config.get("extraction_query")

        source_objects = config.get("source_objects")
        if isinstance(source_objects, list) and source_objects:
            first_obj = source_objects[0]
            if isinstance(first_obj, dict):
                if not source_type:
                    source_type = first_obj.get("source_type")
                if not object_name:
                    object_name = first_obj.get("object_name")
                if not credential_ref:
                    credential_ref = first_obj.get("credential_ref")
                if not extraction_query:
                    extraction_query = first_obj.get("extraction_query")

        if not source_type and default_source_type:
            source_type = default_source_type

        if not source_type:
            raise ValueError("Field 'source_type' is required in compute configuration.")

        return cls(
            source_type=str(source_type).lower(),
            object_name=str(object_name) if object_name is not None else "",
            credential_ref=str(credential_ref) if credential_ref is not None else "",
            extraction_query=extraction_query,
        )


@dataclass(frozen=True)
class DatabaseConnectionDTO:
    """Canonical DTO for relational database connection parameters.

    Strict contract: all connection parameters must be provided by Vault / endpoint credentials.
    No silent defaults masking missing configuration.
    """

    driver: str
    database: str
    user: str
    password: str
    host: str
    port: int
    schema_name: str | None = None
    sslmode: str = ""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DatabaseConnectionDTO:
        """Parse and strictly validate relational database connection parameters."""
        driver = data.get("driver")
        if not driver:
            raise ValueError("Field 'driver' is required in database credentials.")

        database = data.get("database")
        if not database:
            raise ValueError("Field 'database' is required in database credentials.")

        user = data.get("user")
        if not user:
            raise ValueError("Field 'user' is required in database credentials.")

        host = data.get("host")
        if not host:
            raise ValueError("Field 'host' is required in database credentials.")

        port_raw = data.get("port")
        if port_raw is None:
            raise ValueError("Field 'port' is required in database credentials.")

        return cls(
            driver=str(driver).lower(),
            database=str(database),
            user=str(user),
            password=str(data.get("password", "")),
            host=str(host),
            port=int(port_raw),
            schema_name=data.get("schema"),
            sslmode=str(data.get("sslmode", "")).strip().lower(),
        )

    def to_dsn(self) -> str:
        """Standard DSN representation for database attachment and psycopg."""
        return f"host={self.host} port={self.port} dbname={self.database} user={self.user} password={self.password}"

    def to_uri(self) -> str:
        """Standard connection URI for OmniBeam database ingestion."""
        driver_scheme = (
            "postgresql" if self.driver in ("postgres", "pgx", "cockroach") else self.driver
        )
        auth_part = f"{self.user}:{self.password}@" if (self.user or self.password) else ""
        return f"{driver_scheme}://{auth_part}{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class DatabaseSourceDTO:
    """Canonical DTO for OmniBeam relational database sources matching OmniBeam §4.2."""

    driver: str
    table: str
    credential_ref: str
    database: str
    connection_uri: str | None = None
    schema_name: str | None = None
    query: str | None = None
    query_filter: str | None = None
    columns: list[str] = field(default_factory=list)
    partition_column: str | None = None
    batch_size: int = DatabaseDefaults.batch_size
    num_partitions: int = DatabaseDefaults.num_partitions
    max_open_conns: int = DatabaseDefaults.max_open_conns
    max_idle_conns: int = DatabaseDefaults.max_idle_conns
    conn_max_lifetime_s: int = DatabaseDefaults.conn_max_lifetime_s
    flatten_nested: bool = DatabaseDefaults.flatten_nested
    watermark_column: str | None = None
    watermark_value: Any = None
    snapshot_fields: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_inputs(
        cls,
        *,
        object_name: str,
        config: dict[str, Any],
        creds: dict[str, Any],
        snapshot_fields: list[dict[str, Any]],
        pipeline_id: str,
    ) -> DatabaseSourceDTO:
        table_raw = object_name if object_name else config.get("table", "")
        query = config.get("query")
        if not table_raw and not query:
            raise ValueError(
                f"A 'table' (or object_name) or 'query' is required for database pipeline {pipeline_id!r}"
            )

        driver = creds.get("driver")
        if not driver:
            raise ValueError(
                f"driver is required in endpoint credentials for database pipeline {pipeline_id!r}"
            )

        schema_name = creds.get("schema")
        table = str(table_raw)
        if schema_name and "." not in table:
            table = f"{schema_name}.{table}"

        conn_uri = creds.get("connection_uri")
        database = creds.get("database")
        if not conn_uri and creds.get("host") and database:
            db_conn = DatabaseConnectionDTO.from_dict(creds)
            conn_uri = db_conn.to_uri()

        if not database and not conn_uri:
            raise ValueError(
                f"database (or connection_uri) is required in endpoint credentials for database pipeline {pipeline_id!r}"
            )

        raw_cred_ref = config.get("credential_ref")
        credential_ref = str(raw_cred_ref) if raw_cred_ref is not None else ""

        # Partition configuration (OmniBeam §4.2)
        partition_cfg = config.get("partition_config")
        if isinstance(partition_cfg, dict):
            partition_column = partition_cfg.get("partition_column")
            raw_batch_size = partition_cfg.get("batch_size")
            raw_num_partitions = partition_cfg.get("num_partitions")
        else:
            partition_column = config.get("partition_column")
            raw_batch_size = config.get("batch_size")
            raw_num_partitions = config.get("num_partitions")

        batch_size = (
            int(raw_batch_size) if raw_batch_size is not None else DatabaseDefaults.batch_size
        )
        num_partitions = (
            int(raw_num_partitions)
            if raw_num_partitions is not None
            else DatabaseDefaults.num_partitions
        )

        # Connection Pool configuration (OmniBeam §4.2)
        pool_cfg = config.get("pool_config")
        if isinstance(pool_cfg, dict):
            raw_max_open = pool_cfg.get("max_open_conns")
            raw_max_idle = pool_cfg.get("max_idle_conns")
            raw_lifetime = pool_cfg.get("conn_max_lifetime_s")
        else:
            raw_max_open = config.get("max_open_conns")
            raw_max_idle = config.get("max_idle_conns")
            raw_lifetime = config.get("conn_max_lifetime_s")

        max_open_conns = (
            int(raw_max_open) if raw_max_open is not None else DatabaseDefaults.max_open_conns
        )
        max_idle_conns = (
            int(raw_max_idle) if raw_max_idle is not None else DatabaseDefaults.max_idle_conns
        )
        conn_max_lifetime_s = (
            int(raw_lifetime) if raw_lifetime is not None else DatabaseDefaults.conn_max_lifetime_s
        )

        return cls(
            driver=str(driver).lower(),
            table=str(table),
            credential_ref=credential_ref,
            connection_uri=conn_uri,
            database=str(database) if database is not None else "",
            schema_name=schema_name,
            query=query,
            query_filter=config.get("query_filter"),
            columns=list(config.get("columns", [])),
            partition_column=partition_column,
            batch_size=batch_size,
            num_partitions=num_partitions,
            max_open_conns=max_open_conns,
            max_idle_conns=max_idle_conns,
            conn_max_lifetime_s=conn_max_lifetime_s,
            flatten_nested=bool(config.get("flatten_nested", DatabaseDefaults.flatten_nested)),
            watermark_column=config.get("watermark_column"),
            watermark_value=config.get("watermark_value"),
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class MongoSourceDTO:
    """Canonical DTO for OmniBeam MongoDB sources matching OmniBeam §4.2."""

    collection: str
    database: str
    credential_ref: str
    connection_uri: str | None = None
    filter_json: str | None = None
    flatten_nested: bool = True
    snapshot_fields: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_inputs(
        cls,
        *,
        object_name: str,
        config: dict[str, Any],
        creds: dict[str, Any],
        snapshot_fields: list[dict[str, Any]],
        pipeline_id: str,
    ) -> MongoSourceDTO:
        collection = object_name if object_name else config.get("collection", "")
        if not collection:
            raise ValueError(
                f"collection (object_name) is required for MongoDB pipeline {pipeline_id!r}"
            )

        database = creds.get("database")
        conn_uri = creds.get("connection_uri")
        if not database and not conn_uri:
            raise ValueError(
                f"database is required in endpoint credentials for MongoDB pipeline {pipeline_id!r}"
            )

        raw_cred_ref = config.get("credential_ref")
        credential_ref = str(raw_cred_ref) if raw_cred_ref is not None else ""

        return cls(
            collection=collection,
            credential_ref=credential_ref,
            database=str(database) if database is not None else "",
            connection_uri=conn_uri,
            filter_json=config.get("filter_json"),
            flatten_nested=bool(config.get("flatten_nested", True)),
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class RestApiSourceDTO:
    """Canonical DTO for OmniBeam REST API sources matching OmniBeam §4.3."""

    base_url: str
    endpoint: str
    credential_ref: str
    http_method: str = RestApiDefaults.http_method
    headers: dict[str, str] = field(default_factory=dict)
    query_params: dict[str, str] = field(default_factory=dict)
    auth_type: str | None = None
    token_ref: str | None = None
    api_key_header: str | None = None
    api_key_query: str | None = None
    username_ref: str | None = None
    password_ref: str | None = None
    token_url: str | None = None
    client_id_ref: str | None = None
    client_secret_ref: str | None = None
    scopes: list[str] = field(default_factory=list)
    pagination_type: str = RestApiDefaults.pagination_type
    page_param: str = RestApiDefaults.page_param
    size_param: str = RestApiDefaults.size_param
    page_size: int = ExtractionDefaults.page_size
    initial_page: int = RestApiDefaults.initial_page
    max_pages_limit: int = RestApiDefaults.max_pages_limit
    cursor_param: str | None = None
    next_cursor_path: str | None = None
    total_count_path: str | None = None
    has_more_path: str | None = None
    skip_tls_verify: bool = False
    records_path: str = ""
    field_mapping: dict[str, str] = field(default_factory=dict)
    snapshot_fields: list[dict[str, Any]] = field(default_factory=list)

    @classmethod
    def from_inputs(
        cls,
        *,
        object_name: str,
        config: dict[str, Any],
        creds: dict[str, Any],
        snapshot_fields: list[dict[str, Any]],
        pipeline_id: str,
    ) -> RestApiSourceDTO:
        base_url = creds.get("base_url")
        if not base_url:
            raise ValueError(
                f"base_url is required in endpoint credentials for REST API pipeline {pipeline_id!r}"
            )

        endpoint = config.get("endpoint")
        if not endpoint and object_name:
            endpoint = normalize_resource_path(object_name)
        if not endpoint:
            raise ValueError(
                f"endpoint (or object_name) is required for REST API pipeline {pipeline_id!r}"
            )

        raw_cred_ref = config.get("credential_ref")
        credential_ref = str(raw_cred_ref) if raw_cred_ref is not None else ""
        auth_type = creds.get("auth_type")
        if auth_type == "bearer":
            auth_type = "bearer_token"

        # Canonical pagination parameters strictly aligned with OmniBeam §4.3 and RestApiDefaults
        raw_pag = config.get("pagination")
        pag_config: dict[str, Any] = raw_pag if isinstance(raw_pag, dict) else {}
        pag_type = str(pag_config.get("type", RestApiDefaults.pagination_type))
        page_param = str(pag_config.get("page_param", RestApiDefaults.page_param))
        size_param = str(pag_config.get("size_param", RestApiDefaults.size_param))
        page_size = int(pag_config.get("page_size", ExtractionDefaults.page_size))
        initial_page = int(pag_config.get("initial_page", RestApiDefaults.initial_page))
        max_pages_limit = int(pag_config.get("max_pages_limit", RestApiDefaults.max_pages_limit))

        token_ref = creds.get("token_ref")
        if not token_ref and auth_type == "bearer_token" and credential_ref:
            token_ref = credential_ref

        records_path = ""
        if "records_path" in config and config["records_path"]:
            records_path = str(config["records_path"])
        elif "records_path" in creds and creds["records_path"]:
            records_path = str(creds["records_path"])
        elif isinstance(config.get("schema_snapshot"), dict):
            extra = config["schema_snapshot"].get("extra")
            if isinstance(extra, dict) and extra.get("wrapper_key"):
                records_path = str(extra["wrapper_key"])

        return cls(
            base_url=base_url,
            endpoint=endpoint,
            credential_ref=credential_ref,
            http_method=str(config.get("http_method", RestApiDefaults.http_method)).upper(),
            headers=dict(config.get("headers", {})),
            query_params=dict(config.get("query_params", {})),
            auth_type=auth_type,
            token_ref=token_ref,
            api_key_header=creds.get("api_key_header"),
            api_key_query=creds.get("api_key_query"),
            username_ref=creds.get("username_ref"),
            password_ref=creds.get("password_ref"),
            token_url=creds.get("token_url"),
            client_id_ref=creds.get("client_id_ref"),
            client_secret_ref=creds.get("client_secret_ref"),
            scopes=list(creds.get("scopes", [])),
            pagination_type=pag_type,
            page_param=page_param,
            size_param=size_param,
            page_size=page_size,
            initial_page=initial_page,
            max_pages_limit=max_pages_limit,
            cursor_param=pag_config.get("cursor_param"),
            next_cursor_path=pag_config.get("next_cursor_path"),
            total_count_path=pag_config.get("total_count_path"),
            has_more_path=pag_config.get("has_more_path"),
            skip_tls_verify=bool(config.get("skip_tls_verify", False)),
            records_path=records_path,
            field_mapping=dict(config.get("field_mapping", {})),
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class StorageSourceDTO:
    """Canonical DTO for OmniBeam file storage sources matching OmniBeam §4.1."""

    paths: list[str]
    format: str = StorageDefaults.format
    snapshot_fields: list[dict[str, Any]] = field(default_factory=list)
    delimiter: str = StorageDefaults.delimiter
    quote_char: str = StorageDefaults.quote_char
    multiline: bool = StorageDefaults.multiline
    charset: str = ExtractionDefaults.encoding
    compression: str = StorageDefaults.compression
    chunk_size_bytes: int = StorageDefaults.chunk_size_bytes

    @classmethod
    def from_inputs(
        cls,
        *,
        object_name: str,
        config: dict[str, Any],
        creds: dict[str, Any],
        snapshot_fields: list[dict[str, Any]],
        pipeline_id: str,
    ) -> StorageSourceDTO:
        if "paths" in config and config["paths"] is not None:
            input_paths = [str(p) for p in config["paths"]]
        elif "path" in config and config["path"]:
            input_paths = [str(config["path"]).replace("\\", "/")]
        elif "files" in config and config["files"]:
            input_paths = [_extract_file_path(f) for f in config["files"] if _extract_file_path(f)]
        elif object_name:
            input_paths = [str(object_name)]
        else:
            raise ValueError(
                f"No input paths or target object defined for storage pipeline {pipeline_id!r}"
            )

        file_format = config.get("format")
        if not file_format:
            raise ValueError(f"format is required for storage pipeline {pipeline_id!r}")

        return cls(
            paths=input_paths,
            format=str(file_format).lower(),
            snapshot_fields=snapshot_fields,
            delimiter=config.get("delimiter", StorageDefaults.delimiter),
            quote_char=config.get("quote_char", StorageDefaults.quote_char),
            multiline=bool(config.get("multiline", StorageDefaults.multiline)),
            charset=config.get("charset", ExtractionDefaults.encoding),
            compression=config.get("compression", StorageDefaults.compression),
            chunk_size_bytes=int(config.get("chunk_size_bytes", StorageDefaults.chunk_size_bytes)),
        )
