# app/infrastructure/adapters/compute/source_dtos.py
from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.infrastructure.adapters.compute.rest_api_helpers import normalize_resource_path
from app.infrastructure.discovery.connection_url_builder import build_connection_url


def _extract_file_path(item: Any) -> str:
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        return str(item.get("file_path") or "")
    return str(getattr(item, "file_path", "") or "")


@dataclass(frozen=True)
class PipelineExecutionTargetDTO:
    """Canonical representation of the pipeline target object and credentials pointer.

    Single point of extraction for pipeline metadata across all compute adapters.
    """

    source_type: str
    object_name: str
    credential_ref: str
    extraction_query: str | None = None

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PipelineExecutionTargetDTO:
        source_objects = config.get("source_objects", [])
        first_obj = (
            source_objects[0] if (source_objects and isinstance(source_objects[0], dict)) else {}
        )

        source_type = str(config.get("source_type") or first_obj.get("source_type", "")).lower()

        object_name = str(first_obj.get("object_name") or config.get("object_name", ""))

        credential_ref = str(config.get("credential_ref") or first_obj.get("credential_ref", ""))

        extraction_query = config.get("extraction_query") or first_obj.get("extraction_query")

        return cls(
            source_type=source_type,
            object_name=object_name,
            credential_ref=credential_ref,
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


@dataclass(frozen=True)
class DatabaseSourceDTO:
    """Canonical DTO for OmniBeam relational database sources.

    Strict separation:
    - Connection/infrastructure properties (driver, database, schema, uri) belong to creds (Vault).
    - Pipeline execution properties (table, query, watermark, partitions) belong to config.
    """

    driver: str
    table: str
    credential_ref: str
    database: str
    connection_uri: str | None = None
    schema_name: str | None = None
    query: str | None = None
    partition_column: str | None = None
    num_partitions: int = 1
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
        table = object_name or config.get("table")
        if not table:
            raise ValueError(
                f"table (object_name) is required for database pipeline {pipeline_id!r}"
            )

        credential_ref = str(config.get("credential_ref", ""))

        driver = creds.get("driver")
        if not driver:
            raise ValueError(
                f"driver is required in endpoint credentials for database pipeline {pipeline_id!r}"
            )

        conn_uri = creds.get("connection_uri")
        if not conn_uri and creds:
            with contextlib.suppress(Exception):
                conn_uri = build_connection_url(creds, async_driver=False)

        database = creds.get("database")
        if not database and conn_uri:
            with contextlib.suppress(Exception):
                from urllib.parse import urlparse

                database = urlparse(conn_uri).path.lstrip("/").split("?")[0]
        if not database:
            raise ValueError(
                f"database is required in endpoint credentials for database pipeline {pipeline_id!r}"
            )

        schema_name = creds.get("schema")

        return cls(
            driver=driver,
            table=table,
            credential_ref=credential_ref,
            connection_uri=conn_uri,
            database=database,
            schema_name=schema_name,
            query=config.get("query"),
            partition_column=config.get("partition_column"),
            num_partitions=int(config.get("num_partitions", 1)),
            watermark_column=config.get("watermark_column"),
            watermark_value=config.get("watermark_value"),
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class MongoSourceDTO:
    """Canonical DTO for OmniBeam MongoDB sources.

    Strict separation:
    - Connection/infrastructure properties (database, uri) belong to creds (Vault).
    - Pipeline execution properties (collection, filter) belong to config.
    """

    collection: str
    database: str
    credential_ref: str
    connection_uri: str | None = None
    filter_json: str | None = None
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
        collection = object_name or config.get("collection")
        if not collection:
            raise ValueError(
                f"collection (object_name) is required for MongoDB pipeline {pipeline_id!r}"
            )

        credential_ref = str(config.get("credential_ref", ""))

        conn_uri = creds.get("connection_uri")
        if not conn_uri and creds:
            with contextlib.suppress(Exception):
                conn_uri = build_connection_url(creds, async_driver=False)

        database = creds.get("database")
        if not database and conn_uri:
            with contextlib.suppress(Exception):
                from urllib.parse import urlparse

                database = urlparse(conn_uri).path.lstrip("/").split("?")[0]
        if not database:
            raise ValueError(
                f"database is required in endpoint credentials for MongoDB pipeline {pipeline_id!r}"
            )

        return cls(
            collection=collection,
            credential_ref=credential_ref,
            database=database,
            connection_uri=conn_uri,
            filter_json=config.get("filter_json"),
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class RestApiSourceDTO:
    """Canonical DTO for OmniBeam REST API sources.

    Strict separation:
    - Connection/infrastructure properties (base_url, auth_type) belong to creds (Vault).
    - Pipeline execution properties (endpoint, pagination, records_path) belong to config.
    """

    base_url: str
    endpoint: str
    credential_ref: str
    auth_type: str = ""
    pagination_strategy: str = "none"
    records_path: str = "data"
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

        endpoint = config.get("endpoint") or (
            normalize_resource_path(object_name) if object_name else None
        )
        if not endpoint:
            raise ValueError(
                f"endpoint (or object_name) is required for REST API pipeline {pipeline_id!r}"
            )

        credential_ref = str(config.get("credential_ref", ""))
        auth_type = str(creds.get("auth_type", ""))
        pagination_strategy = str(
            config.get("pagination_strategy")
            or config.get("pagination", {}).get("strategy", "none")
        )
        records_path = str(config.get("records_path", "data"))

        return cls(
            base_url=base_url,
            endpoint=endpoint,
            credential_ref=credential_ref,
            auth_type=auth_type,
            pagination_strategy=pagination_strategy,
            records_path=records_path,
            snapshot_fields=snapshot_fields,
        )


@dataclass(frozen=True)
class StorageSourceDTO:
    """Canonical DTO for OmniBeam file storage sources."""

    paths: list[str]
    format: str
    snapshot_fields: list[dict[str, Any]] = field(default_factory=list)
    delimiter: str = ","
    quote_char: str = '"'
    compression: str = "none"

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
        input_paths: list[str] = []
        raw_files = config.get("files")
        if raw_files and isinstance(raw_files, list):
            for rf in raw_files:
                f_path = _extract_file_path(rf)
                if f_path and Path(f_path).exists():
                    input_paths.append(Path(f_path).resolve().as_posix())
        elif config.get("paths") is not None:
            input_paths = list(config["paths"])

        file_format = config.get("format")
        if not file_format:
            raise ValueError(f"format is required for storage pipeline {pipeline_id!r}")

        return cls(
            paths=input_paths,
            format=str(file_format).lower(),
            snapshot_fields=snapshot_fields,
            delimiter=config.get("delimiter", ","),
            quote_char=config.get("quote_char", '"'),
            compression=config.get("compression", "none"),
        )
