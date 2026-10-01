from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, Field, model_validator

from app.domain.shared.platform_defaults import (
    DatabaseDefaults,
    DestinationDefaults,
    DlqDefaults,
    ExtractionDefaults,
    OmniBeamDefaults,
    RestApiDefaults,
    StorageDefaults,
)

# Canonical Type Aliases for OmniBeam Spec
OmniBeamFieldDataType = Literal[
    "string",
    "int64",
    "float64",
    "bool",
    "timestamp",
    "date",
    "decimal",
    "bytes",
    "json",
]
OmniBeamDecimalOverflow = Literal["round", "fail"]
OmniBeamSecretsProvider = Literal["openbao", "vault", "bao", "gcp", "gcp_sm", "gcp_secret_manager"]
OmniBeamStorageSourceType = Literal["file", "storage", "gcs", "local_storage"]
OmniBeamStorageFormat = Literal["csv", "txt", "tsv", "json", "jsonl", "jsonlines", "parquet"]
OmniBeamDatabaseDriver = Literal[
    "postgres",
    "pgx",
    "cockroach",
    "mysql",
    "mariadb",
    "mongo",
    "mongodb",
]
OmniBeamApiAuthType = Literal["bearer_token", "api_key", "basic_auth", "oauth2_client_credentials"]
OmniBeamApiPaginationType = Literal["page_number", "offset_limit", "cursor_token", "link_header"]
OmniBeamHttpMethod = Literal["GET", "POST", "PUT", "PATCH"]
OmniBeamEncryptionType = Literal["none", "pgp", "kms"]
OmniBeamStorageDestinationType = Literal[
    "file",
    "storage",
    "local_storage",
    "gcs",
    "s3",
    "parquet",
    "csv",
    "jsonl",
]
OmniBeamDestinationFormat = Literal["parquet", "csv", "jsonl", "json", "jsonlines", "txt", "tsv"]
OmniBeamBigQueryWriteDisposition = Literal["WRITE_APPEND", "WRITE_TRUNCATE", "WRITE_EMPTY"]
OmniBeamQualityRuleType = Literal["not_null", "accepted_values", "row_count_min", "row_count_max"]
OmniBeamPipelineType = Literal["ingestion", "sql", "api"]
OmniBeamRunnerType = Literal["direct", "dataflow"]


class OmniBeamFieldSchema(BaseModel):
    """Zero-allocation schema field representation matching OmniBeam §6."""

    name: str
    type: OmniBeamFieldDataType
    nullable: bool = True
    scale: int | None = None
    on_overflow: OmniBeamDecimalOverflow = ExtractionDefaults.decimal_overflow

    @model_validator(mode="after")
    def validate_decimal_scale(self) -> OmniBeamFieldSchema:
        if self.type == "decimal" and self.scale is None:
            raise ValueError(
                f"Field '{self.name}': 'scale' is required and cannot be None when type is 'decimal'."
            )
        return self


class OmniBeamSchemaWrapper(BaseModel):
    """Wrapper holding typed fields for OmniBeam sources and root configurations."""

    fields: list[OmniBeamFieldSchema]


class OmniBeamResilienceConfig(BaseModel):
    """Fault tolerance, retry and rate limiting configuration matching OmniBeam §4.4."""

    max_retries: int = RestApiDefaults.max_retries
    initial_backoff_ms: int = RestApiDefaults.initial_backoff_ms
    max_backoff_ms: int = RestApiDefaults.max_backoff_ms
    backoff_multiplier: float = RestApiDefaults.backoff_multiplier
    rate_limit_rps: float = RestApiDefaults.rate_limit_rps
    timeout_ms: int = RestApiDefaults.timeout_ms
    circuit_breaker_fails: int = RestApiDefaults.circuit_breaker_fails
    circuit_breaker_timeout_ms: int = RestApiDefaults.circuit_breaker_timeout_ms
    half_open_limit: int = RestApiDefaults.half_open_limit


class OmniBeamSecretsConfig(BaseModel):
    """Universal secrets resolution backend configuration matching OmniBeam §3."""

    provider: OmniBeamSecretsProvider
    vault_url: str | None = None
    vault_token: str | None = None
    gcp_project_id: str | None = None
    host_override: dict[str, str] = Field(default_factory=dict)


# =============================================================================
# Ingestion Sources (§4)
# =============================================================================


class StorageSourceConfig(BaseModel):
    """ByteStream / Storage stream ingestion configuration matching OmniBeam §4.1."""

    type: OmniBeamStorageSourceType = "file"
    paths: list[str] = Field(default_factory=list)
    path: str | None = None
    format: OmniBeamStorageFormat = StorageDefaults.format  # type: ignore[assignment]
    delimiter: str = StorageDefaults.delimiter
    quote_char: str = StorageDefaults.quote_char
    multiline: bool = StorageDefaults.multiline
    charset: str = ExtractionDefaults.encoding
    compression: str = StorageDefaults.compression
    chunk_size_bytes: int = StorageDefaults.chunk_size_bytes
    schema_: OmniBeamSchemaWrapper | None = Field(default=None, alias="schema")


class OmniBeamPartitionConfig(BaseModel):
    """Partition query parameters for database sources matching OmniBeam §4.2."""

    partition_column: str | None = None
    batch_size: int = DatabaseDefaults.batch_size
    num_partitions: int = DatabaseDefaults.num_partitions


class OmniBeamPoolConfig(BaseModel):
    """Connection pool parameters for database sources matching OmniBeam §4.2."""

    max_open_conns: int = DatabaseDefaults.max_open_conns
    max_idle_conns: int = DatabaseDefaults.max_idle_conns
    conn_max_lifetime_s: int = DatabaseDefaults.conn_max_lifetime_s


class DatabaseSourceConfig(BaseModel):
    """Relational SQL and MongoDB partitioned database ingestion matching OmniBeam §4.2."""

    driver: OmniBeamDatabaseDriver
    connection_uri: str | None = None
    host: str | None = None
    port: int | None = None
    database: str | None = None
    username: str | None = None
    password_ref: str | None = None
    credential_ref: str | None = None
    table: str | None = None
    query: str | None = None
    query_filter: str | None = None
    columns: list[str] = Field(default_factory=list)
    flatten_nested: bool = DatabaseDefaults.flatten_nested
    partition_config: OmniBeamPartitionConfig = Field(default_factory=OmniBeamPartitionConfig)
    pool_config: OmniBeamPoolConfig = Field(default_factory=OmniBeamPoolConfig)
    resilience: OmniBeamResilienceConfig = Field(default_factory=OmniBeamResilienceConfig)
    schema_: OmniBeamSchemaWrapper | None = Field(default=None, alias="schema")

    @model_validator(mode="after")
    def validate_target(self) -> DatabaseSourceConfig:
        if not self.table and not self.query:
            raise ValueError("Database source requires either 'table' or 'query' to be specified.")
        return self


class OmniBeamApiAuth(BaseModel):
    """Authentication parameters for REST API sources matching OmniBeam §4.3."""

    type: OmniBeamApiAuthType
    token_ref: str | None = None
    api_key_header: str | None = None
    api_key_query: str | None = None
    username_ref: str | None = None
    password_ref: str | None = None
    token_url: str | None = None
    client_id_ref: str | None = None
    client_secret_ref: str | None = None
    scopes: list[str] = Field(default_factory=list)


class OmniBeamApiPagination(BaseModel):
    """Pagination strategy configuration matching OmniBeam §4.3."""

    type: OmniBeamApiPaginationType
    page_param: str = RestApiDefaults.page_param
    size_param: str = RestApiDefaults.size_param
    page_size: int = ExtractionDefaults.page_size
    initial_page: int = RestApiDefaults.initial_page
    max_pages_limit: int = RestApiDefaults.max_pages_limit
    total_pages_hint: int = 0
    cursor_param: str | None = None
    next_cursor_path: str | None = None
    total_count_path: str | None = None
    has_more_path: str | None = None


class RestApiSourceConfig(BaseModel):
    """Paged REST API source ingestion configuration matching OmniBeam §4.3."""

    base_url: str
    endpoint: str
    http_method: OmniBeamHttpMethod = RestApiDefaults.http_method  # type: ignore[assignment]
    headers: dict[str, str] = Field(default_factory=dict)
    query_params: dict[str, str] = Field(default_factory=dict)
    auth: OmniBeamApiAuth | None = None
    pagination: OmniBeamApiPagination
    retry: OmniBeamResilienceConfig = Field(default_factory=OmniBeamResilienceConfig)
    skip_tls_verify: bool = False
    records_path: str = ""
    field_mapping: dict[str, str] = Field(default_factory=dict)
    schema_: OmniBeamSchemaWrapper | None = Field(default=None, alias="schema")


SourceConfigUnion = Annotated[
    StorageSourceConfig | DatabaseSourceConfig | RestApiSourceConfig,
    Field(discriminator="type"),
]

# Alias for backwards compatibility with storage source references
OmniBeamSourceConfig = StorageSourceConfig


# =============================================================================
# Destination Sinks & Data Export (§5)
# =============================================================================


class OmniBeamEncryptionConfig(BaseModel):
    """Envelope encryption for storage sinks matching OmniBeam §5.1.1."""

    type: OmniBeamEncryptionType = "none"
    public_key_ref: str | None = None
    kms_key_ref: str | None = None

    @model_validator(mode="after")
    def validate_key_refs(self) -> OmniBeamEncryptionConfig:
        if self.type == "pgp" and not self.public_key_ref:
            raise ValueError("Encryption type 'pgp' requires 'public_key_ref'.")
        if self.type == "kms" and not self.kms_key_ref:
            raise ValueError("Encryption type 'kms' requires 'kms_key_ref'.")
        return self


class OmniBeamFormatOptions(BaseModel):
    """Delimited file formatting options matching OmniBeam §5.1.2."""

    delimiter: str = StorageDefaults.delimiter
    include_header: bool = False
    charset: str = ExtractionDefaults.encoding
    line_terminator: str = "\n"
    quote_all: bool = False


class OmniBeamStorageDestinationConfig(BaseModel):
    """Storage & File sink configuration matching OmniBeam §5.1."""

    type: OmniBeamStorageDestinationType = "file"
    output_path: str
    output_format: OmniBeamDestinationFormat = DestinationDefaults.output_format  # type: ignore[assignment]
    compression: str = ExtractionDefaults.compression
    single_file: bool = DestinationDefaults.single_file
    include_audit_columns: bool = DestinationDefaults.include_audit_columns
    metrics_dir: str | None = None
    encryption: OmniBeamEncryptionConfig = Field(default_factory=OmniBeamEncryptionConfig)
    format_options: OmniBeamFormatOptions = Field(default_factory=OmniBeamFormatOptions)


class OmniBeamBigQueryOptions(BaseModel):
    """Target options for Google Cloud BigQuery Storage Write API matching OmniBeam §5.2."""

    project_id: str | None = None
    dataset_id: str
    table_id: str
    write_disposition: OmniBeamBigQueryWriteDisposition = (
        DestinationDefaults.bigquery_write_disposition  # type: ignore[assignment]
    )
    batch_size: int = DestinationDefaults.bigquery_batch_size


class OmniBeamBigQueryDestinationConfig(BaseModel):
    """Google BigQuery destination sink matching OmniBeam §5.2."""

    type: Literal["bigquery"] = "bigquery"
    bigquery_options: OmniBeamBigQueryOptions


class OmniBeamApiEndpointConfig(BaseModel):
    """Target endpoint connection parameters for REST HTTP API Sink matching OmniBeam §5.3."""

    base_url: str
    credential_ref: str | None = None
    auth_type: OmniBeamApiAuthType | None = None
    headers: dict[str, str] = Field(default_factory=dict)


class OmniBeamApiOptions(BaseModel):
    """Micro-batching and request options for REST HTTP API Sink matching OmniBeam §5.3."""

    resource_path: str
    method: OmniBeamHttpMethod = DestinationDefaults.rest_api_method  # type: ignore[assignment]
    batch_size: int = DestinationDefaults.rest_api_batch_size
    body_envelope: str = ""
    rate_limit_rps: int = DestinationDefaults.rest_api_rate_limit_rps
    timeout_ms: int = DestinationDefaults.rest_api_timeout_ms
    max_retries: int = DestinationDefaults.rest_api_max_retries


class OmniBeamRestApiDestinationConfig(BaseModel):
    """REST HTTP API sink export configuration matching OmniBeam §5.3."""

    type: Literal["rest_api", "http_api"] = "rest_api"
    endpoint: OmniBeamApiEndpointConfig
    api_options: OmniBeamApiOptions


DestinationConfigUnion = Annotated[
    OmniBeamStorageDestinationConfig
    | OmniBeamBigQueryDestinationConfig
    | OmniBeamRestApiDestinationConfig,
    Field(discriminator="type"),
]

OmniBeamDestinationConfig = OmniBeamStorageDestinationConfig


# =============================================================================
# Data Quality, DLQ and Security (§7)
# =============================================================================


class OmniBeamDlqConfig(BaseModel):
    """Dead-Letter Queue configuration matching OmniBeam §7.1."""

    enabled: bool = DlqDefaults.enabled
    quarantine_path: str
    max_error_percentage: float = DlqDefaults.max_error_percentage


class OmniBeamQualityRule(BaseModel):
    """Data quality rule assertions matching OmniBeam §7.2."""

    type: OmniBeamQualityRuleType
    column: str | None = None
    values: list[Any] | None = None
    value: int | float | None = None

    @model_validator(mode="after")
    def validate_rule_parameters(self) -> OmniBeamQualityRule:
        if self.type == "not_null" and not self.column:
            raise ValueError("Rule 'not_null' requires 'column'.")
        if self.type == "accepted_values" and (not self.column or not self.values):
            raise ValueError("Rule 'accepted_values' requires both 'column' and 'values'.")
        if self.type in ("row_count_min", "row_count_max") and self.value is None:
            raise ValueError(f"Rule '{self.type}' requires 'value'.")
        return self


class OmniBeamQualityConfig(BaseModel):
    """Data quality assertions wrapper matching OmniBeam §7.2."""

    rules: list[OmniBeamQualityRule] = Field(default_factory=list)


class OmniBeamSecurityConfig(BaseModel):
    """Sensitive fields list for PII redaction matching OmniBeam §7.3."""

    sensitive_fields: list[str] = Field(default_factory=list)


# =============================================================================
# Root Manifest Specification (§2)
# =============================================================================


class OmniBeamManifest(BaseModel):
    """Root OmniBeam Pipeline configuration object strictly matching OmniBeam §2."""

    pipeline_id: str
    run_id: str
    pipeline_type: OmniBeamPipelineType = OmniBeamDefaults.pipeline_type  # type: ignore[assignment]
    runner: OmniBeamRunnerType = OmniBeamDefaults.runner  # type: ignore[assignment]
    secrets_config: OmniBeamSecretsConfig | None = None
    schema_: OmniBeamSchemaWrapper | None = Field(default=None, alias="schema")

    # Exactly one source configuration must be populated (OmniBeam §2 & §4)
    source: StorageSourceConfig | None = None
    database_source: DatabaseSourceConfig | None = None
    api_source: RestApiSourceConfig | None = None

    destination: DestinationConfigUnion
    dlq_config: OmniBeamDlqConfig
    quality_config: OmniBeamQualityConfig | None = None
    security: OmniBeamSecurityConfig | None = None

    @model_validator(mode="after")
    def validate_exactly_one_source(self) -> OmniBeamManifest:
        populated = [
            name
            for name, val in [
                ("source", self.source),
                ("database_source", self.database_source),
                ("api_source", self.api_source),
            ]
            if val is not None
        ]
        if len(populated) == 0:
            raise ValueError(
                "Exactly one source must be populated ('source', 'database_source', or 'api_source'). "
                "None was provided."
            )
        if len(populated) > 1:
            raise ValueError(
                f"Sources are mutually exclusive. Exactly one source must be populated, "
                f"but found: {', '.join(populated)}."
            )
        return self

    def to_json(self) -> str:
        return self.model_dump_json(by_alias=True, indent=2, exclude_none=True)
