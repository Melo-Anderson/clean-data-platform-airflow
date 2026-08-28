from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field


class OmniBeamFieldSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True
    scale: int | None = None
    format: str | None = None


class OmniBeamSchemaWrapper(BaseModel):
    fields: list[OmniBeamFieldSchema]


class StorageSourceConfig(BaseModel):
    type: Literal["storage"] = "storage"
    paths: list[str] = Field(default_factory=list)
    format: str = "csv"
    delimiter: str = ","
    quote_char: str = '"'
    multiline: bool = False
    charset: str = "utf-8"
    compression: str = "none"
    chunk_size_bytes: int = 16777216
    schema_: OmniBeamSchemaWrapper = Field(..., alias="schema")


class DatabaseSourceConfig(BaseModel):
    type: Literal["database"] = "database"
    credential_ref: str
    table: str | None = None
    query: str | None = None
    partition_column: str | None = None
    num_partitions: int = 1
    watermark_column: str | None = None
    watermark_value: str | None = None
    schema_: OmniBeamSchemaWrapper = Field(..., alias="schema")


class RestApiSourceConfig(BaseModel):
    type: Literal["rest_api"] = "rest_api"
    base_url: str
    path: str
    auth_type: str = ""
    pagination_strategy: str = "page_number"
    schema_: OmniBeamSchemaWrapper = Field(..., alias="schema")


class MongoSourceConfig(BaseModel):
    type: Literal["mongodb"] = "mongodb"
    credential_ref: str
    database: str
    collection: str
    filter_json: str | None = None
    schema_: OmniBeamSchemaWrapper = Field(..., alias="schema")


SourceConfigUnion = Annotated[
    StorageSourceConfig | DatabaseSourceConfig | RestApiSourceConfig | MongoSourceConfig,
    Field(discriminator="type"),
]

# Alias for backwards compatibility / ergonomic file storage config
OmniBeamSourceConfig = StorageSourceConfig


class OmniBeamDestinationConfig(BaseModel):
    type: str = "storage"
    output_path: str
    output_format: str = "parquet"
    compression: str = "zstd"
    single_file: bool = False
    include_audit_columns: bool = True


class OmniBeamDlqConfig(BaseModel):
    enabled: bool = True
    quarantine_path: str
    max_error_percentage: float = 5.0


class OmniBeamQualityRule(BaseModel):
    type: str
    column: str | None = None


class OmniBeamQualityConfig(BaseModel):
    rules: list[OmniBeamQualityRule] = Field(default_factory=list)


class OmniBeamSecurityConfig(BaseModel):
    sensitive_fields: list[str] = Field(default_factory=list)


class OmniBeamManifest(BaseModel):
    pipeline_id: str
    run_id: str
    pipeline_type: str = "ingestion"
    runner: str = "dataflow"
    source: SourceConfigUnion
    destination: OmniBeamDestinationConfig
    dlq_config: OmniBeamDlqConfig
    quality_config: OmniBeamQualityConfig
    security: OmniBeamSecurityConfig

    def to_json(self) -> str:
        return self.model_dump_json(by_alias=True, indent=2)
