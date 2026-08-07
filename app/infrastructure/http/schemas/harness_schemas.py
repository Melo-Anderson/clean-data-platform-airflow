from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


class ValidationRequest(BaseModel):
    pipeline_yaml: str = Field(description="Raw YAML content of the pipeline configuration.")
    pipeline_type: str = Field(
        default="ingestion", description="Type of pipeline (ingestion, export, etl)."
    )
    endpoint_type: str = Field(
        default="relational",
        description="Source endpoint type (relational, mongodb, rest_api).",
    )


class ValidationErrorDetail(BaseModel):
    json_pointer: str = Field(
        description="JSON Pointer (RFC 6901) indicating the exact field with the error."
    )
    error_code: str = Field(description="Categorized error code (e.g., MISSING_ID, INVALID_SQL).")
    message: str = Field(description="Technical message describing the problem.")
    suggestion: str = Field(description="Direct and actionable corrective suggestion.")


class ValidationResponse(BaseModel):
    is_valid: bool = Field(description="True if the YAML meets all platform requirements.")
    errors: list[ValidationErrorDetail] = Field(
        default_factory=list, description="List of validation errors; empty if is_valid is True."
    )


class HarnessSchemaResponse(BaseModel):
    type: str = Field(description="The root type of the schema (usually 'object').")
    properties: dict[str, Any] = Field(description="The properties defined in the schema.")

    model_config = {"extra": "allow"}


from pydantic import AliasChoices  # noqa: E402


class RelationalExtractionObjectSpec(BaseModel):
    object_id: str = Field(description="Identifier of the source table or view")
    load_strategy: Literal["full_load", "incremental", "cdc"] = Field(
        description="Extraction strategy"
    )
    watermark_column: str | None = Field(default=None, description="Column for incremental control")
    extraction_query: str | None = Field(default=None, description="Custom SQL override")
    credential_ref: str | None = Field(
        default=None, description="Secret manager key for source credentials"
    )


class MongoExtractionObjectSpec(BaseModel):
    collection_name: str = Field(description="MongoDB collection name")
    load_strategy: Literal["full_load", "incremental"] = Field(
        description="Extraction strategy for NoSQL documents"
    )
    filter_query: dict[str, Any] | None = Field(
        default=None, description="BSON/JSON filter applied at extraction"
    )


class ApiExtractionObjectSpec(BaseModel):
    endpoint_path: str = Field(description="Relative path or full URL of the REST endpoint")
    http_method: Literal["GET", "POST"] = Field(default="GET", description="HTTP method")
    pagination_strategy: Literal["offset", "cursor", "page_number"] | None = Field(
        default=None, description="Pagination strategy for the API"
    )


class IngestionRelationalPipelineSpec(BaseModel):
    name: str = Field(
        validation_alias=AliasChoices("name", "pipeline_id"),
        description="Unique pipeline name (used as Airflow dag_id)",
    )
    pipeline_type: Literal["ingestion"] = Field(default="ingestion")
    owner_email: str = Field(description="Responsible team or person e-mail")
    cron_schedule: str = Field(description="Cron expression for scheduling")
    source_asset: str = Field(description="Source asset identifier (e.g. postgres_prod)")
    source_objects: list[RelationalExtractionObjectSpec] = Field(
        description="List of relational objects to extract"
    )
    destination_asset: str | None = Field(default=None)


class IngestionMongoPipelineSpec(BaseModel):
    name: str = Field(
        validation_alias=AliasChoices("name", "pipeline_id"),
        description="Unique pipeline name",
    )
    pipeline_type: Literal["ingestion"] = Field(default="ingestion")
    owner_email: str = Field(description="Responsible team or person e-mail")
    cron_schedule: str = Field(description="Cron expression for scheduling")
    source_asset: str = Field(description="Source MongoDB asset identifier")
    source_objects: list[MongoExtractionObjectSpec] = Field(
        description="List of MongoDB collections to extract"
    )
    destination_asset: str | None = Field(default=None)


class IngestionApiPipelineSpec(BaseModel):
    name: str = Field(
        validation_alias=AliasChoices("name", "pipeline_id"),
        description="Unique pipeline name",
    )
    pipeline_type: Literal["ingestion"] = Field(default="ingestion")
    owner_email: str = Field(description="Responsible team or person e-mail")
    cron_schedule: str = Field(description="Cron expression for scheduling")
    source_asset: str = Field(description="Source API asset identifier")
    source_objects: list[ApiExtractionObjectSpec] = Field(
        description="List of API endpoints to extract"
    )
    destination_asset: str | None = Field(default=None)


class ExportPipelineSpec(BaseModel):
    name: str = Field(
        validation_alias=AliasChoices("name", "pipeline_id"),
        description="Unique pipeline name",
    )
    pipeline_type: Literal["export"] = Field(default="export")
    owner_email: str = Field(description="Responsible team or person e-mail")
    cron_schedule: str = Field(description="Cron expression for scheduling")
    source_asset: str = Field(description="Source asset (data lake / DWH)")
    destination_asset: str = Field(description="Destination asset (e.g. external system)")


SCHEMA_FACTORY: dict[tuple[str, str], type[BaseModel]] = {
    ("ingestion", "relational"): IngestionRelationalPipelineSpec,
    ("ingestion", "mongodb"): IngestionMongoPipelineSpec,
    ("ingestion", "rest_api"): IngestionApiPipelineSpec,
    ("ingestion", "api"): IngestionApiPipelineSpec,
    ("export", "relational"): ExportPipelineSpec,
}


class HarnessGoldExamplesResponse(BaseModel):
    examples: list[dict[str, Any]] = Field(description="List of canonical gold standard examples.")


class PipelineYamlExportResponse(BaseModel):
    """Response model for the unauthenticated pipeline YAML export endpoint."""

    pipeline_id: str = Field(description="Unique identifier of the pipeline (matches dag_id).")
    pipeline_yaml: str = Field(description="Canonical YAML representation of the pipeline.")
