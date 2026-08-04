from __future__ import annotations

from typing import Any, Self

from pydantic import BaseModel, Field, model_validator

from app.config import get_settings


class ExtractionObjectRequest(BaseModel):
    object_id: str
    load_strategy: str = Field(
        default_factory=lambda: get_settings().default_load_strategy
    )  # "full_load" | "incremental"
    watermark_column: str | None = None
    page_size: int = Field(default_factory=lambda: get_settings().default_page_size)
    partition_column: str | None = None
    compression: str = Field(default_factory=lambda: get_settings().default_compression)
    encoding: str = Field(default_factory=lambda: get_settings().default_encoding)
    extraction_query: str | None = None


class ComputeConfigRequest(BaseModel):
    engine: str = Field(
        default_factory=lambda: get_settings().default_compute_engine
    )  # "duckdb" | "rest_api" | "spark"
    staging_bucket: str = Field(
        default_factory=lambda: get_settings().default_compute_staging_bucket
    )
    num_workers: int = Field(default_factory=lambda: get_settings().default_compute_num_workers)
    machine_type: str = Field(default_factory=lambda: get_settings().default_compute_machine_type)


class QualityRuleRequest(BaseModel):
    type: str  # "not_null" | "row_count_min" | "completeness" etc.
    column: str | None = None
    value: float | None = None


class AirflowConfigRequest(BaseModel):
    retries: int = Field(default_factory=lambda: get_settings().default_airflow_retries)
    retry_delay_minutes: int = Field(
        default_factory=lambda: get_settings().default_airflow_retry_delay_minutes
    )
    execution_timeout_minutes: int = Field(
        default_factory=lambda: get_settings().default_airflow_execution_timeout_minutes
    )
    sla_minutes: int = Field(default_factory=lambda: get_settings().default_airflow_sla_minutes)
    tags: list[str] = Field(default_factory=list)
    pool: str = Field(default_factory=lambda: get_settings().default_airflow_pool)


class CreatePipelineRequest(BaseModel):
    name: str
    pipeline_type: str  # "ingestion" | "etl" | "export"
    owner_email: str
    source_asset_id: str
    cron_schedule: str
    destination_asset_id: str | None = None
    destination_objects: list[dict[str, Any]] | None = None
    source_objects: list[ExtractionObjectRequest] | None = None
    compute: ComputeConfigRequest | None = None
    quality_rules: list[QualityRuleRequest] | None = None
    airflow_config: AirflowConfigRequest | None = None

    @model_validator(mode="after")
    def check_export_destination(self) -> Self:
        if self.pipeline_type == "export" and not self.destination_asset_id:
            raise ValueError("destination_asset_id is required for 'export' pipelines")
        return self


class PipelineResponse(BaseModel):
    id: str
    name: str
    pipeline_type: str
    owner_email: str
    source_asset_id: str
    cron_schedule: str | None = None


class TriggerRunRequest(BaseModel):
    triggered_by: str


class PipelineRunResponse(BaseModel):
    id: str
    pipeline_id: str
    pipeline_name: str
    dag_run_id: str
    status: str


class QualityGateReportRequest(BaseModel):
    metrics: dict[str, Any]


class QualityGateReportResponse(BaseModel):
    run_id: str
    status: str
    violations: list[str]
