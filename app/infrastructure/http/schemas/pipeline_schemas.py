from __future__ import annotations

from datetime import date, datetime
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


class ExtractionObjectRequest(BaseModel):
    object_name: str
    load_strategy: str  # "full_load" | "incremental"
    page_size: int
    compression: str
    encoding: str
    watermark_column: str | None = None
    partition_column: str | None = None
    extraction_query: str | None = None
    credential_ref: str | None = None


class DestinationObjectRequest(BaseModel):
    object_name: str
    create_if_not_exists: bool


class ComputeConfigRequest(BaseModel):
    engine: str  # "duckdb" | "rest_api" | "spark"
    staging_bucket: str
    num_workers: int
    machine_type: str


class QualityRuleRequest(BaseModel):
    type: str  # "not_null" | "row_count_min" | "completeness" etc.
    column: str | None = None
    value: float | None = None


class AirflowConfigRequest(BaseModel):
    retries: int
    retry_delay_minutes: int
    execution_timeout_minutes: int
    sla_minutes: int
    tags: list[str]
    pool: str


class CreatePipelineRequest(BaseModel):
    name: str
    pipeline_type: str  # "ingestion" | "etl" | "export" | "transformation"
    owner_email: str
    source_asset_name: str
    cron_schedule: str | None = None
    destination_asset_name: str | None = None
    destination_objects: list[DestinationObjectRequest] | None = None
    source_objects: list[ExtractionObjectRequest] | None = None
    compute: ComputeConfigRequest | None = None
    quality_rules: list[QualityRuleRequest] | None = None
    airflow_config: AirflowConfigRequest | None = None

    @model_validator(mode="after")
    def check_export_destination(self) -> Self:
        if self.pipeline_type == "export" and not self.destination_asset_name:
            raise ValueError("destination_asset_name is required for 'export' pipelines")
        return self


class PipelineResponse(BaseModel):
    id: str
    name: str
    pipeline_type: str
    owner_email: str
    source_asset_name: str
    destination_asset_name: str | None = None
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


class PipelineRunRecordFileRequest(BaseModel):
    id: str | None = None
    file_path: str
    file_name: str
    file_size_bytes: int
    mtime: datetime
    hash_md5: str
    status: str
    processed_at: datetime | None = None


class PipelineRunRecordRequest(BaseModel):
    id: str | None = None
    pipeline_name: str
    pipeline_type: str
    dag_run_id: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    failed_task: str | None = None
    optional_failures: list[str] = Field(default_factory=list)
    quality_violations: list[str] = Field(default_factory=list)
    metrics: dict[str, Any] = Field(default_factory=dict)
    sla_minutes: int
    sla_breached: bool
    files: list[PipelineRunRecordFileRequest] = Field(default_factory=list)


class PipelineRunStatusCheckResponse(BaseModel):
    pipeline_id: str
    success: bool
    status: str | None = None
    logical_date: datetime | None = None


class FailureNotificationRequest(BaseModel):
    failed_task: str
    error_message: str | None = None


class BackfillRequest(BaseModel):
    from_date: date
    to_date: date


class BackfillResponse(BaseModel):
    backfill_id: str
    pipeline_id: str
