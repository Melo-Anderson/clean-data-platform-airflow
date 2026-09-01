from __future__ import annotations

from datetime import datetime
from typing import Any, Self

from pydantic import BaseModel, Field, model_validator


class ExtractionObjectRequest(BaseModel):
    object_id: str
    load_strategy: str  # "full_load" | "incremental"
    page_size: int
    compression: str
    encoding: str
    watermark_column: str | None = None
    partition_column: str | None = None
    extraction_query: str | None = None
    credential_ref: str | None = None


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
    source_asset: str | None = None
    cron_schedule: str | None = None
    destination_asset: str | None = None
    destination_objects: list[dict[str, Any]] | None = None
    source_objects: list[ExtractionObjectRequest] | None = None
    compute: ComputeConfigRequest | None = None
    quality_rules: list[QualityRuleRequest] | None = None
    airflow_config: AirflowConfigRequest | None = None
    source_asset_id: str | None = None
    destination_asset_id: str | None = None

    @model_validator(mode="after")
    def check_export_destination(self) -> Self:
        src = self.source_asset or self.source_asset_id
        if not src:
            raise ValueError("source_asset is required")
        self.source_asset = src

        dest = self.destination_asset or self.destination_asset_id
        if dest:
            self.destination_asset = dest

        if self.pipeline_type == "export" and not self.destination_asset:
            raise ValueError("destination_asset is required for 'export' pipelines")
        return self


class PipelineResponse(BaseModel):
    id: str
    name: str
    pipeline_type: str
    owner_email: str
    source_asset: str = ""
    destination_asset: str | None = None
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
    status: str = "PROCESSED"
    processed_at: datetime | None = None


class PipelineRunRecordRequest(BaseModel):
    id: str | None = None
    pipeline_name: str
    pipeline_type: str = "ingestion"
    dag_run_id: str = "unknown"
    status: str = "success"
    started_at: datetime
    finished_at: datetime | None = None
    failed_task: str | None = None
    optional_failures: list[str] = []
    quality_violations: list[str] = []
    metrics: dict[str, Any] = Field(default_factory=dict)
    sla_minutes: int = 90
    sla_breached: bool = False
    files: list[PipelineRunRecordFileRequest] = []


class PipelineRunStatusCheckResponse(BaseModel):
    pipeline_id: str
    success: bool
    status: str | None = None
    logical_date: datetime | None = None


class FailureNotificationRequest(BaseModel):
    failed_task: str
    error_message: str | None = None
