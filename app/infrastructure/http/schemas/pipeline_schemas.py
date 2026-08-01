from __future__ import annotations

from typing import Any, Self

from pydantic import BaseModel, model_validator


class ExtractionObjectRequest(BaseModel):
    object_id: str
    load_strategy: str = "full_load"  # "full_load" | "incremental"
    watermark_column: str | None = None
    page_size: int = 1000
    partition_column: str | None = None
    compression: str = "snappy"
    encoding: str = "utf-8"
    extraction_query: str | None = None


class ComputeConfigRequest(BaseModel):
    engine: str = "duckdb"  # "duckdb" | "rest_api" | "spark"
    staging_bucket: str = ""
    num_workers: int = 1
    machine_type: str = "n1-standard-2"


class QualityRuleRequest(BaseModel):
    type: str  # "not_null" | "row_count_min" | "completeness" etc.
    column: str | None = None
    value: float | None = None


class AirflowConfigRequest(BaseModel):
    retries: int = 3
    retry_delay_minutes: int = 5
    execution_timeout_minutes: int = 120
    sla_minutes: int = 90
    tags: list[str] = []
    pool: str = "default_pool"


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
