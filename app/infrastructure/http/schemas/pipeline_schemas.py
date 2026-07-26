from __future__ import annotations

from typing import Any, Self

from pydantic import BaseModel, model_validator


class CreatePipelineRequest(BaseModel):
    name: str
    pipeline_type: str  # "ingestion" | "etl" | "export"
    owner_email: str
    source_asset_id: str
    cron_schedule: str  # ex: "0 0 * * *"
    destination_asset_id: str | None = None

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
