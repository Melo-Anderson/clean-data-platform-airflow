from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field


class TriggerDiscoveryRequest(BaseModel):
    triggered_by: str = Field(
        ..., description="Who or what triggered this run (e.g. 'manual', 'scheduler')"
    )


class DiscoveryRunResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    asset_id: str
    triggered_by: str
    status: str
    started_at: datetime | None
    completed_at: datetime | None
    objects_discovered: int
    fields_discovered: int
    error_message: str | None


class DriftDecisionRequest(BaseModel):
    decision: str = Field(..., description="'approved' or 'rejected'")
    decided_by: str
    notes: str | None = None


class DriftApprovalResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    discovery_run_id: str
    asset_id: str
    object_id: str
    field_name: str | None
    change_type: str
    severity_description: str
    decision: str
    decided_by: str | None
    decided_at: datetime | None
    owner_notes: str | None
