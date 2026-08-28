from __future__ import annotations

from pydantic import BaseModel, Field


class LineageNodeSchema(BaseModel):
    object_id: str
    column_name: str
    transformation: str | None = None


class LineageGraphResponse(BaseModel):
    upstream: list[LineageNodeSchema] = Field(default_factory=list)
    downstream: list[LineageNodeSchema] = Field(default_factory=list)


class RawLineageRequest(BaseModel):
    pipeline_id: str
    source_object_ids: list[str] = Field(default_factory=list)
    destination_object_ids: list[str] = Field(default_factory=list)
    schema_path: str | None = None


class FreshnessUpdateRequest(BaseModel):
    pipeline_id: str
    destination_object_ids: list[str] = Field(default_factory=list)


class EtlLineageRequest(BaseModel):
    pipeline_id: str
    transform_ref: str
    schema_path: str | None = None


class ExportLineageRequest(BaseModel):
    pipeline_id: str
    source_object_ids: list[str] = Field(default_factory=list)
    destination_object_ids: list[str] = Field(default_factory=list)
    schema_path: str | None = None


class LineageEventResponse(BaseModel):
    status: str = "ok"
    pipeline_id: str
    message: str | None = None
