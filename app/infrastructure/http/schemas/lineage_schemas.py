from __future__ import annotations

from pydantic import BaseModel, Field


class LineageNodeSchema(BaseModel):
    object_id: str
    column_name: str
    transformation: str | None = None


class LineageGraphResponse(BaseModel):
    upstream: list[LineageNodeSchema] = Field(default_factory=list)
    downstream: list[LineageNodeSchema] = Field(default_factory=list)
