from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.lineage.lineage_mapping import LineageMapping


@runtime_checkable
class LineageRepository(Protocol):
    """Repository interface for LineageMapping persistence."""

    async def save(self, mapping: LineageMapping) -> LineageMapping: ...
    async def find_by_pipeline_id(self, pipeline_id: str) -> list[LineageMapping]: ...
    async def find_by_destination_object_id(
        self, destination_object_id: str
    ) -> list[LineageMapping]: ...
    async def find_by_id(self, lineage_mapping_id: str) -> LineageMapping | None: ...
    async def find_graph_neighborhood(
        self, object_id: str, direction: str
    ) -> list[LineageMapping]: ...
