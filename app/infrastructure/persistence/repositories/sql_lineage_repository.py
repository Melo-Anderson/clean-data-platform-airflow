from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.lineage.lineage_mapping import LineageMapping


class SqlLineageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, mapping: LineageMapping) -> LineageMapping:
        # Implementation placeholder
        return mapping

    async def find_by_pipeline_id(self, pipeline_id: str) -> list[LineageMapping]:
        # Implementation placeholder
        return []

    async def find_by_destination_object_id(
        self, destination_object_id: str
    ) -> list[LineageMapping]:
        # Implementation placeholder
        return []

    async def find_by_id(self, lineage_mapping_id: str) -> LineageMapping | None:
        # Implementation placeholder
        return None

    async def find_graph_neighborhood(self, object_id: str, direction: str) -> list[LineageMapping]:
        # Implementation placeholder
        return []
