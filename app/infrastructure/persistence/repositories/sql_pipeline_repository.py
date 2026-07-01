from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline import Pipeline


class SqlPipelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, p: Pipeline) -> Pipeline:
        # Implementation placeholder
        return p

    async def find_by_id(self, pid: str) -> Pipeline | None:
        # Implementation placeholder
        return None

    async def find_all(self) -> list[Pipeline]:
        # Implementation placeholder
        return []

    async def update_schema_version(self, pid: str, sv: str) -> Pipeline:
        # Implementation placeholder
        raise NotImplementedError
