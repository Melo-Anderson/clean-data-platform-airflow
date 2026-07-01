from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.domain.assets.asset_repository import AssetRepository
from app.domain.endpoints.endpoint_repository import EndpointRepository
from app.infrastructure.persistence.repositories.sql_asset_repository import (
    SqlAssetRepository,
)
from app.infrastructure.persistence.repositories.sql_data_object_repository import (
    SqlDataObjectRepository,
)
from app.infrastructure.persistence.repositories.sql_endpoint_repository import (
    SqlEndpointRepository,
)
from app.infrastructure.persistence.repositories.sql_lineage_repository import SqlLineageRepository
from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
    SqlPipelineRepository,
)


class SqlUnitOfWork:
    """
    SQLAlchemy implementation of UnitOfWork.

    Manages the AsyncSession lifecycle and exposes typed repositories.
    Creates repositories per-transaction so they share the same session.

    Example:
        async with SqlUnitOfWork(session_factory) as uow:
            asset = await uow.assets.save(new_asset)
            await uow.commit()
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory
        self._session: AsyncSession | None = None

    @property
    def assets(self) -> AssetRepository:
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlAssetRepository(self._session)

    @property
    def endpoints(self) -> EndpointRepository:
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlEndpointRepository(self._session)

    @property
    def objects(self):
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlDataObjectRepository(self._session)

    @property
    def pipelines(self):
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlPipelineRepository(self._session)

    @property
    def lineage(self):
        assert self._session is not None, "UoW must be used as a context manager"
        return SqlLineageRepository(self._session)

    async def commit(self) -> None:
        assert self._session is not None
        await self._session.commit()

    async def rollback(self) -> None:
        assert self._session is not None
        await self._session.rollback()

    async def __aenter__(self) -> SqlUnitOfWork:
        self._session = self._session_factory()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None:
        if exc_type is not None:
            await self.rollback()
        if self._session is not None:
            await self._session.close()
            self._session = None
