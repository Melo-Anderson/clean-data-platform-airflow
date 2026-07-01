from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.assets.asset_repository import AssetRepository
from app.domain.endpoints.endpoint_repository import EndpointRepository


@runtime_checkable
class UnitOfWork(Protocol):
    """
    Unit of Work: groups repositories under a single transactional boundary.

    All use cases that perform writes must use a UoW to ensure atomicity.
    This covers: create asset + emit audit log + publish catalog + send notification.

    The UoW is a context manager: use it with `async with`:

    Example:
        async with uow:
            asset = await uow.assets.save(new_asset)
            await uow.commit()
        # Side effects (catalog, notifications) dispatched after commit
    """

    assets: AssetRepository
    endpoints: EndpointRepository

    async def commit(self) -> None: ...

    async def rollback(self) -> None: ...

    async def __aenter__(self) -> UnitOfWork: ...

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: object,
    ) -> None: ...
