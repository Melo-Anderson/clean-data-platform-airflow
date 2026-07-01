from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset


class ActivateAssetUseCase:
    """Transitions DataAsset DRAFT → ACTIVE within a UoW transaction."""

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(self, asset_id: str, endpoint_id: str) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            asset = await service.transition_to_active(asset_id, endpoint_id)
            await self._uow.commit()
        return asset
