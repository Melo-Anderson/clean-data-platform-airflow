from __future__ import annotations

from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset
from app.infrastructure.adapters.catalog.catalog_adapter import CatalogAdapter
from app.infrastructure.adapters.notifications.notification_adapter import NotificationAdapter


class ActivateAssetUseCase:
    """Transitions DataAsset DRAFT → ACTIVE within a UoW transaction."""

    def __init__(
        self, uow: UnitOfWork, catalog: CatalogAdapter, notifications: NotificationAdapter
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications

    async def execute(self, asset_id: str, endpoint_id: str) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            asset = await service.transition_to_active(asset_id, endpoint_id)
            await self._uow.commit()

        await self._catalog.publish_asset(
            asset_id=asset.id,
            name=asset.name,
            state=asset.state.value,
            metadata={"endpoint_id": endpoint_id},
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="Data Asset Activated",
            message=f"Asset {asset.name} is now ACTIVE.",
            level="info",
        )
        return asset
