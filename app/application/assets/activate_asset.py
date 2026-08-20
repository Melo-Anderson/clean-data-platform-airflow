from __future__ import annotations

from app.application.shared.ports.catalog_port import CatalogPort
from app.application.shared.ports.notification_port import NotificationPort
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.exceptions import PlatformNotFoundError


class ActivateAssetUseCase:
    """Transitions DataAsset DRAFT → ACTIVE within a UoW transaction."""

    def __init__(
        self, uow: UnitOfWork, catalog: CatalogPort, notifications: NotificationPort
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications

    async def execute(self, asset_id: str, endpoint_id: str) -> DataAsset:
        async with self._uow:
            asset = await self._uow.assets.find_by_id(asset_id)
            if asset is None:
                raise PlatformNotFoundError(f"DataAsset not found: {asset_id}")

            asset.activate(endpoint_id)
            await self._uow.assets.update_endpoint(asset_id, endpoint_id)
            await self._uow.assets.update_state(asset_id, AssetState.ACTIVE)

            self._uow.audit_logs.save(
                event_type="asset.activated",
                entity_type="DataAsset",
                entity_id=asset.id,
                actor_id="system",
                actor_email="system@platform.local",
                payload={"status": "ACTIVE"},
                description=f"Asset {asset.name} activated",
            )
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
