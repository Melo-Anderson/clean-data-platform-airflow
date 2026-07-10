from __future__ import annotations

from app.application.shared.adapters.catalog_adapter import CatalogAdapter
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.infrastructure.adapters.notifications.notification_adapter import NotificationAdapter


class UpdateAssetUseCase:
    """Updates DataAsset fields and publishes the metadata delta to the catalog.

    Receives internal IDs only. Name resolution (asset_name -> asset_id,
    endpoint_name -> endpoint_id) is the Router's responsibility.
    """

    def __init__(
        self, uow: UnitOfWork, catalog: CatalogAdapter, notifications: NotificationAdapter
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications

    async def execute(
        self,
        asset_id: str,
        description: str | None = None,
        tags: list[str] | None = None,
        policy_tags: list[PolicyTag] | None = None,
        endpoint_id: str | None = None,
    ) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            updated = await service.update(
                asset_id,
                description=description,
                tags=tags,
                policy_tags=policy_tags,
                endpoint_id=endpoint_id,
            )
            await self._uow.commit()

        await self._catalog.publish_asset(
            asset_id=updated.id,
            name=updated.name,
            state=updated.state.value,
            metadata={"endpoint_id": updated.endpoint_id},
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="Data Asset Updated",
            message=f"Asset {updated.name} was successfully updated.",
            level="info",
        )
        return updated
