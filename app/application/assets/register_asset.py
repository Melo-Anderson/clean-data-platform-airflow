from __future__ import annotations

import uuid

from app.application.shared.adapters.catalog_adapter import CatalogAdapter
from app.application.shared.adapters.dwh_provisioner_adapter import DwhProvisionerAdapter
from app.application.shared.ports.notification_port import NotificationPort
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.dwh_provisioners.noop_provisioner import NoOpDwhProvisioner


class RegisterAssetUseCase:
    """
    Orchestrates DataAsset registration within a single UoW transaction.

    After commit: catalog publish, DWH dataset provisioning, and notification dispatch
    happen outside the transaction to avoid blocking the DB session on external API calls.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        catalog: CatalogAdapter,
        notifications: NotificationPort,
        dwh_provisioner: DwhProvisionerAdapter | None = None,
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications
        self._dwh_provisioner = dwh_provisioner or NoOpDwhProvisioner()

    async def execute(
        self,
        name: str,
        description: str,
        owner_email: str,
        tags: list[str],
        policy_tags: list[str],
        discovery_schedule: str,
        discovery_scope_include: list[str],
        discovery_scope_exclude: list[str],
    ) -> DataAsset:
        async with self._uow:
            service = AssetService(repo=self._uow.assets)
            asset = await service.register(
                asset_id=str(uuid.uuid4()),
                name=name,
                description=description,
                owner=EmailAddress(owner_email),
                tags=tags,
                policy_tags=[PolicyTag(t) for t in policy_tags],
                discovery_schedule=CronSchedule(discovery_schedule),
                discovery_scope=DiscoveryScope(
                    include=discovery_scope_include,
                    exclude=discovery_scope_exclude,
                ),
            )
            await self._uow.commit()

        labels = {
            "managed_by": "clean_data_platform",
            "owner": asset.owner.value.replace("@", "_at_"),
        }
        for tag in asset.tags:
            labels[tag] = "true"

        await self._dwh_provisioner.ensure_dataset_exists(
            dataset_id=asset.name,
            description=asset.description,
            labels=labels,
        )

        await self._catalog.publish_asset(
            asset_id=asset.id, name=asset.name, state=asset.state.value, metadata={}
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="New Data Asset Registered",
            message=f"Asset {asset.name} was registered in {asset.state.value} state.",
            level="info",
        )
        return asset
