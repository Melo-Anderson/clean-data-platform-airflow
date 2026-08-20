from __future__ import annotations

import uuid

from app.application.shared.ports.catalog_port import CatalogPort
from app.application.shared.ports.dwh_provisioner_port import DwhProvisionerPort
from app.application.shared.ports.notification_port import NotificationPort
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class RegisterAssetUseCase:
    """
    Orchestrates DataAsset registration within a single UoW transaction.

    After commit: catalog publish, DWH dataset provisioning, and notification dispatch
    happen outside the transaction to avoid blocking the DB session on external API calls.
    """

    def __init__(
        self,
        uow: UnitOfWork,
        catalog: CatalogPort,
        notifications: NotificationPort,
        dwh_provisioner: DwhProvisionerPort | None = None,
    ) -> None:
        self._uow = uow
        self._catalog = catalog
        self._notifications = notifications
        self._dwh_provisioner = dwh_provisioner

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
        asset = DataAsset(
            id=str(uuid.uuid4()),
            name=name,
            description=description,
            owner=EmailAddress(owner_email),
            tags=tags,
            policy_tags=[PolicyTag(t) for t in policy_tags],
            state=AssetState.DRAFT,
            discovery_schedule=CronSchedule(discovery_schedule),
            discovery_scope=DiscoveryScope(
                include=discovery_scope_include,
                exclude=discovery_scope_exclude,
            ),
        )
        async with self._uow:
            saved = await self._uow.assets.save(asset)
            await self._uow.commit()

        labels = {
            "managed_by": "clean_data_platform",
            "owner": saved.owner.value.replace("@", "_at_"),
        }
        for tag in saved.tags:
            labels[tag] = "true"

        if self._dwh_provisioner:
            await self._dwh_provisioner.ensure_dataset_exists(
                dataset_id=saved.name,
                description=saved.description,
                labels=labels,
            )

        await self._catalog.publish_asset(
            asset_id=saved.id, name=saved.name, state=saved.state.value, metadata={}
        )
        await self._notifications.send_alert(
            channel="#data-platform",
            title="New Data Asset Registered",
            message=f"Asset {saved.name} was registered in {saved.state.value} state.",
            level="info",
        )
        return saved
