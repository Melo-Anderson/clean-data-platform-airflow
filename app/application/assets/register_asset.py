from __future__ import annotations

import uuid

from app.application.unit_of_work import UnitOfWork
from app.domain.assets.asset_service import AssetService
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class RegisterAssetUseCase:
    """
    Orchestrates DataAsset registration within a single UoW transaction.

    After commit: catalog publish and notification dispatch happen outside the transaction
    to avoid blocking the DB session on external HTTP calls.

    Example:
        use_case = RegisterAssetUseCase(uow=sql_uow, catalog=noop_adapter, notifications=noop_adapter)
        asset = await use_case.execute(name="customers", owner_email="po@co.com", ...)
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

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
        return asset
