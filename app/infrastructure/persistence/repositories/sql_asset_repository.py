from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.assets.asset_service import AssetNotFoundError
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.persistence.models.data_asset_model import DataAssetModel


def _to_domain(m: DataAssetModel) -> DataAsset:
    """Map ORM model → domain entity. No business logic."""
    return DataAsset(
        id=m.id,
        name=m.name,
        description=m.description,
        owner=EmailAddress(m.owner_email),
        tags=list(m.tags),
        policy_tags=[PolicyTag(t) for t in m.policy_tags],
        state=AssetState(m.state),
        discovery_schedule=CronSchedule(m.discovery_schedule),
        discovery_scope=DiscoveryScope.from_dict(m.discovery_scope),
        endpoint_id=m.endpoint_id,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


def _to_model(asset: DataAsset) -> DataAssetModel:
    """Map domain entity → ORM model. No business logic."""
    return DataAssetModel(
        id=asset.id,
        name=asset.name,
        description=asset.description,
        owner_email=asset.owner.value,
        tags=asset.tags,
        policy_tags=[t.value for t in asset.policy_tags],
        state=asset.state.value,
        discovery_schedule=asset.discovery_schedule.expression,
        discovery_scope=asset.discovery_scope.to_dict(),
        endpoint_id=asset.endpoint_id,
    )


class SqlAssetRepository:
    """SQLAlchemy implementation of AssetRepository. Infrastructure only."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, asset: DataAsset) -> DataAsset:
        model = _to_model(asset)
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        result = await self._session.execute(select(DataAssetModel).where(DataAssetModel.id == asset_id))
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.state = new_state.value
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.endpoint_id = endpoint_id
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        model = await self._fetch_or_raise(asset_id)
        model.discovery_scope = scope.to_dict()
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def _fetch_or_raise(self, asset_id: str) -> DataAssetModel:
        result = await self._session.execute(select(DataAssetModel).where(DataAssetModel.id == asset_id))
        model = result.scalar_one_or_none()
        if model is None:
            raise AssetNotFoundError(asset_id)
        return model
