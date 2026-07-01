# tests/unit/domain/assets/test_asset_service.py
from __future__ import annotations

import uuid

import pytest

from app.domain.assets.asset_service import (
    AssetNotFoundError,
    AssetService,
    InvalidStateTransitionError,
)
from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress


class FakeAssetRepository:
    """Named fake implementing AssetRepository Protocol. Used in unit tests only."""

    def __init__(self) -> None:
        self._store: dict[str, DataAsset] = {}

    async def save(self, asset: DataAsset) -> DataAsset:
        self._store[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._store.get(asset_id)

    async def update_state(self, asset_id: str, new_state: AssetState) -> DataAsset:
        asset = self._store[asset_id]
        asset.state = new_state
        asset.touch()
        return asset

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        asset = self._store[asset_id]
        asset.endpoint_id = endpoint_id
        asset.touch()
        return asset

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        asset = self._store[asset_id]
        asset.discovery_scope = scope
        asset.touch()
        return asset


def _new_service() -> tuple[AssetService, FakeAssetRepository]:
    repo = FakeAssetRepository()
    return AssetService(repo=repo), repo


async def _registered_asset(service: AssetService, name: str = "customers") -> DataAsset:
    return await service.register(
        asset_id=str(uuid.uuid4()),
        name=name,
        description="Test asset",
        owner=EmailAddress("po@co.com"),
        tags=["core"],
        policy_tags=[PolicyTag.PII],
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(),
    )


@pytest.mark.asyncio
async def test_register_creates_asset_in_draft() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    assert asset.state == AssetState.DRAFT
    assert PolicyTag.PII in asset.policy_tags


@pytest.mark.asyncio
async def test_transition_to_active_sets_endpoint_and_state() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    activated = await service.transition_to_active(asset.id, endpoint_id="ep-uuid-1")
    assert activated.state == AssetState.ACTIVE
    assert activated.endpoint_id == "ep-uuid-1"


@pytest.mark.asyncio
async def test_invalid_transition_from_archived_raises_error() -> None:
    service, repo = _new_service()
    asset = await _registered_asset(service)
    await repo.update_state(asset.id, AssetState.ARCHIVED)
    with pytest.raises(InvalidStateTransitionError) as exc:
        await service.transition_to_active(asset.id, endpoint_id="ep-1")
    assert "archived" in str(exc.value)


@pytest.mark.asyncio
async def test_asset_not_found_raises_error() -> None:
    service, _ = _new_service()
    with pytest.raises(AssetNotFoundError):
        await service.transition_to_active("nonexistent", endpoint_id="ep-1")


@pytest.mark.asyncio
async def test_update_scope_replaces_discovery_scope() -> None:
    service, _ = _new_service()
    asset = await _registered_asset(service)
    new_scope = DiscoveryScope(include=["orders"], exclude=["temp_*"])
    updated = await service.update_scope(asset.id, new_scope)
    assert "orders" in updated.discovery_scope.include
    assert "temp_*" in updated.discovery_scope.exclude
