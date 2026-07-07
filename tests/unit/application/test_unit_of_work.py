# tests/unit/application/test_unit_of_work.py
from __future__ import annotations

import uuid

import pytest

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.value_objects import (
    CronSchedule,
    DiscoveryScope,
    EmailAddress,
)


class FakeAssetRepo:
    def __init__(self) -> None:
        self._store: dict[str, DataAsset] = {}
        self.committed = False
        self.rolled_back = False

    async def save(self, asset: DataAsset) -> DataAsset:
        self._store[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._store.get(asset_id)

    async def find_by_name(self, name: str) -> object | None:
        return next((a for a in self._store.values() if getattr(a, "name", None) == name), None)

    async def update(self, asset: object) -> object:
        self._store[getattr(asset, "id")] = asset
        return asset

    async def update_state(self, asset_id: str, new_state: object) -> object:
        self._store[asset_id].state = new_state
        return self._store[asset_id]

    async def update_endpoint(self, asset_id: str, endpoint_id: str) -> DataAsset:
        self._store[asset_id].endpoint_id = endpoint_id
        return self._store[asset_id]

    async def update_scope(self, asset_id: str, scope: DiscoveryScope) -> DataAsset:
        self._store[asset_id].discovery_scope = scope
        return self._store[asset_id]


class FakeEndpointRepo:
    def __init__(self) -> None:
        self._store: dict[str, object] = {}

    async def save(self, endpoint: object) -> object:
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> object | None:
        return None

    async def find_by_name(self, name: str) -> object | None:
        return None


class FakeUnitOfWork:
    """Named fake UoW for use case tests."""

    def __init__(self) -> None:
        self.assets = FakeAssetRepo()
        self.endpoints = FakeEndpointRepo()
        self._committed = False
        self._rolled_back = False

    async def commit(self) -> None:
        self._committed = True

    async def rollback(self) -> None:
        self._rolled_back = True

    async def __aenter__(self) -> FakeUnitOfWork:
        return self

    async def __aexit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        if exc_type is not None:
            await self.rollback()


@pytest.mark.asyncio
async def test_uow_commit_is_called_on_success() -> None:
    uow = FakeUnitOfWork()
    async with uow:
        asset = DataAsset(
            id=str(uuid.uuid4()),
            name="test",
            description="desc",
            owner=EmailAddress("po@co.com"),
            discovery_schedule=CronSchedule("0 6 * * *"),
        )
        await uow.assets.save(asset)
        await uow.commit()
    assert uow._committed is True


@pytest.mark.asyncio
async def test_uow_rollback_is_called_on_exception() -> None:
    uow = FakeUnitOfWork()
    with pytest.raises(RuntimeError):
        async with uow:
            raise RuntimeError("something went wrong")
    assert uow._rolled_back is True
