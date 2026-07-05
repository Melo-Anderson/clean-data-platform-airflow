# tests/integration/repositories/test_sql_asset_repository.py
from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.assets.asset_state import AssetState
from app.domain.assets.data_asset import DataAsset
from app.domain.shared.policy_tag import PolicyTag
from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress
from app.infrastructure.persistence.repositories.sql_asset_repository import SqlAssetRepository


def _asset(name: str | None = None) -> DataAsset:
    return DataAsset(
        id=str(uuid.uuid4()),
        name=name or f"asset_{uuid.uuid4().hex[:6]}",
        description="Integration test asset",
        owner=EmailAddress("po@co.com"),
        tags=["core"],
        policy_tags=[PolicyTag.PII],
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(include=["customers"], exclude=[]),
    )


@pytest.mark.asyncio
async def test_save_and_find_by_id(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    found = await repo.find_by_id(asset.id)
    assert found is not None
    assert found.owner.value == "po@co.com"
    assert PolicyTag.PII in found.policy_tags
    assert "customers" in found.discovery_scope.include


@pytest.mark.asyncio
async def test_timestamps_populated_on_save(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    assert asset.created_at is not None
    assert asset.updated_at is not None


@pytest.mark.asyncio
async def test_update_state(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    updated = await repo.update_state(asset.id, AssetState.ACTIVE)
    assert updated.state == AssetState.ACTIVE


@pytest.mark.asyncio
async def test_discovery_scope_roundtrip(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    scope = DiscoveryScope(include=["orders", "products"], exclude=["temp_*"])
    asset = _asset()
    asset.discovery_scope = scope
    saved = await repo.save(asset)
    found = await repo.find_by_id(saved.id)
    assert found is not None
    assert set(found.discovery_scope.include) == {"orders", "products"}
    assert "temp_*" in found.discovery_scope.exclude


@pytest.mark.asyncio
async def test_find_by_name(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset(name="find_me"))
    found = await repo.find_by_name("find_me")
    assert found is not None
    assert found.id == asset.id


@pytest.mark.asyncio
async def test_update(db_session: AsyncSession) -> None:
    repo = SqlAssetRepository(db_session)
    asset = await repo.save(_asset())
    asset.description = "Updated description"
    asset.tags = ["updated"]
    updated = await repo.update(asset)
    
    found = await repo.find_by_id(asset.id)
    assert found is not None
    assert found.description == "Updated description"
    assert found.tags == ["updated"]
