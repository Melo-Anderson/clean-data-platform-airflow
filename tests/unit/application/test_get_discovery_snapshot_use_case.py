# tests/unit/application/test_get_discovery_snapshot_use_case.py
from __future__ import annotations

import pytest

from app.application.discovery.get_discovery_snapshot_use_case import (
    GetDiscoverySnapshotUseCase,
)
from app.domain.assets.data_asset import DataAsset
from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_type import ObjectType
from app.domain.shared.exceptions import PlatformNotFoundError
from app.domain.shared.value_objects import EmailAddress
from tests.unit.fakes import FakeUnitOfWork


@pytest.mark.asyncio
async def test_get_discovery_snapshot_raises_when_asset_not_found() -> None:
    uow = FakeUnitOfWork()
    use_case = GetDiscoverySnapshotUseCase(uow=uow)
    with pytest.raises(PlatformNotFoundError, match="Asset not found"):
        await use_case.execute(asset_name="missing-asset")


@pytest.mark.asyncio
async def test_get_discovery_snapshot_returns_snapshot_when_asset_exists() -> None:
    uow = FakeUnitOfWork()
    asset = DataAsset(
        id="asset-1",
        name="sales_db",
        description="Sales database",
        owner=EmailAddress("owner@example.com"),
    )
    await uow.assets.save(asset)

    obj = DataObject(
        id="obj-1",
        asset_id="asset-1",
        name="orders",
        type=ObjectType.TABLE,
        elements=[
            DataElement(
                id="elem-1",
                object_id="obj-1",
                name="order_id",
                destination_type=ElementType.STRING,
                source_type=ElementType.STRING,
                nullable=False,
                is_primary_key=True,
            )
        ],
    )
    await uow.objects.save(obj)

    use_case = GetDiscoverySnapshotUseCase(uow=uow)
    snapshot = await use_case.execute(asset_name="sales_db")

    assert snapshot["asset_id"] == "asset-1"
    assert snapshot["asset_name"] == "sales_db"
    assert "orders" in snapshot["objects"]
    assert len(snapshot["fields"]) == 1
    assert snapshot["fields"][0]["name"] == "order_id"
    assert snapshot["fields"][0]["is_primary_key"] is True
