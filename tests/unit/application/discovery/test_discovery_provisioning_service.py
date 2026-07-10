import pytest
from datetime import datetime
from unittest.mock import AsyncMock

from app.application.discovery.discovery_provisioning_service import DiscoveryProvisioningService
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType


class MockUoW:
    def __init__(self):
        self._objects = AsyncMock()
        self.saved_objects = []

        async def fake_save(obj):
            self.saved_objects.append(obj)
            return obj

        self._objects.save.side_effect = fake_save

    @property
    def objects(self):
        return self._objects


@pytest.mark.asyncio
async def test_provision_missing_objects_creates_missing_objects() -> None:
    uow = MockUoW()
    service = DiscoveryProvisioningService(uow)

    snapshots = [
        SchemaSnapshot(
            object_name="new_table",
            object_id="dummy",
            runner_type="test",
            captured_at=datetime.now(),
            row_count_estimate=100,
            fields=[],
        )
    ]

    updated_snapshots = await service.provision_missing_objects(
        asset_id="asset_1", snapshots=snapshots, existing_objects=[]
    )

    assert len(uow.saved_objects) == 1
    saved_obj = uow.saved_objects[0]
    assert saved_obj.name == "new_table"
    assert saved_obj.asset_id == "asset_1"

    assert len(updated_snapshots) == 1
    assert updated_snapshots[0].object_id == saved_obj.id


@pytest.mark.asyncio
async def test_provision_missing_objects_uses_existing_objects() -> None:
    uow = MockUoW()
    service = DiscoveryProvisioningService(uow)

    existing_obj = DataObject(
        id="existing_id",
        asset_id="asset_1",
        name="existing_table",
        type=ObjectType.TABLE,
    )

    snapshots = [
        SchemaSnapshot(
            object_name="existing_table",
            object_id="dummy",
            runner_type="test",
            captured_at=datetime.now(),
            row_count_estimate=100,
            fields=[],
        )
    ]

    updated_snapshots = await service.provision_missing_objects(
        asset_id="asset_1", snapshots=snapshots, existing_objects=[existing_obj]
    )

    # Should not save any new objects
    assert len(uow.saved_objects) == 0

    assert len(updated_snapshots) == 1
    assert updated_snapshots[0].object_id == "existing_id"
