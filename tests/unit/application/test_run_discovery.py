from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.endpoints.endpoint import DatabaseEndpoint
from app.domain.objects.data_object import DataObject
from app.domain.objects.freshness_status import FreshnessStatus
from app.domain.objects.object_type import ObjectType


@pytest.mark.asyncio
async def test_run_discovery_auto_provisions_missing_objects() -> None:
    # Arrange
    uow = AsyncMock()
    uow.__aenter__.return_value = uow

    runner_factory = MagicMock()
    schema_differ = MagicMock()
    tag_inferrer = MagicMock()

    use_case = RunDiscoveryUseCase(
        uow=uow,
        runner_factory=runner_factory,
        schema_differ=schema_differ,
        tag_inferrer=tag_inferrer,
    )

    asset_id = "asset-1"
    endpoint_id = "endpoint-1"

    from app.domain.assets.asset_state import AssetState
    from app.domain.shared.value_objects import CronSchedule, DiscoveryScope, EmailAddress

    asset = DataAsset(
        id=asset_id,
        name="Asset 1",
        description="",
        owner=EmailAddress("test@test.com"),
        tags=[],
        policy_tags=[],
        state=AssetState.ACTIVE,
        discovery_schedule=CronSchedule("0 6 * * *"),
        discovery_scope=DiscoveryScope(include=["*"]),
        endpoint_id=endpoint_id,
    )
    uow.assets.find_by_id.return_value = asset

    endpoint = DatabaseEndpoint(
        id=endpoint_id,
        name="Test DB",
        credential_ref=MagicMock(),
        technical_description="",
    )
    uow.endpoints.find_by_id.return_value = endpoint

    # Existing object
    existing_obj = DataObject(
        id="obj-1",
        asset_id=asset_id,
        name="existing_table",
        type=ObjectType.TABLE,
        description="",
        policy_tags=[],
        last_run=None,
        last_success=None,
        freshness_status=FreshnessStatus.UNKNOWN,
        elements=[],
        auto_generated_description=False,
    )
    uow.objects.find_by_asset_id.return_value = [existing_obj]

    runner = AsyncMock()
    runner_factory.create.return_value = runner

    # Discovery returns existing and a NEW table
    snapshots = [
        SchemaSnapshot(
            object_id="",
            object_name="existing_table",
            runner_type="database",
            fields=[SchemaField(name="id", source_type="int", normalized_type="INTEGER")],
        ),
        SchemaSnapshot(
            object_id="",
            object_name="new_table",
            runner_type="database",
            fields=[SchemaField(name="id", source_type="int", normalized_type="INTEGER")],
        ),
    ]
    runner.run.return_value = snapshots

    uow.discovery_runs.find_latest_by_asset_id.return_value = None

    async def mock_save_run(run):
        return run

    uow.discovery_runs.save.side_effect = mock_save_run

    async def mock_save_obj(obj):
        obj.id = "new-obj-id"
        return obj

    uow.objects.save.side_effect = mock_save_obj

    schema_differ.diff.return_value = []
    tag_inferrer.infer.return_value = None

    # Act
    run = await use_case.execute(asset_id, triggered_by="test")

    # Assert
    assert run is not None
    assert len(run.snapshots) == 2

    # Verify the new object was saved
    uow.objects.save.assert_called_once()
    saved_arg = uow.objects.save.call_args[0][0]
    assert saved_arg.name == "new_table"
    assert saved_arg.auto_generated_description is True

    # Verify the snapshot was updated with the object ID
    new_table_snapshot = next(s for s in run.snapshots if s.object_name == "new_table")
    assert new_table_snapshot.object_id == "new-obj-id"

    existing_table_snapshot = next(s for s in run.snapshots if s.object_name == "existing_table")
    assert existing_table_snapshot.object_id == "obj-1"
