from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.discovery.discovery_runner import DiscoveryRunner, DiscoveryRunnerFactory
from app.application.discovery.run_discovery_use_case import RunDiscoveryUseCase
from app.application.unit_of_work import UnitOfWork
from app.domain.assets.data_asset import DataAsset
from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer
from app.domain.objects.data_object import DataObject
from app.domain.objects.object_type import ObjectType


class MockUoW(UnitOfWork):
    def __init__(self):
        self.commit_called = False
        self.rollback_called = False
        self._assets = AsyncMock()
        self._endpoints = AsyncMock()
        self._objects = AsyncMock()
        self._discovery_runs = AsyncMock()
        self._drift_approvals = AsyncMock()

    @property
    def assets(self): return self._assets

    @property
    def endpoints(self): return self._endpoints

    @property
    def objects(self): return self._objects

    @property
    def discovery_runs(self): return self._discovery_runs

    @property
    def drift_approvals(self): return self._drift_approvals

    async def commit(self):
        self.commit_called = True

    async def rollback(self):
        self.rollback_called = True

    async def __aenter__(self):
        self.discovery_runs.save.side_effect = lambda x: x
        self.drift_approvals.save.side_effect = lambda x: x
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()

@pytest.fixture
def mock_uow() -> MockUoW:
    return MockUoW()


@pytest.fixture
def mock_runner_factory() -> MagicMock:
    factory = MagicMock(spec=DiscoveryRunnerFactory)
    runner = AsyncMock(spec=DiscoveryRunner)
    factory.create.return_value = runner
    return factory


@pytest.fixture
def use_case(mock_uow: MockUoW, mock_runner_factory: MagicMock) -> RunDiscoveryUseCase:
    differ = SchemaDiffer()
    inferrer = PolicyTagInferrer()
    return RunDiscoveryUseCase(
        uow=mock_uow,
        runner_factory=mock_runner_factory,
        schema_differ=differ,
        tag_inferrer=inferrer,
    )


@pytest.mark.asyncio
async def test_run_discovery_use_case_success(
    use_case: RunDiscoveryUseCase, mock_uow: MockUoW, mock_runner_factory: MagicMock
) -> None:
    # Setup mocks
    asset_id = "asset-1"
    endpoint_id = "ep-1"
    object_id = "obj-1"
    
    # Asset & Endpoint
    mock_asset = MagicMock()
    mock_asset.endpoint_id = endpoint_id
    mock_uow.assets.find_by_id.return_value = mock_asset
    
    mock_endpoint = MagicMock()
    mock_endpoint.id = endpoint_id
    mock_uow.endpoints.find_by_id.return_value = mock_endpoint
    
    # Objects
    mock_obj = DataObject(id=object_id, asset_id=asset_id, name="users", type=ObjectType.TABLE)
    mock_uow.objects.find_all_by_asset_id.return_value = [mock_obj]
    mock_uow.objects.find_by_id.return_value = mock_obj
    
    # Baseline run
    mock_uow.discovery_runs.find_latest_by_asset_id.return_value = None
    
    # Runner behavior
    runner = mock_runner_factory.create.return_value
    runner.run.return_value = [
        SchemaSnapshot(
            object_id=object_id,
            fields=[
                SchemaField(name="id", source_type="INT", normalized_type="integer"),
                SchemaField(name="email", source_type="VARCHAR", normalized_type="string"),
            ]
        )
    ]
    
    # Execute
    run = await use_case.execute(asset_id=asset_id, triggered_by="scheduler")
    
    assert mock_uow.commit_called
    # Save called twice: first for RUNNING, then for COMPLETED
    assert mock_uow.discovery_runs.save.call_count == 2
    assert run.status == "completed"
    assert len(run.snapshots) == 1
    
    # Since there was no baseline, there's 1 event: OBJECT_ADDED (informative)
    assert len(run.informative_events) == 1
    
    # Policy tags: 'email' field gets inferred as PII
    assert len(run.policy_tag_suggestions) == 1
    assert run.policy_tag_suggestions[0].field_name == "email"


@pytest.mark.asyncio
async def test_run_discovery_use_case_with_critical_drift(
    use_case: RunDiscoveryUseCase, mock_uow: MockUoW, mock_runner_factory: MagicMock
) -> None:
    asset_id = "asset-1"
    object_id = "obj-1"
    
    mock_asset = MagicMock()
    mock_asset.endpoint_id = "ep-1"
    mock_uow.assets.find_by_id.return_value = mock_asset
    mock_uow.endpoints.find_by_id.return_value = MagicMock(id="ep-1")
    mock_uow.objects.find_all_by_asset_id.return_value = [
        DataObject(id=object_id, asset_id=asset_id, name="users", type=ObjectType.TABLE)
    ]
    mock_uow.objects.find_by_id.return_value = DataObject(id=object_id, asset_id=asset_id, name="users", type=ObjectType.TABLE)
    
    # Baseline
    baseline = MagicMock()
    baseline.snapshots = [
        SchemaSnapshot(
            object_id=object_id,
            fields=[SchemaField(name="id", source_type="VARCHAR", normalized_type="string")]
        )
    ]
    mock_uow.discovery_runs.find_latest_by_asset_id.return_value = baseline
    
    # Runner returns changed schema (string -> integer, incompatible)
    runner = mock_runner_factory.create.return_value
    runner.run.return_value = [
        SchemaSnapshot(
            object_id=object_id,
            fields=[SchemaField(name="id", source_type="INT", normalized_type="integer")]
        )
    ]
    
    run = await use_case.execute(asset_id=asset_id, triggered_by="manual")
    
    assert len(run.critical_events) == 1
    # Drift approval must have been created
    assert mock_uow.drift_approvals.save.call_count == 1
