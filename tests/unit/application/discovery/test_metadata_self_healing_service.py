from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.application.discovery.metadata_self_healing_service import MetadataSelfHealingService
from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot


def _make_uow() -> MagicMock:
    uow = MagicMock()
    uow.objects = MagicMock()
    uow.drift_approvals = MagicMock()
    uow.drift_approvals.save = AsyncMock()
    return uow


def _make_snapshot(object_id: str) -> SchemaSnapshot:
    return SchemaSnapshot(
        object_id=object_id,
        fields=[SchemaField(name="id", source_type="integer", normalized_type="integer")],
    )


@pytest.mark.asyncio
async def test_apply_self_healing_calls_apply_schema_snapshot_on_informative_events() -> None:
    """Self-healing deve ser aplicado quando há eventos informativos."""
    uow = _make_uow()
    snap = _make_snapshot("obj-1")
    informative_event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.FIELD_ADDED,
        description="field added",
    )

    service = MetadataSelfHealingService(uow=uow)
    with patch(
        "app.application.discovery.metadata_self_healing_service.DataObjectService"
    ) as MockObjectService:
        mock_svc = MockObjectService.return_value
        mock_svc.apply_schema_snapshot = AsyncMock()
        await service.apply_self_healing_and_approvals(
            asset_id="asset-1",
            run_id="run-1",
            snapshots=[snap],
            drift_events=[informative_event],
            prev_snapshots={"obj-1": snap},
        )

    mock_svc.apply_schema_snapshot.assert_called_once_with("obj-1", snap)
    uow.drift_approvals.save.assert_not_called()


@pytest.mark.asyncio
async def test_apply_self_healing_saves_drift_approval_on_critical_event() -> None:
    """Eventos críticos devem gerar DriftApproval persistida."""
    uow = _make_uow()
    snap = _make_snapshot("obj-1")
    critical_event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.FIELD_REMOVED,
        field_name="amount",
        description="field removed",
    )

    service = MetadataSelfHealingService(uow=uow)
    with patch(
        "app.application.discovery.metadata_self_healing_service.DataObjectService"
    ) as MockObjectService:
        mock_svc = MockObjectService.return_value
        mock_svc.apply_schema_snapshot = AsyncMock()
        await service.apply_self_healing_and_approvals(
            asset_id="asset-1",
            run_id="run-1",
            snapshots=[snap],
            drift_events=[critical_event],
            prev_snapshots={"obj-1": snap},
        )

    uow.drift_approvals.save.assert_called_once()
    saved_approval = uow.drift_approvals.save.call_args[0][0]
    assert saved_approval.asset_id == "asset-1"
    assert saved_approval.object_id == "obj-1"
    assert saved_approval.field_name == "amount"


@pytest.mark.asyncio
async def test_apply_self_healing_first_discovery_calls_apply_schema_snapshot() -> None:
    """Sem snapshots anteriores (primeira descoberta), self-healing deve ser aplicado."""
    uow = _make_uow()
    snap = _make_snapshot("obj-1")

    service = MetadataSelfHealingService(uow=uow)
    with patch(
        "app.application.discovery.metadata_self_healing_service.DataObjectService"
    ) as MockObjectService:
        mock_svc = MockObjectService.return_value
        mock_svc.apply_schema_snapshot = AsyncMock()
        await service.apply_self_healing_and_approvals(
            asset_id="asset-1",
            run_id="run-1",
            snapshots=[snap],
            drift_events=[],
            prev_snapshots={},  # no previous snapshots
        )

    mock_svc.apply_schema_snapshot.assert_called_once_with("obj-1", snap)
