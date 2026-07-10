from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from app.application.discovery.approve_drift_use_case import ApproveDriftUseCase
from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.drift_approval import DriftApproval, DriftApprovalDecision
from app.domain.discovery.drift_change_type import DriftChangeType


class MockUoW(UnitOfWork):
    def __init__(self):
        self.commit_called = False
        self.rollback_called = False
        self._drift_approvals = AsyncMock()
        self._discovery_runs = AsyncMock()
        self._objects = AsyncMock()

    @property
    def drift_approvals(self):
        return self._drift_approvals

    @property
    def discovery_runs(self):
        return self._discovery_runs

    @property
    def objects(self):
        return self._objects

    async def commit(self):
        self.commit_called = True

    async def rollback(self):
        self.rollback_called = True

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            await self.rollback()


@pytest.fixture
def mock_uow() -> MockUoW:
    return MockUoW()


@pytest.fixture
def use_case(mock_uow: MockUoW) -> ApproveDriftUseCase:
    return ApproveDriftUseCase(uow=mock_uow)


@pytest.mark.asyncio
async def test_approve_drift_use_case(use_case: ApproveDriftUseCase, mock_uow: MockUoW) -> None:
    approval = DriftApproval(
        id="app-1",
        discovery_run_id="run-1",
        asset_id="asset-1",
        object_id="obj-1",
        change_type=DriftChangeType.TYPE_INCOMPATIBLE,
        severity_description="test",
    )
    mock_uow.drift_approvals.find_by_id.return_value = approval

    mock_run = AsyncMock()
    mock_run.snapshots = []
    mock_uow.discovery_runs.find_by_id.return_value = mock_run

    result = await use_case.approve(approval_id="app-1", decided_by="owner@company.com", notes="OK")

    assert result.decision == DriftApprovalDecision.APPROVED
    assert result.decided_by == "owner@company.com"
    assert mock_uow.commit_called
    mock_uow.drift_approvals.save.assert_called_once_with(approval)


@pytest.mark.asyncio
async def test_reject_drift_use_case(use_case: ApproveDriftUseCase, mock_uow: MockUoW) -> None:
    approval = DriftApproval(
        id="app-1",
        discovery_run_id="run-1",
        asset_id="asset-1",
        object_id="obj-1",
        change_type=DriftChangeType.TYPE_INCOMPATIBLE,
        severity_description="test",
    )
    mock_uow.drift_approvals.find_by_id.return_value = approval

    result = await use_case.reject(approval_id="app-1", decided_by="owner@company.com", notes="NO")

    assert result.decision == DriftApprovalDecision.REJECTED
    assert result.decided_by == "owner@company.com"
    assert mock_uow.commit_called
    mock_uow.drift_approvals.save.assert_called_once_with(approval)
