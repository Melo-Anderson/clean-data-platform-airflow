from __future__ import annotations

import logging

from app.application.unit_of_work import UnitOfWork
from app.domain.discovery.drift_approval import DriftApproval
from app.domain.objects.object_service import DataObjectService
from app.domain.shared.exceptions import PlatformNotFoundError

logger = logging.getLogger(__name__)


class ApproveDriftUseCase:
    """Handles the asset owner's decision to approve or reject a DriftApproval."""

    def __init__(self, uow: UnitOfWork, object_service: DataObjectService | None = None) -> None:
        self._uow = uow
        self._object_service = object_service

    def _get_object_service(self, uow: UnitOfWork) -> DataObjectService:
        if self._object_service is not None:
            return self._object_service
        return DataObjectService(uow.objects)

    async def approve(
        self, approval_id: str, decided_by: str, notes: str | None = None
    ) -> DriftApproval:
        async with self._uow as uow:
            approval = await self._get_approval_or_404(uow, approval_id)
            approval.approve(decided_by=decided_by, notes=notes)
            await uow.drift_approvals.save(approval)
            await self._apply_approved_schema(uow, approval)
            await uow.commit()
            return approval

    async def reject(
        self, approval_id: str, decided_by: str, notes: str | None = None
    ) -> DriftApproval:
        async with self._uow as uow:
            approval = await self._get_approval_or_404(uow, approval_id)
            approval.reject(decided_by=decided_by, notes=notes)
            await uow.drift_approvals.save(approval)
            await uow.commit()
            return approval

    async def _get_approval_or_404(self, uow: UnitOfWork, approval_id: str) -> DriftApproval:
        approval = await uow.drift_approvals.find_by_id(approval_id)
        if not approval:
            raise PlatformNotFoundError(f"DriftApproval not found: {approval_id}")
        return approval

    async def _apply_approved_schema(self, uow: UnitOfWork, approval: DriftApproval) -> None:
        run = await uow.discovery_runs.find_by_id(approval.discovery_run_id)
        if not run:
            return
        snapshot = next((s for s in run.snapshots if s.object_id == approval.object_id), None)
        if not snapshot:
            return
        object_service = self._get_object_service(uow)
        await object_service.apply_schema_snapshot(approval.object_id, snapshot)
