from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.discovery.drift_approval import DriftApproval
from app.domain.discovery.drift_approval_decision import DriftApprovalDecision
from app.domain.discovery.drift_change_type import DriftChangeType
from app.infrastructure.persistence.models.drift_approval_model import DriftApprovalModel


def _to_domain(m: DriftApprovalModel) -> DriftApproval:
    return DriftApproval(
        id=m.id,
        discovery_run_id=m.discovery_run_id,
        asset_id=m.asset_id,
        object_id=m.object_id,
        field_name=m.field_name,
        change_type=DriftChangeType(m.change_type),
        severity_description=m.severity_description,
        decision=DriftApprovalDecision(m.decision),
        decided_by=m.decided_by,
        decided_at=m.decided_at,
        owner_notes=m.owner_notes,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


class SqlDriftApprovalRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, approval: DriftApproval) -> DriftApproval:
        m = await self._session.get(DriftApprovalModel, approval.id)
        if m is None:
            m = DriftApprovalModel(id=approval.id)
            self._session.add(m)

        m.discovery_run_id = approval.discovery_run_id
        m.asset_id = approval.asset_id
        m.object_id = approval.object_id
        m.field_name = approval.field_name
        m.change_type = approval.change_type.value
        m.severity_description = approval.severity_description
        m.decision = approval.decision.value
        m.decided_by = approval.decided_by
        m.decided_at = approval.decided_at
        m.owner_notes = approval.owner_notes

        await self._session.flush()
        await self._session.refresh(m)
        return _to_domain(m)

    async def find_by_id(self, approval_id: str) -> DriftApproval | None:
        result = await self._session.execute(
            select(DriftApprovalModel).where(DriftApprovalModel.id == approval_id)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_pending_by_asset_id(self, asset_id: str) -> list[DriftApproval]:
        result = await self._session.execute(
            select(DriftApprovalModel)
            .where(DriftApprovalModel.asset_id == asset_id)
            .where(DriftApprovalModel.decision == DriftApprovalDecision.PENDING.value)
        )
        return [_to_domain(m) for m in result.scalars().all()]

    async def find_by_discovery_run_id(
        self, discovery_run_id: str
    ) -> list[DriftApproval]:
        result = await self._session.execute(
            select(DriftApprovalModel).where(
                DriftApprovalModel.discovery_run_id == discovery_run_id
            )
        )
        return [_to_domain(m) for m in result.scalars().all()]
