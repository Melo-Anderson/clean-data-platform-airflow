from __future__ import annotations
from typing import Protocol, runtime_checkable

from app.domain.discovery.drift_approval import DriftApproval


@runtime_checkable
class DriftApprovalRepository(Protocol):

    async def save(self, approval: DriftApproval) -> DriftApproval: ...

    async def find_by_id(self, approval_id: str) -> DriftApproval | None: ...

    async def find_pending_by_asset_id(self, asset_id: str) -> list[DriftApproval]:
        """Return all pending approvals blocking self-healing for an asset."""
        ...

    async def find_by_discovery_run_id(
        self, discovery_run_id: str
    ) -> list[DriftApproval]: ...
