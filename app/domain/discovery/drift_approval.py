from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from app.domain.discovery.drift_approval_decision import DriftApprovalDecision
from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.shared.auditable import Auditable


@dataclass(kw_only=True)
class DriftApproval(Auditable):
    """
    Records the asset owner's decision on a critical DriftEvent.
    """

    id: str
    discovery_run_id: str
    asset_id: str
    object_id: str
    change_type: DriftChangeType
    severity_description: str                     
    field_name: str | None = None                 
    decision: DriftApprovalDecision = DriftApprovalDecision.PENDING
    decided_by: str | None = None                 
    decided_at: datetime | None = None
    owner_notes: str | None = None                

    def approve(self, decided_by: str, notes: str | None = None) -> None:
        if self.decision != DriftApprovalDecision.PENDING:
            raise ValueError(f"DriftApproval already decided: {self.decision!r}")
        self.decision = DriftApprovalDecision.APPROVED
        self.decided_by = decided_by
        self.decided_at = datetime.now(tz=timezone.utc)
        self.owner_notes = notes
        self.touch()

    def reject(self, decided_by: str, notes: str | None = None) -> None:
        if self.decision != DriftApprovalDecision.PENDING:
            raise ValueError(f"DriftApproval already decided: {self.decision!r}")
        self.decision = DriftApprovalDecision.REJECTED
        self.decided_by = decided_by
        self.decided_at = datetime.now(tz=timezone.utc)
        self.owner_notes = notes
        self.touch()

    @property
    def is_pending(self) -> bool:
        return self.decision == DriftApprovalDecision.PENDING
