from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime

from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.shared.auditable import Auditable


@dataclass(kw_only=True)
class DiscoveryRun(Auditable):
    """
    Aggregate root for a single Discovery execution against a DataAsset.
    """

    id: str
    asset_id: str
    triggered_by: str
    status: DiscoveryRunStatus = DiscoveryRunStatus.PENDING
    started_at: datetime | None = None
    completed_at: datetime | None = None
    error_message: str | None = None
    snapshots: list[SchemaSnapshot] = field(default_factory=list)
    drift_events: list[DriftEvent] = field(default_factory=list)
    policy_tag_suggestions: list[PolicyTagSuggestion] = field(default_factory=list)
    auto_generated_descriptions: dict[str, str] = field(default_factory=dict)
    objects_discovered: int = 0
    fields_discovered: int = 0
    soft_failures: list[str] = field(default_factory=list)

    def start(self) -> None:
        if self.status != DiscoveryRunStatus.PENDING:
            raise ValueError(f"Cannot start a run in status={self.status!r}")
        self.status = DiscoveryRunStatus.RUNNING
        self.started_at = datetime.now(tz=UTC)
        self.touch()

    def complete(
        self,
        snapshots: list[SchemaSnapshot],
        drift_events: list[DriftEvent],
        policy_tag_suggestions: list[PolicyTagSuggestion],
        auto_generated_descriptions: dict[str, str],
        soft_failures: list[str] | None = None,
    ) -> None:
        if self.status != DiscoveryRunStatus.RUNNING:
            raise ValueError(f"Cannot complete a run in status={self.status!r}")
        self.snapshots = snapshots
        self.drift_events = drift_events
        self.policy_tag_suggestions = policy_tag_suggestions
        self.auto_generated_descriptions = auto_generated_descriptions
        self.soft_failures = soft_failures or []
        self.objects_discovered = len(snapshots)
        self.fields_discovered = sum(len(s.fields) for s in snapshots)
        self.completed_at = datetime.now(tz=UTC)
        self.status = (
            DiscoveryRunStatus.PARTIAL if self.soft_failures else DiscoveryRunStatus.COMPLETED
        )
        self.touch()

    def fail(self, error_message: str) -> None:
        if self.status != DiscoveryRunStatus.RUNNING:
            raise ValueError(f"Cannot fail a run in status={self.status!r}")
        self.status = DiscoveryRunStatus.FAILED
        self.error_message = error_message
        self.completed_at = datetime.now(tz=UTC)
        self.touch()

    @property
    def has_critical_drift(self) -> bool:
        return any(e.is_critical for e in self.drift_events)

    @property
    def critical_events(self) -> list[DriftEvent]:
        return [e for e in self.drift_events if e.is_critical]

    @property
    def informative_events(self) -> list[DriftEvent]:
        return [e for e in self.drift_events if not e.is_critical]

    @property
    def high_confidence_suggestions(self) -> list[PolicyTagSuggestion]:
        return [s for s in self.policy_tag_suggestions if s.confidence == PolicyTagConfidence.HIGH]

    def duration_seconds(self) -> float | None:
        if self.started_at is None or self.completed_at is None:
            return None
        return (self.completed_at - self.started_at).total_seconds()

    def is_terminal(self) -> bool:
        return self.status in (
            DiscoveryRunStatus.COMPLETED,
            DiscoveryRunStatus.FAILED,
            DiscoveryRunStatus.PARTIAL,
        )
