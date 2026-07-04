from __future__ import annotations

from dataclasses import dataclass

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_severity import DriftSeverity

_SEVERITY_MAP: dict[DriftChangeType, DriftSeverity] = {
    DriftChangeType.FIELD_ADDED: DriftSeverity.INFORMATIVE,
    DriftChangeType.FIELD_REMOVED: DriftSeverity.CRITICAL,
    DriftChangeType.TYPE_WIDENED: DriftSeverity.INFORMATIVE,
    DriftChangeType.TYPE_INCOMPATIBLE: DriftSeverity.CRITICAL,
    DriftChangeType.NULLABLE_TO_REQUIRED: DriftSeverity.CRITICAL,
    DriftChangeType.REQUIRED_TO_NULLABLE: DriftSeverity.INFORMATIVE,
    DriftChangeType.OBJECT_ADDED: DriftSeverity.INFORMATIVE,
    DriftChangeType.OBJECT_REMOVED: DriftSeverity.CRITICAL,
}

@dataclass(frozen=True)
class DriftEvent:
    """
    Immutable record of a single detected schema change between two SchemaSnapshots.
    """

    object_id: str
    change_type: DriftChangeType
    description: str
    field_name: str | None = None
    previous_value: str | None = None
    current_value: str | None = None

    @property
    def severity(self) -> DriftSeverity:
        return _SEVERITY_MAP[self.change_type]

    @property
    def is_critical(self) -> bool:
        return self.severity == DriftSeverity.CRITICAL

    @property
    def requires_approval(self) -> bool:
        return self.is_critical
