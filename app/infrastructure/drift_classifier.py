from __future__ import annotations

from typing import Any

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_differ import SchemaDiffer

_BLOCKING_CHANGE_TYPES = frozenset(
    {
        DriftChangeType.FIELD_REMOVED,
        DriftChangeType.TYPE_INCOMPATIBLE,
    }
)


def _parse_snapshot(data: dict[str, Any]) -> SchemaSnapshot:
    """Convert raw dict payload to SchemaSnapshot domain object."""
    fields = [
        SchemaField(
            name=f["name"],
            source_type=f["type"],
            normalized_type=f["type"],
            nullable=f.get("nullable", True),
        )
        for f in data.get("fields", [])
    ]
    return SchemaSnapshot(object_id=data.get("object_id", "unknown"), fields=fields)


class DriftClassifier:
    """Classifies schema drift between two snapshots using the domain SchemaDiffer.

    Infrastructure adapter: converts raw dict payloads into domain objects and
    delegates comparison logic entirely to SchemaDiffer. No business logic here.

    Example:
        result = DriftClassifier().classify_models({
            "prev": {"object_id": "orders", "fields": [{"name": "id", "type": "integer"}]},
            "curr": {"object_id": "orders", "fields": [{"name": "id", "type": "string"}]},
        })
        # result == {"can_proceed": False, "blocked_reason": "id: type_incompatible"}
    """

    def __init__(self) -> None:
        self._differ = SchemaDiffer()

    def classify_models(self, source_models: dict[str, Any]) -> dict[str, Any]:
        """Classify schema changes, returning whether the ETL can proceed.

        Args:
            source_models: Dict with optional "prev" and "curr" snapshot dicts.

        Returns:
            {"can_proceed": bool, "blocked_reason": str}
        """
        prev_data = source_models.get("prev")
        curr_data = source_models.get("curr")

        if not prev_data or not curr_data:
            return {"can_proceed": True, "blocked_reason": ""}

        events = self._differ.diff(_parse_snapshot(prev_data), _parse_snapshot(curr_data))

        blocking = [e for e in events if e.change_type in _BLOCKING_CHANGE_TYPES]

        if not blocking:
            return {"can_proceed": True, "blocked_reason": ""}

        reason = "; ".join(f"{e.field_name}: {e.change_type.value}" for e in blocking)
        return {"can_proceed": False, "blocked_reason": f"Incompatible drift detected: {reason}"}

    def classify(self, schema_snapshot: dict[str, Any], policy: str) -> dict[str, Any]:
        """Legacy stub for backward compatibility. Does not block."""
        return {"can_proceed": True, "blocked_reason": ""}
