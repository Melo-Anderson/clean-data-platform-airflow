from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.domain.discovery.schema_field import SchemaField

@dataclass(frozen=True)
class SchemaSnapshot:
    """
    Immutable point-in-time snapshot of a DataObject schema as seen by the runner.
    """

    object_id: str
    fields: list["SchemaField"] = field(default_factory=list)
    captured_at: datetime = field(default_factory=lambda: datetime.now(tz=timezone.utc))
    runner_type: str = ""                   
    object_name: str = ""                   
    row_count_estimate: int | None = None

    def field_by_name(self, name: str) -> "SchemaField | None":
        return next((f for f in self.fields if f.name == name), None)

    def field_names(self) -> frozenset[str]:
        return frozenset(f.name for f in self.fields)
