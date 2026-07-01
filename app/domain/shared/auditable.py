from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime


def _utcnow() -> datetime:
    return datetime.now(tz=UTC)


@dataclass(kw_only=True)
class Auditable:
    """
    Mixin for domain entities that track creation and last update timestamps.

    All platform entities inherit this for governance purposes.
    Call touch() after any mutation to update updated_at.
    """

    created_at: datetime = field(default_factory=_utcnow, compare=False)
    updated_at: datetime = field(default_factory=_utcnow, compare=False)

    def touch(self) -> None:
        """Update updated_at to current UTC time. Call after any field mutation."""
        self.updated_at = _utcnow()
