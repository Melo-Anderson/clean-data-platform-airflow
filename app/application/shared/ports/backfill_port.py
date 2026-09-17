from __future__ import annotations

from datetime import date
from typing import Protocol, runtime_checkable


@runtime_checkable
class BackfillPort(Protocol):
    """Port for triggering programmatic backfills in the orchestrator."""

    async def create_backfill(self, dag_id: str, from_date: date, to_date: date) -> str: ...
