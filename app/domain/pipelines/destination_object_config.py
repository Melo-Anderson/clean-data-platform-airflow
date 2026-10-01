from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class DestinationObjectConfig:
    object_name: str
    create_if_not_exists: bool = True
    schema_fields: list[dict[str, Any]] | None = None
