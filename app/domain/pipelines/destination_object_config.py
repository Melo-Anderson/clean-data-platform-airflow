from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class DestinationObjectConfig:
    object_id: str
    create_if_not_exists: bool = True
