from __future__ import annotations

from enum import StrEnum


class OnCriticalChange(StrEnum):
    BLOCK = "block"
    SELF_HEAL = "self_heal"
    ALERT_ONLY = "alert_only"
