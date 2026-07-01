from __future__ import annotations

from enum import StrEnum


class ScheduleMode(StrEnum):
    CRON = "cron"
    TRIGGER = "trigger"
    TRIGGER_WITH_GATE = "trigger_with_gate"
