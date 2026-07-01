from __future__ import annotations

from enum import StrEnum


class LoadStrategy(StrEnum):
    FULL_LOAD = "full_load"
    INCREMENTAL = "incremental"
    CDC = "cdc"
