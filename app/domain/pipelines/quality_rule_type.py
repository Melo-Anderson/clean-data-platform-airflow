from __future__ import annotations

from enum import StrEnum


class QualityRuleType(StrEnum):
    NOT_NULL = "not_null"
    ROW_COUNT_MIN = "row_count_min"
    UNIQUE = "unique"
    ACCEPTED_VALUES = "accepted_values"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    CHECKSUM = "checksum"
