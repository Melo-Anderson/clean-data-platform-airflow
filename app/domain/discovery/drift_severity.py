from __future__ import annotations

from enum import StrEnum


class DriftSeverity(StrEnum):
    """
    Classification of a schema change detected during Discovery.
    """

    INFORMATIVE = "informative"
    CRITICAL = "critical"
