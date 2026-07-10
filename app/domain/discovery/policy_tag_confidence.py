from __future__ import annotations

from enum import StrEnum


class PolicyTagConfidence(StrEnum):
    """
    Confidence level of an inferred PolicyTag suggestion.
    """

    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"
