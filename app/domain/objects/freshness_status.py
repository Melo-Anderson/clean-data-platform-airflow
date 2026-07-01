from __future__ import annotations

from enum import StrEnum


class FreshnessStatus(StrEnum):
    """Data freshness status."""

    FRESH = "fresh"  # last_success within expected schedule window
    STALE = "stale"  # last_success outside expected schedule window
    UNKNOWN = "unknown"  # no execution recorded yet
