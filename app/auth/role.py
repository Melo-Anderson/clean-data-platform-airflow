from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    """Platform roles. Maps to identity provider claims or directory groups."""

    PO_PM = "po_pm"
    ANALYTICS_ENGINEER = "analytics_engineer"
    SRE = "sre"
