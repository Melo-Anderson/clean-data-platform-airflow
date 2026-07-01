from __future__ import annotations

from enum import StrEnum


class Role(StrEnum):
    """Platform roles. Maps to IAM groups in GKE / identity provider claims."""

    PO_PM = "po_pm"
    ANALYTICS_ENGINEER = "analytics_engineer"
    SRE = "sre"
