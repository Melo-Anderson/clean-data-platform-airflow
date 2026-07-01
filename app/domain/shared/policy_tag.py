from __future__ import annotations

from enum import StrEnum


class PolicyTag(StrEnum):
    """
    Data sensitivity classification tags.

    Inherited by all DataObjects and DataElements derived from a DataAsset.
    Discovery engine infers and suggests tags; asset owner confirms.
    """

    PII = "PII"
    RESTRICTED = "RESTRICTED"
    PUBLIC = "PUBLIC"
    CONFIDENTIAL = "CONFIDENTIAL"
