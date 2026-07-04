from enum import StrEnum

class DriftApprovalDecision(StrEnum):
    """
    Decision made by the asset owner on a critical DriftEvent.
    """
    APPROVED = "approved"
    REJECTED = "rejected"
    PENDING = "pending"
