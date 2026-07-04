from __future__ import annotations

from dataclasses import dataclass

from app.domain.shared.policy_tag import PolicyTag
from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence

@dataclass(frozen=True)
class PolicyTagSuggestion:
    """
    Inferred PolicyTag suggestion for a DataElement field.
    """

    field_name: str
    suggested_tag: PolicyTag
    confidence: PolicyTagConfidence
    matched_pattern: str                   
    auto_generated_description: str | None = None
