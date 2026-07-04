from __future__ import annotations

import re

from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.shared.policy_tag import PolicyTag


class PolicyTagInferrer:
    """
    Service to infer PolicyTags based on field names and metadata.
    """

    def __init__(self) -> None:
        self._rules = [
            (re.compile(r"cpf|cnpj", re.IGNORECASE), PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
            (re.compile(r"password|senha", re.IGNORECASE), PolicyTag.RESTRICTED, PolicyTagConfidence.HIGH),
            (re.compile(r"email", re.IGNORECASE), PolicyTag.PII, PolicyTagConfidence.HIGH),
            (re.compile(r"phone|telefone|celular", re.IGNORECASE), PolicyTag.PII, PolicyTagConfidence.MEDIUM),
            (re.compile(r"address|endereco|cep|zipcode", re.IGNORECASE), PolicyTag.PII, PolicyTagConfidence.MEDIUM),
            (re.compile(r"name|nome", re.IGNORECASE), PolicyTag.PII, PolicyTagConfidence.LOW),
        ]

    def infer(self, field_name: str) -> PolicyTagSuggestion | None:
        for pattern, tag, confidence in self._rules:
            if pattern.search(field_name):
                return PolicyTagSuggestion(
                    field_name=field_name,
                    suggested_tag=tag,
                    confidence=confidence,
                    matched_pattern=pattern.pattern,
                    auto_generated_description=f"Auto-suggested based on field name matching '{pattern.pattern}'",
                )
        return None
