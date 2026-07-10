from __future__ import annotations

from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.shared.policy_tag import PolicyTag


def test_inferrer_matches_cpf_high_confidence() -> None:
    inferrer = PolicyTagInferrer()
    sug = inferrer.infer("customer_cpf")
    assert sug is not None
    assert sug.suggested_tag == PolicyTag.RESTRICTED
    assert sug.confidence == PolicyTagConfidence.HIGH


def test_inferrer_matches_email_high_confidence() -> None:
    inferrer = PolicyTagInferrer()
    sug = inferrer.infer("user_email_address")
    assert sug is not None
    assert sug.suggested_tag == PolicyTag.PII
    assert sug.confidence == PolicyTagConfidence.HIGH


def test_inferrer_matches_address_medium_confidence() -> None:
    inferrer = PolicyTagInferrer()
    sug = inferrer.infer("endereco_completo")
    assert sug is not None
    assert sug.suggested_tag == PolicyTag.PII
    assert sug.confidence == PolicyTagConfidence.MEDIUM


def test_inferrer_matches_name_low_confidence() -> None:
    inferrer = PolicyTagInferrer()
    sug = inferrer.infer("first_name")
    assert sug is not None
    assert sug.suggested_tag == PolicyTag.PII
    assert sug.confidence == PolicyTagConfidence.LOW


def test_inferrer_returns_none_for_unmatched() -> None:
    inferrer = PolicyTagInferrer()
    sug = inferrer.infer("created_at")
    assert sug is None
