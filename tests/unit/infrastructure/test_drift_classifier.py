from __future__ import annotations

from app.infrastructure.drift_classifier import DriftClassifier


def _snap(fields: list[dict]) -> dict:
    return {"object_id": "orders", "fields": fields}


def test_classify_models_returns_can_proceed_when_no_drift() -> None:
    """Schemas idênticos não produzem bloqueio."""
    fields = [{"name": "id", "type": "integer", "nullable": False}]
    result = DriftClassifier().classify_models({"prev": _snap(fields), "curr": _snap(fields)})
    assert result["can_proceed"] is True
    assert result["blocked_reason"] == ""


def test_classify_models_allows_field_addition() -> None:
    """Campo adicionado é drift compatível — não bloqueia."""
    prev = _snap([{"name": "id", "type": "integer", "nullable": False}])
    curr = _snap(
        [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "name", "type": "string", "nullable": True},
        ]
    )
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is True


def test_classify_models_allows_type_widening() -> None:
    """integer → bigint é widening compatível — não bloqueia."""
    prev = _snap([{"name": "amount", "type": "integer", "nullable": True}])
    curr = _snap([{"name": "amount", "type": "bigint", "nullable": True}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is True


def test_classify_models_blocks_on_field_removal() -> None:
    """Campo removido é drift incompatível — bloqueia com motivo descritivo."""
    prev = _snap(
        [
            {"name": "id", "type": "integer", "nullable": False},
            {"name": "amount", "type": "float", "nullable": True},
        ]
    )
    curr = _snap([{"name": "id", "type": "integer", "nullable": False}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "amount" in result["blocked_reason"]


def test_classify_models_blocks_on_incompatible_type_change() -> None:
    """integer → string é tipo incompatível — bloqueia."""
    prev = _snap([{"name": "order_id", "type": "integer", "nullable": False}])
    curr = _snap([{"name": "order_id", "type": "string", "nullable": False}])
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "order_id" in result["blocked_reason"]


def test_classify_models_empty_prev_or_curr_returns_can_proceed() -> None:
    """Sem snapshots anteriores ou atuais, não há o que comparar — não bloqueia."""
    result = DriftClassifier().classify_models({})
    assert result["can_proceed"] is True


def test_classify_models_preserves_all_blocked_fields_in_reason() -> None:
    """Quando múltiplos campos estão com drift incompatível, todos aparecem no blocked_reason."""
    prev = _snap(
        [
            {"name": "a", "type": "integer", "nullable": True},
            {"name": "b", "type": "float", "nullable": True},
        ]
    )
    curr = _snap(
        [
            {"name": "a", "type": "string", "nullable": True},  # incompatible
            # "b" removed
        ]
    )
    result = DriftClassifier().classify_models({"prev": prev, "curr": curr})
    assert result["can_proceed"] is False
    assert "a" in result["blocked_reason"]
    assert "b" in result["blocked_reason"]
