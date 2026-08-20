from __future__ import annotations

import pytest

from app.domain.pipelines.quality_gate_evaluator import QualityGateEvaluator


@pytest.fixture
def evaluator() -> QualityGateEvaluator:
    return QualityGateEvaluator()


def test_domain_row_count_min_passes(evaluator: QualityGateEvaluator) -> None:
    violations = evaluator.evaluate(
        metrics={"row_count": 1000},
        rules=[{"type": "row_count_min", "value": 500}],
    )
    assert violations == []


def test_domain_row_count_min_fails(evaluator: QualityGateEvaluator) -> None:
    violations = evaluator.evaluate(
        metrics={"row_count": 100},
        rules=[{"type": "row_count_min", "value": 500}],
    )
    assert len(violations) == 1
    assert "row_count" in violations[0]


def test_domain_not_null_passes(evaluator: QualityGateEvaluator) -> None:
    violations = evaluator.evaluate(
        metrics={"null_count_email": 0},
        rules=[{"type": "not_null", "column": "email"}],
    )
    assert violations == []


def test_domain_not_null_fails(evaluator: QualityGateEvaluator) -> None:
    violations = evaluator.evaluate(
        metrics={"null_count_email": 5},
        rules=[{"type": "not_null", "column": "email"}],
    )
    assert len(violations) == 1
    assert "email" in violations[0]
