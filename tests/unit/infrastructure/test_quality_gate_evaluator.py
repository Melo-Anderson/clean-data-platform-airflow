from app.infrastructure.quality_gate_evaluator import QualityGateEvaluator

evaluator = QualityGateEvaluator()


def test_row_count_min_passes() -> None:
    violations = evaluator.evaluate(
        metrics={"row_count": 1000},
        rules=[{"type": "row_count_min", "value": 500}],
    )
    assert violations == []


def test_row_count_min_fails() -> None:
    violations = evaluator.evaluate(
        metrics={"row_count": 100},
        rules=[{"type": "row_count_min", "value": 500}],
    )
    assert len(violations) == 1
    assert "row_count" in violations[0]


def test_not_null_passes_when_zero_nulls() -> None:
    violations = evaluator.evaluate(
        metrics={"null_count_email": 0},
        rules=[{"type": "not_null", "column": "email"}],
    )
    assert violations == []


def test_not_null_fails_when_nulls_present() -> None:
    violations = evaluator.evaluate(
        metrics={"null_count_email": 5},
        rules=[{"type": "not_null", "column": "email"}],
    )
    assert len(violations) == 1
    assert "email" in violations[0]


def test_unique_passes_when_no_duplicates() -> None:
    violations = evaluator.evaluate(
        metrics={"duplicate_count_customer_id": 0},
        rules=[{"type": "unique", "column": "customer_id"}],
    )
    assert violations == []


def test_unique_fails_when_duplicates_exist() -> None:
    violations = evaluator.evaluate(
        metrics={"duplicate_count_customer_id": 3},
        rules=[{"type": "unique", "column": "customer_id"}],
    )
    assert len(violations) == 1


def test_accepted_values_fails() -> None:
    violations = evaluator.evaluate(
        metrics={"invalid_value_count_status": 2},
        rules=[{"type": "accepted_values", "column": "status"}],
    )
    assert len(violations) == 1


def test_referential_integrity_fails() -> None:
    violations = evaluator.evaluate(
        metrics={"orphan_count_order_id": 1},
        rules=[{"type": "referential_integrity", "column": "order_id"}],
    )
    assert len(violations) == 1


def test_checksum_passes_when_matching() -> None:
    violations = evaluator.evaluate(
        metrics={"checksum": "abc123"},
        rules=[{"type": "checksum", "value": "abc123"}],
    )
    assert violations == []


def test_checksum_fails_when_mismatch() -> None:
    violations = evaluator.evaluate(
        metrics={"checksum": "abc123"},
        rules=[{"type": "checksum", "value": "xyz999"}],
    )
    assert len(violations) == 1


def test_missing_metric_key_triggers_violation() -> None:
    """If compute did not emit the metric, the gate must FAIL (no silent approval)."""
    violations = evaluator.evaluate(
        metrics={},
        rules=[{"type": "row_count_min", "value": 100}],
    )
    assert len(violations) == 1
    assert "metric not computed/missing" in violations[0]


def test_quality_gate_with_nan_completeness_metric() -> None:
    """NaN metric values should fail rules since they don't satisfy bounds."""

    violations = evaluator.evaluate(
        metrics={"row_count": float("nan")},
        rules=[{"type": "row_count_min", "value": 1}],
    )
    # math.isnan check will result in NaN < threshold -> True (actually, float('nan') < 1 is False, float('nan') >= 1 is False)
    # Let's verify how Python compares NaN. In Python, nan < x is always False.
    # Therefore, if the check is: if actual < threshold, actual=nan < 1 is False, so it wouldn't fail!
    # Wait! If nan < 1 is False, then the check passes! But NaN should fail!
    # So we should write the test to assert that NaN fails. Let's see if the implementation fails it.
    assert len(violations) == 1


def test_quality_gate_with_zero_row_count_triggers_violation() -> None:
    """Row count of zero must trigger a violation if min is positive."""
    violations = evaluator.evaluate(
        metrics={"row_count": 0},
        rules=[{"type": "row_count_min", "value": 1}],
    )
    assert len(violations) == 1
    assert "row_count" in violations[0]
