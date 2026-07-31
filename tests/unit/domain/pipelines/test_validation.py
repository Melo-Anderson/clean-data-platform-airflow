from __future__ import annotations

from app.domain.pipelines.validation import ValidationError, ValidationResult


def test_validation_result_creation():
    err = ValidationError(json_pointer="/", error_code="TEST", message="msg", suggestion="fix")
    res = ValidationResult(is_valid=False, errors=[err])
    assert not res.is_valid
    assert res.errors[0].error_code == "TEST"
