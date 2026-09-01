from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from app.domain.pipelines.validation import ValidationError, ValidationResult


def test_validation_result_creation():
    err = ValidationError(json_pointer="/", error_code="TEST", message="msg", suggestion="fix")
    res = ValidationResult(is_valid=False, errors=(err,))
    assert not res.is_valid
    assert len(res.errors) == 1
    assert res.errors[0].error_code == "TEST"


def test_validation_error_is_immutable():
    err = ValidationError(json_pointer="/", error_code="TEST", message="msg", suggestion="fix")
    with pytest.raises(FrozenInstanceError):
        err.message = "new_msg"  # type: ignore[misc]


def test_validation_result_is_immutable():
    res = ValidationResult(is_valid=True)
    with pytest.raises(FrozenInstanceError):
        res.is_valid = False  # type: ignore[misc]
