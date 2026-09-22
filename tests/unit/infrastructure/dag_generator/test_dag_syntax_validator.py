from __future__ import annotations

import pytest

from app.domain.shared.exceptions import PlatformValidationError
from app.infrastructure.dag_generator.dag_validator import DagSyntaxValidator


def test_validator_accepts_valid_dag_code() -> None:
    code = """
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@daily", start_date=datetime(2026, 1, 1))
def sample_pipeline():
    @task
    def step_one() -> None:
        pass
    step_one()

sample_pipeline()
"""
    validator = DagSyntaxValidator()
    validator.validate(code)  # Must not raise


def test_validator_rejects_syntax_error() -> None:
    code = "def broken_code(: syntax error here"
    validator = DagSyntaxValidator()
    with pytest.raises(PlatformValidationError) as exc_info:
        validator.validate(code)
    assert "Syntax error" in str(exc_info.value)


def test_validator_rejects_empty_string() -> None:
    validator = DagSyntaxValidator()
    with pytest.raises(PlatformValidationError) as exc_info:
        validator.validate("")
    assert "empty" in str(exc_info.value).lower()


def test_validator_rejects_whitespace_only_code() -> None:
    validator = DagSyntaxValidator()
    with pytest.raises(PlatformValidationError) as exc_info:
        validator.validate("   \n\t  ")
    assert "empty" in str(exc_info.value).lower()


def test_validator_includes_filename_in_syntax_error_message() -> None:
    code = "def broken(:"
    validator = DagSyntaxValidator()
    with pytest.raises(PlatformValidationError) as exc_info:
        validator.validate(code, filename="my_pipeline.py")
    assert "my_pipeline.py" in str(exc_info.value)
