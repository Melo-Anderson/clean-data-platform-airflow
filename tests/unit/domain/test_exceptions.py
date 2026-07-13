from __future__ import annotations

from app.domain.shared.exceptions import (
    CircuitBreakerOpenError,
    DataQualityViolationException,
    DomainException,
    PipelineExecutionException,
    PlatformNotFoundError,
    PlatformValidationError,
)


def test_platform_not_found_is_domain_exception():
    exc = PlatformNotFoundError("Pipeline 123 not found")
    assert isinstance(exc, DomainException)
    assert str(exc) == "Pipeline 123 not found"


def test_platform_validation_is_domain_exception():
    assert issubclass(PlatformValidationError, DomainException)


def test_pipeline_execution_is_domain_exception():
    assert issubclass(PipelineExecutionException, DomainException)


def test_data_quality_is_domain_exception():
    assert issubclass(DataQualityViolationException, DomainException)


def test_circuit_breaker_open_is_domain_exception():
    exc = CircuitBreakerOpenError("airflow-cb")
    assert isinstance(exc, DomainException)
    assert "airflow-cb" in str(exc)
