from __future__ import annotations


class DomainException(Exception):
    """Base class for all domain-level exceptions.

    All subclasses map to specific HTTP status codes in the exception handlers.
    Never raise DomainException directly — use a specific subclass.
    """


class PlatformNotFoundError(DomainException):
    """Raised when a requested domain entity does not exist. Maps to HTTP 404."""


class PlatformValidationError(DomainException):
    """Raised when a domain business rule is violated. Maps to HTTP 422."""


class PipelineExecutionException(DomainException):
    """Raised when a pipeline cannot be triggered due to orchestrator failure. Maps to HTTP 503."""


class DataQualityViolationException(DomainException):
    """Raised when a data quality gate fails. Maps to HTTP 409."""


class CircuitBreakerOpenError(DomainException):
    """Raised by AsyncCircuitBreaker when the circuit is OPEN. Maps to HTTP 503."""

    def __init__(self, name: str) -> None:
        super().__init__(f"Circuit breaker '{name}' is OPEN — dependency unavailable")
        self.name = name
