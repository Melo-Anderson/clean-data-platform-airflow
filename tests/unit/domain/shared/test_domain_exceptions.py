from __future__ import annotations

from app.domain.shared.exceptions import (
    DomainException,
    PlatformForbiddenError,
    PlatformNotFoundError,
    PlatformUnauthorizedError,
    PlatformValidationError,
)


def test_domain_exception_hierarchy() -> None:
    """Todas as exceções de domínio devem herdar de DomainException."""
    assert issubclass(PlatformNotFoundError, DomainException)
    assert issubclass(PlatformValidationError, DomainException)
    assert issubclass(PlatformForbiddenError, DomainException)
    assert issubclass(PlatformUnauthorizedError, DomainException)
