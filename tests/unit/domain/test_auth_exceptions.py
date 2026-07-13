from __future__ import annotations

from app.domain.shared.exceptions import (
    DomainException,
    PlatformForbiddenError,
    PlatformUnauthorizedError,
)


def test_unauthorized_is_domain_exception():
    exc = PlatformUnauthorizedError("token expired")
    assert isinstance(exc, DomainException)
    assert "token expired" in str(exc)


def test_forbidden_is_domain_exception():
    exc = PlatformForbiddenError("pipeline:create")
    assert isinstance(exc, DomainException)
    assert "pipeline:create" in str(exc)
