from __future__ import annotations

from app.domain.shared.exceptions import DomainException


class UnsupportedEndpointError(DomainException):
    """Raised when an endpoint type has no registered DiscoveryRunner.

    Maps to HTTP 422 Unprocessable Entity via the exception handler layer.
    """
