from __future__ import annotations


class PlatformNotFoundError(Exception):
    """Raised when a requested domain entity does not exist.

    Use instead of bare ValueError for not-found cases so HTTP handlers
    can map this to 404 without inspecting the message string.

    Example:
        raise PlatformNotFoundError(f"Pipeline not found: {pipeline_id}")
    """


class PlatformValidationError(Exception):
    """Raised when a domain business rule is violated.

    Maps to HTTP 422. Use instead of bare ValueError for validation failures.

    Example:
        raise PlatformValidationError(f"Invalid cron expression: {expr!r}")
    """
