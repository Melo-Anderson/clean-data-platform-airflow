from __future__ import annotations

from dataclasses import dataclass

from app.auth.role import Role
from app.domain.shared.value_objects import EmailAddress


@dataclass(frozen=True)
class CurrentUser:
    """Authenticated user resolved from JWT token. Immutable per request."""

    id: str
    email: EmailAddress
    role: Role
