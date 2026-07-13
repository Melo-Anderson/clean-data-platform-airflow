from __future__ import annotations

from typing import Any

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.current_user import CurrentUser
from app.auth.jwt_validator import JwtValidator
from app.auth.permission_resolver import DatabasePermissionResolver
from app.config import get_settings
from app.domain.shared.exceptions import PlatformForbiddenError, PlatformUnauthorizedError
from app.domain.shared.value_objects import EmailAddress
from app.infrastructure.persistence.database import get_session_factory

_bearer = HTTPBearer(auto_error=False)


def get_jwt_validator() -> JwtValidator:
    return JwtValidator(get_settings())


def get_permission_resolver() -> DatabasePermissionResolver:
    return DatabasePermissionResolver(
        get_session_factory(), ttl_seconds=get_settings().permission_cache_ttl_seconds
    )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    validator: JwtValidator = Depends(get_jwt_validator),
) -> CurrentUser:
    """Resolve the authenticated user from a Bearer JWT (RS256). Raises PlatformUnauthorizedError if invalid."""
    if not credentials:
        raise PlatformUnauthorizedError("Authorization header missing")
    payload = validator.validate(credentials.credentials)
    roles = validator.extract_roles(payload)
    return CurrentUser(
        id=payload.get("sub", ""),
        email=EmailAddress(payload.get("email", f"{payload.get('sub', 'unknown')}@platform.local")),
        roles=roles,
    )


def require_permission(permission: str) -> Any:
    """FastAPI dependency factory. Enforces that the caller has the given permission string."""

    async def _enforce(
        credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
        _v: JwtValidator = Depends(get_jwt_validator),
        _r: DatabasePermissionResolver = Depends(get_permission_resolver),
    ) -> CurrentUser:
        if not credentials:
            raise PlatformUnauthorizedError("Authorization header missing")
        payload = _v.validate(credentials.credentials)
        roles = _v.extract_roles(payload)
        permissions = await _r.get_permissions_for_roles(roles)
        if permission not in permissions:
            raise PlatformForbiddenError(
                f"Permission '{permission}' required but user has: {sorted(permissions)}"
            )
        return CurrentUser(
            id=payload.get("sub", ""),
            email=EmailAddress(
                payload.get("email", f"{payload.get('sub', 'unknown')}@platform.local")
            ),
            roles=roles,
        )

    return _enforce
