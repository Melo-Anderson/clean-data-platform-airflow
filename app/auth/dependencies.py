from __future__ import annotations

from typing import Any

from fastapi import Depends
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.current_user import CurrentUser
from app.auth.jwt_validator import JwtConfig, JwtValidator
from app.auth.permission_resolver import DatabasePermissionResolver
from app.config import get_settings
from app.domain.shared.exceptions import PlatformForbiddenError, PlatformUnauthorizedError
from app.domain.shared.value_objects import EmailAddress
from app.infrastructure.persistence.database import get_session_factory

_bearer = HTTPBearer(auto_error=False)


def get_jwt_validator() -> JwtValidator:
    settings = get_settings()
    return JwtValidator(
        JwtConfig(
            public_key_pem=settings.resolved_auth_jwt_public_key_pem,
            issuer=settings.auth_jwt_issuer,
            audience=settings.auth_jwt_audience,
            roles_claim=settings.jwt_roles_claim,
        )
    )


def get_permission_resolver() -> DatabasePermissionResolver:
    return DatabasePermissionResolver(
        get_session_factory(), ttl_seconds=get_settings().permission_cache_ttl_seconds
    )


def _build_current_user(payload: dict, roles: list[str]) -> CurrentUser:
    return CurrentUser(
        id=payload.get("sub", ""),
        email=EmailAddress(payload.get("email", f"{payload.get('sub', 'unknown')}@platform.local")),
        roles=roles,
    )


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer),
    validator: JwtValidator = Depends(get_jwt_validator),
) -> CurrentUser:
    if not credentials:
        raise PlatformUnauthorizedError("Authorization header missing")
    payload = validator.validate(credentials.credentials)
    roles = validator.extract_roles(payload)
    return _build_current_user(payload, roles)


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
        return _build_current_user(payload, roles)

    return _enforce
