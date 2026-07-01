from __future__ import annotations

from typing import Annotated, Any, Callable, Coroutine

from fastapi import Depends, HTTPException, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.auth.current_user import CurrentUser
from app.auth.role import Role
from app.domain.shared.value_objects import EmailAddress

_bearer = HTTPBearer()


async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(_bearer),
) -> CurrentUser:
    """
    Resolve the authenticated user from a Bearer JWT token.

    Stub implementation — replace with real JWT decode (python-jose) in production.
    In GKE, the JWT is issued by the identity provider and validated here.
    """
    # TODO: decode JWT, extract id/email/role claims, validate signature
    return CurrentUser(
        id="dev-user",
        email=EmailAddress("dev@platform.local"),
        role=Role.ANALYTICS_ENGINEER,
    )


def require_role(*allowed_roles: Role) -> Callable[..., Coroutine[Any, Any, CurrentUser]]:
    """
    FastAPI dependency factory enforcing role-based access control.

    Raises HTTP 403 if the user's role is not in allowed_roles.

    Example:
        @router.post("/endpoints", dependencies=[Depends(require_role(Role.SRE))])
    """

    async def _enforce(user: CurrentUser = Depends(get_current_user)) -> CurrentUser:
        if user.role not in allowed_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=(
                    f"Role '{user.role}' is not allowed. "
                    f"Required one of: {[r.value for r in allowed_roles]}"
                ),
            )
        return user

    return _enforce
