from __future__ import annotations

from functools import reduce

import jwt as pyjwt

from app.config import Settings
from app.domain.shared.exceptions import PlatformUnauthorizedError


class JwtValidator:
    """Decodes and validates RS256 JWTs using a statically configured RSA public key PEM."""

    def __init__(self, settings: Settings) -> None:
        self._public_key = settings.auth_jwt_public_key_pem
        self._issuer = settings.auth_jwt_issuer or None
        self._audience = settings.auth_jwt_audience or None
        self._roles_claim = settings.jwt_roles_claim

    def validate(self, token: str) -> dict:
        """Decode and validate the JWT. Raises PlatformUnauthorizedError on any failure."""
        options: dict = {}
        if not self._public_key:
            raise PlatformUnauthorizedError("JWT public key not configured")
        try:
            payload = pyjwt.decode(
                token,
                self._public_key,
                algorithms=["RS256"],
                issuer=self._issuer,
                audience=self._audience,
                options=options,
            )
        except pyjwt.ExpiredSignatureError as exc:
            raise PlatformUnauthorizedError("Token has expired") from exc
        except pyjwt.InvalidTokenError as exc:
            raise PlatformUnauthorizedError(f"Invalid token: {exc}") from exc
        return payload

    def extract_roles(self, payload: dict) -> list[str]:
        """Extract roles from the configured claim path (supports dotted notation)."""
        value = payload
        for part in self._roles_claim.split("."):
            if not isinstance(value, dict):
                return []
            value = value.get(part)  # type: ignore[assignment]
            if value is None:
                return []
        if isinstance(value, list):
            return [str(r) for r in value]
        if isinstance(value, str):
            return [value]
        return []
