from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import jwt as pyjwt

from app.domain.shared.exceptions import PlatformUnauthorizedError


@dataclass(frozen=True)
class JwtConfig:
    """Focused config for JWT validation — avoids leaking full Settings to the validator."""

    public_key_pem: str
    issuer: str | None
    audience: str | None
    roles_claim: str


class JwtValidator:
    """Decodes and validates RS256 JWTs using a statically configured RSA public key PEM."""

    def __init__(self, config: JwtConfig) -> None:
        if not isinstance(config, JwtConfig):
            raise TypeError(
                f"JwtValidator requires a JwtConfig instance, got {type(config).__name__!r}. "
                "Build a JwtConfig from settings.auth.* fields."
            )
        self._public_key = config.public_key_pem
        self._issuer = config.issuer or None
        self._audience = config.audience or None
        self._roles_claim = config.roles_claim

    def validate(self, token: str) -> dict:
        """Decode and validate the JWT. Raises PlatformUnauthorizedError on any failure."""
        if not self._public_key:
            raise PlatformUnauthorizedError("JWT public key not configured")
        try:
            payload = pyjwt.decode(
                token,
                self._public_key,
                algorithms=["RS256"],
                issuer=self._issuer,
                audience=self._audience,
            )
        except pyjwt.ExpiredSignatureError as exc:
            raise PlatformUnauthorizedError("Token has expired") from exc
        except pyjwt.InvalidTokenError as exc:
            raise PlatformUnauthorizedError(f"Invalid token: {exc}") from exc
        return payload

    def extract_roles(self, payload: dict[str, Any]) -> list[str]:
        """Extract roles from the configured claim path (supports dotted notation)."""
        value: Any = payload
        for part in self._roles_claim.split("."):
            if not isinstance(value, dict):
                return []
            value = value.get(part)
            if value is None:
                return []
        if isinstance(value, list):
            return [str(r) for r in value]
        if isinstance(value, str):
            return [value]
        return []
