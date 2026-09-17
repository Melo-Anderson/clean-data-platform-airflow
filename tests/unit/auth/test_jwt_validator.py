from __future__ import annotations

import time

import jwt as pyjwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from app.auth.jwt_validator import JwtConfig, JwtValidator
from app.domain.shared.exceptions import PlatformUnauthorizedError


def test_jwt_validator_requires_jwt_config() -> None:
    """JwtValidator deve aceitar apenas JwtConfig — não Settings ou objeto arbitrário."""
    with pytest.raises(TypeError):
        JwtValidator(config=object())  # type: ignore[arg-type]


def test_jwt_validator_raises_unauthorized_when_no_public_key() -> None:
    config = JwtConfig(public_key_pem="", issuer=None, audience=None, roles_claim="roles")
    v = JwtValidator(config)
    with pytest.raises(PlatformUnauthorizedError, match="not configured"):
        v.validate("any-token")


def test_jwt_validator_extract_roles_returns_empty_for_missing_claim() -> None:
    config = JwtConfig(public_key_pem="key", issuer=None, audience=None, roles_claim="roles")
    v = JwtValidator(config)
    assert v.extract_roles({}) == []
    assert v.extract_roles({"other_key": ["admin"]}) == []


@pytest.fixture(scope="module")
def rsa_keypair():
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    public_pem = (
        private_key.public_key()
        .public_bytes(serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo)
        .decode()
    )
    return private_pem, public_pem


@pytest.fixture
def validator(rsa_keypair):
    _, public_pem = rsa_keypair
    config = JwtConfig(
        public_key_pem=public_pem,
        issuer=None,
        audience=None,
        roles_claim="roles",
    )
    return JwtValidator(config)


def _make_token(private_pem: str, payload: dict) -> str:
    return pyjwt.encode(payload, private_pem, algorithm="RS256")


def test_valid_token_returns_payload(rsa_keypair, validator):
    private_pem, _ = rsa_keypair
    token = _make_token(
        private_pem, {"sub": "user1", "roles": ["sre"], "exp": int(time.time()) + 300}
    )
    payload = validator.validate(token)
    assert payload["sub"] == "user1"


def test_expired_token_raises_unauthorized(rsa_keypair, validator):
    private_pem, _ = rsa_keypair
    token = _make_token(private_pem, {"sub": "user1", "exp": int(time.time()) - 10})
    with pytest.raises(PlatformUnauthorizedError):
        validator.validate(token)


def test_invalid_signature_raises_unauthorized(validator):
    # forge a token with a different key
    import cryptography.hazmat.primitives.asymmetric.rsa as _rsa

    other_key = _rsa.generate_private_key(public_exponent=65537, key_size=2048)
    other_pem = other_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.TraditionalOpenSSL,
        serialization.NoEncryption(),
    ).decode()
    token = _make_token(other_pem, {"sub": "attacker", "exp": int(time.time()) + 300})
    with pytest.raises(PlatformUnauthorizedError):
        validator.validate(token)


def test_extract_roles_from_list_claim(rsa_keypair, validator):
    private_pem, _ = rsa_keypair
    token = _make_token(
        private_pem,
        {"sub": "u", "roles": ["sre", "analytics_engineer"], "exp": int(time.time()) + 300},
    )
    payload = validator.validate(token)
    roles = validator.extract_roles(payload)
    assert "sre" in roles
    assert "analytics_engineer" in roles


def test_extract_roles_from_string_claim(rsa_keypair, validator):
    private_pem, _ = rsa_keypair
    token = _make_token(private_pem, {"sub": "u", "roles": "po_pm", "exp": int(time.time()) + 300})
    payload = validator.validate(token)
    roles = validator.extract_roles(payload)
    assert roles == ["po_pm"]


def test_extract_roles_missing_claim_returns_empty(rsa_keypair, validator):
    private_pem, _ = rsa_keypair
    token = _make_token(private_pem, {"sub": "u", "exp": int(time.time()) + 300})
    payload = validator.validate(token)
    assert validator.extract_roles(payload) == []
