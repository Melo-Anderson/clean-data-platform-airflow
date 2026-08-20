from __future__ import annotations

from app.infrastructure.adapters.compute.rest_api_helpers import (
    build_auth_headers,
    normalize_resource_path,
    resolve_jsonpath,
)


def test_build_auth_headers_bearer():
    headers = build_auth_headers("bearer", {"token": "secret123"})
    assert headers["Authorization"] == "Bearer secret123"


def test_build_auth_headers_api_key():
    headers = build_auth_headers("api_key", {"api_key": "k123", "api_key_header": "X-API-Key"})
    assert headers["X-API-Key"] == "k123"


def test_build_auth_headers_basic():
    headers = build_auth_headers("basic", {"username": "user", "password": "pass"})
    assert headers["Authorization"].startswith("Basic ")


def test_normalize_resource_path():
    assert normalize_resource_path("users") == "/api/v1/users"
    assert normalize_resource_path("/api/v1/orders") == "/api/v1/orders"
    assert normalize_resource_path("transactions") == "/api/v1/orders"
    assert normalize_resource_path("api/v1/api/v1/users") == "/api/v1/users"


def test_resolve_jsonpath():
    data = {"pagination": {"next": "token_abc"}}
    assert resolve_jsonpath(data, "pagination.next") == "token_abc"
    assert resolve_jsonpath(data, "missing.key") is None
