from __future__ import annotations

from unittest.mock import AsyncMock

import httpx
import respx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.config import get_settings
from app.infrastructure.http.routers.health_router import router as health_router
from app.infrastructure.persistence.database import get_db


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(health_router)
    return app


def test_health_returns_200_with_status() -> None:
    """GET /health must always return 200 with status=healthy."""
    client = TestClient(_make_app())
    response = client.get("/health")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "healthy"
    assert "version" in body


def test_health_does_not_depend_on_db() -> None:
    """Health check must succeed even without a database connection.

    This validates that /health is safe for Kubernetes liveness probes,
    which must not depend on downstream services.
    """
    client = TestClient(_make_app())
    # Without any DB setup, this must still return 200
    response = client.get("/health")
    assert response.status_code == 200


@respx.mock
def test_health_ready_success() -> None:
    """GET /health/ready must return 200 when dependencies are healthy."""
    app = _make_app()
    mock_db = AsyncMock()
    mock_db.execute.return_value = None
    app.dependency_overrides[get_db] = lambda: mock_db

    settings = get_settings()
    vault_url = settings.vault_url.rstrip("/") if settings.vault_url else "http://localhost:8200"
    respx.get(f"{vault_url}/v1/sys/health").mock(
        return_value=httpx.Response(200, json={"initialized": True})
    )

    client = TestClient(app)
    response = client.get("/health/ready")
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "ready"
    assert body["components"]["database"] == "up"
    assert "vault" in body["components"]
