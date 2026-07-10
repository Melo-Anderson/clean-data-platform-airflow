from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.infrastructure.http.routers.health_router import router as health_router


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
