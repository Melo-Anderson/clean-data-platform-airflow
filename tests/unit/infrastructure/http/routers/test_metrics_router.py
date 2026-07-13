from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.infrastructure.http.routers.metrics_router import router as metrics_router


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(metrics_router)
    return app


def test_metrics_returns_200() -> None:
    """GET /metrics must return 200."""
    client = TestClient(_make_app())
    response = client.get("/metrics")
    assert response.status_code == 200


def test_metrics_content_type_is_plaintext() -> None:
    """GET /metrics must return prometheus text format content type."""
    client = TestClient(_make_app())
    response = client.get("/metrics")
    assert "text/plain" in response.headers["content-type"]


def test_metrics_body_is_not_empty() -> None:
    """GET /metrics must return non-empty Prometheus exposition."""
    client = TestClient(_make_app())
    response = client.get("/metrics")
    assert len(response.text) > 0
