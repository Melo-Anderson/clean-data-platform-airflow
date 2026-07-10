from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.infrastructure.http.middleware import add_observability_middleware


def test_middleware_adds_correlation_id_header() -> None:
    app = FastAPI()
    add_observability_middleware(app)

    @app.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    client = TestClient(app)
    response = client.get("/ping")
    assert response.status_code == 200
    assert "x-correlation-id" in response.headers


def test_middleware_propagates_given_correlation_id() -> None:
    app = FastAPI()
    add_observability_middleware(app)

    @app.get("/ping")
    async def ping() -> dict:
        return {"ok": True}

    client = TestClient(app)
    response = client.get("/ping", headers={"X-Correlation-ID": "test-id-123"})
    assert response.headers["x-correlation-id"] == "test-id-123"
