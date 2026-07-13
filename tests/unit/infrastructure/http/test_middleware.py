from __future__ import annotations

import pytest
import structlog.contextvars
from fastapi import FastAPI
from fastapi.testclient import TestClient
from httpx import ASGITransport, AsyncClient

from app.infrastructure.http.middleware import add_observability_middleware


def make_app() -> FastAPI:
    app = FastAPI()
    add_observability_middleware(app)

    @app.get("/check")
    async def check():
        ctx = structlog.contextvars.get_contextvars()
        return {"correlation_id": ctx.get("correlation_id", "")}

    return app


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


@pytest.mark.asyncio
async def test_correlation_id_bound_to_contextvars():
    app = make_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/check", headers={"X-Correlation-ID": "test-id-123"})
    assert resp.status_code == 200
    assert resp.json()["correlation_id"] == "test-id-123"


@pytest.mark.asyncio
async def test_generates_correlation_id_if_missing():
    app = make_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/check")
    assert resp.json()["correlation_id"] != ""


@pytest.mark.asyncio
async def test_correlation_id_in_response_header():
    app = make_app()
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        resp = await c.get("/check", headers={"X-Correlation-ID": "my-trace-id"})
    assert resp.headers["x-correlation-id"] == "my-trace-id"
