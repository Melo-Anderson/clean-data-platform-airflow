from __future__ import annotations

import structlog.contextvars
import pytest
from fastapi import FastAPI
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
