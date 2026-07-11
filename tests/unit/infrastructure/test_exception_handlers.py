from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.domain.shared.exceptions import (
    CircuitBreakerOpenError, DataQualityViolationException, DomainException,
    PipelineExecutionException, PlatformNotFoundError, PlatformValidationError,
)
from app.infrastructure.http.exception_handlers import register_exception_handlers


def make_app_with_exception(exc: Exception) -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)

    @app.get("/test")
    async def raise_exc():
        raise exc

    return app


async def get(app: FastAPI, path: str = "/test"):
    async with AsyncClient(transport=ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test") as c:
        return await c.get(path)


@pytest.mark.asyncio
async def test_not_found_returns_404():
    resp = await get(make_app_with_exception(PlatformNotFoundError("Pipeline X not found")))
    assert resp.status_code == 404
    body = resp.json()
    assert body["status"] == 404
    assert "Pipeline X not found" in body["detail"]


@pytest.mark.asyncio
async def test_validation_error_returns_422():
    resp = await get(make_app_with_exception(PlatformValidationError("bad cron")))
    assert resp.status_code == 422


@pytest.mark.asyncio
async def test_pipeline_execution_returns_503():
    resp = await get(make_app_with_exception(PipelineExecutionException("Airflow down")))
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_data_quality_returns_409():
    resp = await get(make_app_with_exception(DataQualityViolationException("check failed")))
    assert resp.status_code == 409


@pytest.mark.asyncio
async def test_circuit_breaker_open_returns_503():
    resp = await get(make_app_with_exception(CircuitBreakerOpenError("airflow-api")))
    assert resp.status_code == 503


@pytest.mark.asyncio
async def test_generic_exception_returns_500_without_details():
    resp = await get(make_app_with_exception(RuntimeError("db connection pool exhausted")))
    assert resp.status_code == 500
    assert "db connection pool exhausted" not in resp.text
    assert resp.json()["status"] == 500


@pytest.mark.asyncio
async def test_rfc7807_schema_present():
    resp = await get(make_app_with_exception(PlatformNotFoundError("X")))
    body = resp.json()
    for field in ("type", "title", "status", "detail"):
        assert field in body
