from __future__ import annotations

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from app.domain.shared.exceptions import PlatformNotFoundError


def test_not_found_error_returns_404() -> None:
    from fastapi import APIRouter
    from app.infrastructure.http.exception_handlers import register_exception_handlers

    app = FastAPI()
    router = APIRouter()

    @router.get("/test/{id}")
    async def get_item(id: str) -> dict:
        raise PlatformNotFoundError(f"Item not found: {id}")

    app.include_router(router)
    register_exception_handlers(app)

    client = TestClient(app, raise_server_exceptions=False)
    response = client.get("/test/missing-id")
    assert response.status_code == 404
    assert "missing-id" in response.json()["detail"]
