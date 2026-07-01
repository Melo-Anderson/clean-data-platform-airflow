from __future__ import annotations

from fastapi import FastAPI

from app.infrastructure.http.routers.asset_router import router as assets_router
from app.infrastructure.http.routers.endpoint_router import router as endpoints_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application. No business logic here."""
    app = FastAPI(
        title="Data Platform API",
        version="0.1.0",
        description="Data platform — DataAsset, Endpoint, and Pipeline management.",
    )
    app.include_router(assets_router, prefix="/assets", tags=["assets"])
    app.include_router(endpoints_router, prefix="/endpoints", tags=["endpoints"])
    return app


app = create_app()
