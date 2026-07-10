from __future__ import annotations

from fastapi import FastAPI

from app.infrastructure.http.routers.asset_router import router as assets_router
from app.infrastructure.http.routers.discovery_router import router as discovery_router
from app.infrastructure.http.routers.endpoint_router import router as endpoints_router
from app.infrastructure.http.routers.lineage_router import router as lineage_router
from app.infrastructure.http.routers.pipeline_router import router as pipeline_router


from app.infrastructure.logging_config import configure_logging
from app.config import get_settings


def create_app() -> FastAPI:
    """Create and configure the FastAPI application. No business logic here."""
    settings = get_settings()
    configure_logging(
        log_level="DEBUG" if settings.debug else "INFO",
        json_output=not settings.debug,
    )
    app = FastAPI(
        title="Data Platform API",
        version="1.0.0",
        description="Data platform — DataAsset, Endpoint, and Pipeline management.",
    )
    app.include_router(assets_router, prefix="/assets", tags=["assets"])
    app.include_router(endpoints_router, prefix="/endpoints", tags=["endpoints"])
    app.include_router(discovery_router)
    app.include_router(lineage_router)
    app.include_router(pipeline_router)
    return app


app = create_app()
