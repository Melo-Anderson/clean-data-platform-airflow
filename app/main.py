from __future__ import annotations

from fastapi import FastAPI

from app.config import get_settings
from app.infrastructure.http.routers.asset_router import router as assets_router
from app.infrastructure.http.routers.discovery_router import router as discovery_router
from app.infrastructure.http.routers.endpoint_router import router as endpoints_router
from app.infrastructure.http.routers.lineage_router import router as lineage_router
from app.infrastructure.http.routers.pipeline_router import router as pipeline_router
from app.infrastructure.logging_config import configure_logging


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
    from app.infrastructure.http.exception_handlers import register_exception_handlers

    register_exception_handlers(app)

    app.include_router(assets_router, prefix="/v1/assets", tags=["assets"])
    app.include_router(endpoints_router, prefix="/v1/endpoints", tags=["endpoints"])
    app.include_router(discovery_router, prefix="/v1")
    app.include_router(lineage_router, prefix="/v1")
    app.include_router(pipeline_router, prefix="/v1")

    from app.infrastructure.http.middleware import add_observability_middleware

    add_observability_middleware(app)
    return app


app = create_app()
