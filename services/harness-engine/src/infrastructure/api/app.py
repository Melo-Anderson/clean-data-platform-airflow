"""FastAPI application factory."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.infrastructure.api.routes import router


def create_app() -> FastAPI:
    app = FastAPI(title="YAML Harness Engine AI", version="0.1.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["POST", "GET"], allow_headers=["*"]
    )
    app.include_router(router, prefix="/api/v1")
    return app


app = create_app()
