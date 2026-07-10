from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.domain.shared.exceptions import PlatformNotFoundError, PlatformValidationError


def register_exception_handlers(app: FastAPI) -> None:
    """Register domain exception → HTTP status code mappings.

    Call once in create_app() so all routers share the same error contract.
    """

    @app.exception_handler(PlatformNotFoundError)
    async def not_found_handler(request: Request, exc: PlatformNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content={"detail": str(exc)})

    @app.exception_handler(PlatformValidationError)
    async def validation_handler(request: Request, exc: PlatformValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content={"detail": str(exc)})
