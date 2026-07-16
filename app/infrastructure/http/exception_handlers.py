from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from sqlalchemy.exc import IntegrityError

from app.domain.endpoints.exceptions import UnsupportedEndpointError
from app.domain.shared.exceptions import (
    CircuitBreakerOpenError,
    DataQualityViolationException,
    DomainException,
    PipelineExecutionException,
    PlatformForbiddenError,
    PlatformNotFoundError,
    PlatformUnauthorizedError,
    PlatformValidationError,
)

logger = logging.getLogger(__name__)


def _problem(status: int, title: str, detail: str) -> dict:
    return {
        "type": f"https://platform.internal/errors/{title.lower().replace(' ', '-')}",
        "title": title,
        "status": status,
        "detail": detail,
    }


def register_exception_handlers(app: FastAPI) -> None:
    """Register domain exception to HTTP status code mappings (RFC 7807).

    Call once in create_app() so all routers share the same error contract.
    """

    @app.exception_handler(PlatformNotFoundError)
    async def not_found_handler(request: Request, exc: PlatformNotFoundError) -> JSONResponse:
        return JSONResponse(status_code=404, content=_problem(404, "Not Found", str(exc)))

    @app.exception_handler(PlatformValidationError)
    async def validation_handler(request: Request, exc: PlatformValidationError) -> JSONResponse:
        return JSONResponse(status_code=422, content=_problem(422, "Validation Error", str(exc)))

    @app.exception_handler(PipelineExecutionException)
    async def pipeline_exec_handler(
        request: Request, exc: PipelineExecutionException
    ) -> JSONResponse:
        return JSONResponse(
            status_code=503, content=_problem(503, "Pipeline Execution Failed", str(exc))
        )

    @app.exception_handler(DataQualityViolationException)
    async def data_quality_handler(
        request: Request, exc: DataQualityViolationException
    ) -> JSONResponse:
        return JSONResponse(
            status_code=409, content=_problem(409, "Data Quality Violation", str(exc))
        )

    @app.exception_handler(CircuitBreakerOpenError)
    async def circuit_breaker_handler(
        request: Request, exc: CircuitBreakerOpenError
    ) -> JSONResponse:
        return JSONResponse(status_code=503, content=_problem(503, "Service Unavailable", str(exc)))

    @app.exception_handler(PlatformUnauthorizedError)
    async def unauthorized_handler(
        request: Request, exc: PlatformUnauthorizedError
    ) -> JSONResponse:
        return JSONResponse(status_code=401, content=_problem(401, "Unauthorized", str(exc)))

    @app.exception_handler(PlatformForbiddenError)
    async def forbidden_handler(request: Request, exc: PlatformForbiddenError) -> JSONResponse:
        return JSONResponse(status_code=403, content=_problem(403, "Forbidden", str(exc)))

    @app.exception_handler(UnsupportedEndpointError)
    async def unsupported_endpoint_handler(
        request: Request, exc: UnsupportedEndpointError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=422, content=_problem(422, "Unsupported Endpoint Type", str(exc))
        )

    @app.exception_handler(DomainException)
    async def domain_exception_handler(request: Request, exc: DomainException) -> JSONResponse:
        return JSONResponse(status_code=400, content=_problem(400, "Bad Request", str(exc)))

    @app.exception_handler(IntegrityError)
    async def integrity_handler(request: Request, exc: IntegrityError) -> JSONResponse:
        logger.warning("Integrity error: %s", exc)
        return JSONResponse(
            status_code=409,
            content=_problem(
                409, "Conflict", "Resource already exists or database constraint violated."
            ),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        logger.critical(
            "Unhandled exception: %s %s -> %s: %s",
            request.method,
            request.url.path,
            type(exc).__name__,
            exc,
            exc_info=True,
        )
        return JSONResponse(
            status_code=500,
            content=_problem(500, "Internal Server Error", "An unexpected error occurred."),
        )
