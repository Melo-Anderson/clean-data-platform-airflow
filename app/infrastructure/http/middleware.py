from __future__ import annotations

import time
import uuid

import structlog
import structlog.contextvars
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import Histogram
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = structlog.get_logger(__name__)

_HTTP_DURATION = Histogram(
    "http_request_duration_seconds",
    "Duration of HTTP requests in seconds",
    ["method", "path", "status"],
)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Injects X-Correlation-ID into every request/response pair.

    If the client sends X-Correlation-ID, it is preserved; otherwise a new UUID4
    is generated. The correlation_id is bound to structlog.contextvars so it
    appears automatically in every structured log line during the request lifecycle.
    Also observes HTTP request duration in the prometheus histogram.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(correlation_id=correlation_id)

        start = time.monotonic()
        status_code = "500"
        try:
            response = await call_next(request)
            status_code = str(response.status_code)
        finally:
            duration = time.monotonic() - start
            _HTTP_DURATION.labels(
                method=request.method,
                path=request.url.path,
                status=status_code,
            ).observe(duration)

        response.headers["X-Correlation-ID"] = correlation_id
        logger.info(
            "http.request",
            method=request.method,
            path=request.url.path,
            status=response.status_code,
            duration_ms=round(duration * 1000, 1),
        )
        return response


def add_observability_middleware(app: FastAPI, allow_origins: list[str] | None = None) -> None:
    """Register CORS and CorrelationId middleware on the app.

    Call once in create_app(), after routers are registered.
    """
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allow_origins or ["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    app.add_middleware(CorrelationIdMiddleware)
