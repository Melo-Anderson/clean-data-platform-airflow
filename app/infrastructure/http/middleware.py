from __future__ import annotations

import logging
import time
import uuid

from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

logger = logging.getLogger(__name__)


class CorrelationIdMiddleware(BaseHTTPMiddleware):
    """Injects X-Correlation-ID into every request/response pair.

    If the client sends X-Correlation-ID, it is preserved; otherwise a new UUID4
    is generated. The correlation_id is included in every structured log line
    emitted during the request lifecycle.
    """

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        correlation_id = request.headers.get("X-Correlation-ID") or str(uuid.uuid4())
        start = time.monotonic()

        response = await call_next(request)

        duration_ms = (time.monotonic() - start) * 1000
        response.headers["X-Correlation-ID"] = correlation_id
        logger.info(
            "http.request | method=%s path=%s status=%d duration_ms=%.1f correlation_id=%s",
            request.method,
            request.url.path,
            response.status_code,
            duration_ms,
            correlation_id,
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
