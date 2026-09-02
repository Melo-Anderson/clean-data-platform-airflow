import contextlib
import logging
from typing import Any

from fastapi import FastAPI

try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        SpanExporter,
    )
except ImportError:
    trace = None  # type: ignore[assignment]
    OTLPSpanExporter = None  # type: ignore[misc,assignment]
    FastAPIInstrumentor = None  # type: ignore[misc,assignment]
    Resource = None  # type: ignore[misc,assignment]
    TracerProvider = None  # type: ignore[misc,assignment]
    BatchSpanProcessor = None  # type: ignore[misc,assignment]
    ConsoleSpanExporter = None  # type: ignore[misc,assignment]
    SpanExporter = None  # type: ignore[misc,assignment]

logger = logging.getLogger(__name__)


def setup_telemetry(
    app: FastAPI,
    service_name: str = "data-platform-api",
    otlp_endpoint: str | None = None,
    exporter: SpanExporter | None = None,
) -> None:
    """Configure OpenTelemetry tracing. Call once in create_app().

    If otlp_endpoint is provided, exports to that collector via gRPC.
    Otherwise falls back to ConsoleSpanExporter (stdout) for local development.
    """
    if (
        trace is None
        or Resource is None
        or TracerProvider is None
        or FastAPIInstrumentor is None
        or BatchSpanProcessor is None
        or ConsoleSpanExporter is None
    ):
        logger.warning("opentelemetry packages not installed; skipping tracing initialization")
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    if exporter is not None:
        raw_exporter: Any = exporter
    elif otlp_endpoint and OTLPSpanExporter is not None:
        raw_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
    else:
        raw_exporter = ConsoleSpanExporter()

    provider.add_span_processor(BatchSpanProcessor(raw_exporter))
    with contextlib.suppress(Exception):
        trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
