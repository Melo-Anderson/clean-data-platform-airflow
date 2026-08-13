from __future__ import annotations

import logging
import os

from fastapi import FastAPI

logger = logging.getLogger(__name__)


def setup_telemetry(app: FastAPI, service_name: str = "data-platform-api") -> None:
    """Configure OpenTelemetry tracing. Call once in create_app().

    If OTEL_EXPORTER_OTLP_ENDPOINT is set, exports to that collector via gRPC.
    Otherwise falls back to ConsoleSpanExporter (stdout) for local development.
    """
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
        logger.warning("opentelemetry packages not installed; skipping tracing initialization")
        return

    resource = Resource.create({"service.name": service_name})
    provider = TracerProvider(resource=resource)

    otlp_endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
    raw_exporter: SpanExporter
    if otlp_endpoint:
        raw_exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
    else:
        raw_exporter = ConsoleSpanExporter()

    provider.add_span_processor(BatchSpanProcessor(raw_exporter))
    trace.set_tracer_provider(provider)
    FastAPIInstrumentor.instrument_app(app)
