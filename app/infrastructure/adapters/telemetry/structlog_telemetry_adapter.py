from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)


class StructlogTelemetryAdapter:
    """TelemetryPort implementation that emits metrics as structured log events.

    Metric schema:  {"event": "metric", "metric_name": str, "value": float, "tags": dict|null}
    Event schema:   {"event": str, **data}

    No external dependencies beyond structlog. Swap to Prometheus/OTel by writing
    a new adapter class — zero application changes required.
    """

    def record_metric(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        logger.info("metric", metric_name=name, value=value, tags=tags)

    def record_event(self, event_name: str, data: dict) -> None:
        logger.info(event_name, **data)
