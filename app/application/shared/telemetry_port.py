from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class TelemetryPort(Protocol):
    """Port for emitting platform telemetry (metrics and events).

    Implementations must NOT block the async event loop.
    The initial implementation (StructlogTelemetryAdapter) emits structured
    log events parseable by any log aggregator (ELK, Loki, Datadog Logs).

    Example:
        telemetry = StructlogTelemetryAdapter()
        telemetry.record_metric("airflow.trigger.latency_ms", 120.0, tags={"dag": "etl"})
        telemetry.record_event("pipeline.triggered", {"pipeline_id": "p1"})
    """

    def record_metric(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Emit a numeric metric.

        Args:
            name: Metric name in dot-notation (e.g. 'airflow.trigger.latency_ms').
            value: Numeric measurement.
            tags: Optional key-value labels for filtering/grouping.
        """
        ...

    def record_event(self, event_name: str, data: dict) -> None:
        """Emit a named business event with structured data.

        Args:
            event_name: Descriptive event name (e.g. 'pipeline.triggered').
            data: Flat dict of event attributes.
        """
        ...
