from __future__ import annotations

import structlog
from prometheus_client import CollectorRegistry, Counter, Histogram

logger = structlog.get_logger(__name__)

_PIPELINE_RUNS_EVENT = "platform.pipeline.triggered"


class PrometheusMetricsAdapter:
    """TelemetryPort implementation that emits metrics to Prometheus.

    Uses isolated registries in tests (pass ``registry=CollectorRegistry()``).
    In production, pass no registry to use the default global registry.

    Metric schema:
        HTTP histogram: labels (method, path, status)
        Pipeline counter: label-less, incremented on 'platform.pipeline.triggered' events
    """

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        from prometheus_client import REGISTRY

        reg = registry or REGISTRY
        self._http_histogram = Histogram(
            "http_request_duration_seconds",
            "Duration of HTTP requests in seconds",
            ["method", "path", "status"],
            registry=reg,
        )
        self._pipeline_counter = Counter(
            "platform_pipeline_runs_total",
            "Total pipeline executions started",
            registry=reg,
        )

    def record_metric(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Observe a histogram metric. Tags must contain method, path, status for HTTP metrics."""
        if name == "http_request_duration_seconds" and tags:
            self._http_histogram.labels(
                method=tags.get("method", ""),
                path=tags.get("path", ""),
                status=tags.get("status", ""),
            ).observe(value)
        else:
            logger.debug("prometheus_metric_skipped", metric_name=name, metric_value=value)

    def record_event(self, event_name: str, data: dict[str, object]) -> None:
        """Increment pipeline counter on trigger events."""
        if event_name == _PIPELINE_RUNS_EVENT:
            self._pipeline_counter.inc()
        else:
            logger.debug("prometheus_event_skipped", event_name=event_name)

    def get_pipeline_runs_total(self) -> float:
        """Helper for testing — returns current counter value."""
        val = self._pipeline_counter._value.get()
        return float(val)
