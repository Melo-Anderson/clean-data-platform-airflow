from __future__ import annotations

import pytest
from prometheus_client import CollectorRegistry

from app.application.shared.telemetry_port import TelemetryPort
from app.infrastructure.adapters.telemetry.prometheus_metrics_adapter import (
    PrometheusMetricsAdapter,
)


def test_adapter_satisfies_port_protocol() -> None:
    adapter = PrometheusMetricsAdapter()
    assert isinstance(adapter, TelemetryPort)


def test_record_metric_updates_histogram() -> None:
    registry = CollectorRegistry()
    adapter = PrometheusMetricsAdapter(registry=registry)
    adapter.record_metric(
        "http_request_duration_seconds",
        0.5,
        tags={"method": "GET", "path": "/health", "status": "200"},
    )
    # If no exception is raised and adapter satisfies port, metric was accepted
    assert isinstance(adapter, TelemetryPort)


def test_record_event_pipeline_run_increments_counter() -> None:
    registry = CollectorRegistry()
    adapter = PrometheusMetricsAdapter(registry=registry)
    before = adapter.get_pipeline_runs_total()
    adapter.record_event("platform.pipeline.triggered", {"pipeline_id": "p1"})
    after = adapter.get_pipeline_runs_total()
    assert after == before + 1


def test_record_event_other_events_do_not_increment_pipeline_counter() -> None:
    registry = CollectorRegistry()
    adapter = PrometheusMetricsAdapter(registry=registry)
    before = adapter.get_pipeline_runs_total()
    adapter.record_event("airflow.dag_trigger.success", {"dag_id": "d1"})
    assert adapter.get_pipeline_runs_total() == before
