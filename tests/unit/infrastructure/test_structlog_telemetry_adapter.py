from __future__ import annotations

import structlog.testing

from app.application.shared.telemetry_port import TelemetryPort
from app.infrastructure.adapters.telemetry.structlog_telemetry_adapter import (
    StructlogTelemetryAdapter,
)


def test_adapter_satisfies_port_protocol():
    adapter = StructlogTelemetryAdapter()
    assert isinstance(adapter, TelemetryPort)


def test_record_metric_emits_structured_log():
    adapter = StructlogTelemetryAdapter()
    with structlog.testing.capture_logs() as logs:
        adapter.record_metric("airflow.trigger.latency_ms", 120.5, tags={"dag": "etl"})
    assert len(logs) == 1
    log = logs[0]
    assert log["event"] == "metric"
    assert log["metric_name"] == "airflow.trigger.latency_ms"
    assert log["value"] == 120.5
    assert log["tags"] == {"dag": "etl"}


def test_record_event_emits_structured_log():
    adapter = StructlogTelemetryAdapter()
    with structlog.testing.capture_logs() as logs:
        adapter.record_event("airflow.dag_trigger.success", {"dag_id": "etl", "run_id": "r1"})
    assert len(logs) == 1
    log = logs[0]
    assert log["event"] == "airflow.dag_trigger.success"
    assert log["dag_id"] == "etl"


def test_record_metric_without_tags():
    adapter = StructlogTelemetryAdapter()
    with structlog.testing.capture_logs() as logs:
        adapter.record_metric("some.counter", 1.0)
    assert logs[0]["tags"] is None
