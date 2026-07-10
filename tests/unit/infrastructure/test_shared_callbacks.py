from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from app.infrastructure.airflow_callbacks.shared_callbacks import (
    alert_and_monitoring,
    check_dependencies,
    emit_monitoring_and_sla,
    quality_gate,
    success_notification,
    validate_compute_execution,
)


def test_check_dependencies_raises_when_upstream_not_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = False

    with patch("app.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        with pytest.raises(RuntimeError, match="Dependency not satisfied"):
            check_dependencies(
                pipeline_id="pipe-1",
                depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
                logical_date=datetime.now(tz=UTC),
            )


def test_check_dependencies_passes_when_all_satisfied() -> None:
    mock_client = MagicMock()
    mock_client.pipeline_succeeded_on.return_value = True

    with patch("app.infrastructure.platform_client.get_platform_client", return_value=mock_client):
        result = check_dependencies(
            pipeline_id="pipe-1",
            depends_on=[{"pipeline_id": "upstream-1", "dependency_type": "dataset"}],
            logical_date=datetime.now(tz=UTC),
        )
        assert result["dependencies_ok"] is True


def test_validate_compute_execution_passes_on_success() -> None:
    result = validate_compute_execution(job_result={"job_id": "j1", "status": "success"})
    assert result["status"] == "success"


def test_validate_compute_execution_raises_on_failure() -> None:
    with pytest.raises(RuntimeError, match="status='failed'"):
        validate_compute_execution(
            job_result={"job_id": "j1", "status": "failed", "error_message": "foo"}
        )


def test_quality_gate_passes_when_no_violations() -> None:
    with patch("app.infrastructure.quality_gate_evaluator.QualityGateEvaluator") as mock_eval:
        mock_eval.return_value.evaluate.return_value = []
        result = quality_gate(pipeline_id="p1", metrics={"cnt": 1}, quality_rules=[])
        assert result["quality_ok"] is True


def test_quality_gate_raises_when_violations_exist() -> None:
    with patch("app.infrastructure.quality_gate_evaluator.QualityGateEvaluator") as mock_eval:
        mock_eval.return_value.evaluate.return_value = ["violation 1"]
        with pytest.raises(RuntimeError, match="Quality gate failed"):
            quality_gate(pipeline_id="p1", metrics={}, quality_rules=[])


def test_success_notification_calls_adapter() -> None:
    with patch(
        "app.infrastructure.adapters.notifications.noop_notification_adapter.NoopNotificationAdapter"
    ) as mock_adapter:
        success_notification(pipeline_id="p1", pipeline_name="name", owner="po@co.com")
        mock_adapter.return_value.send_alert_sync.assert_called_once()


def test_alert_and_monitoring_calls_emit_failure() -> None:
    with (
        patch("app.infrastructure.monitoring_adapter.MonitoringAdapter") as mock_mon,
        patch("app.infrastructure.platform_client.get_platform_client") as mock_client,
    ):
        alert_and_monitoring({"params": {"pipeline_id": "p1"}})
        mock_mon.return_value.emit_failure.assert_called_once()
        mock_client.return_value.notify_failure.assert_called_once()


def test_emit_monitoring_and_sla_calls_upsert() -> None:
    with (
        patch("app.infrastructure.platform_client.get_platform_client") as mock_client,
        patch("app.infrastructure.monitoring_adapter.MonitoringAdapter"),
    ):
        emit_monitoring_and_sla(
            pipeline_id="p1",
            pipeline_name="name",
            sla_minutes=10,
            metrics={},
            dag_run_start="2026-01-01",
        )
        mock_client.return_value.upsert_pipeline_run.assert_called_once()
