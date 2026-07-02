from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from app.infrastructure.adapters.notifications.noop_notification_adapter import (
    NoopNotificationAdapter,
)
from app.infrastructure.monitoring_adapter import MonitoringAdapter
from app.infrastructure.platform_client import get_platform_client
from app.infrastructure.quality_gate_evaluator import QualityGateEvaluator


def check_dependencies(
    *,
    pipeline_id: str,
    depends_on: list[dict[str, Any]],
    logical_date: datetime,
) -> dict[str, Any]:
    """
    Validate upstream pipeline completions and resource availability.

    For DATASET dependencies: verify Airflow Asset event was received.
    For EXTERNAL_EVENT: verify external event payload was received.
    For MANUAL: assert manual approval flag is set.

    Returns {"dependencies_ok": True, "checked_at": "..."}.
    Raises RuntimeError if any dependency is not satisfied — fails the task.
    """
    client = get_platform_client()
    for dep in depends_on:
        if not client.pipeline_succeeded_on(
            pipeline_id=dep["pipeline_id"],
            require_same_day=dep.get("require_same_day", False),
            logical_date=logical_date,
            dependency_type=dep.get("dependency_type", "dataset"),
        ):
            raise RuntimeError(
                f"Dependency not satisfied: pipeline_id={dep['pipeline_id']!r} "
                f"require_same_day={dep.get('require_same_day')} "
                f"dependency_type={dep.get('dependency_type')}"
            )
    return {"dependencies_ok": True, "checked_at": datetime.now(tz=UTC).isoformat()}


def validate_compute_execution(
    *,
    job_result: dict[str, Any],
) -> dict[str, Any]:
    """
    Validate compute job terminal state. Raises on failure/cancellation/timeout.

    Returns the job_result dict unchanged on success (for downstream XCom).
    """
    status = job_result.get("status")
    if status != "success":
        error = job_result.get("error_message", "Unknown error")
        raise RuntimeError(
            f"Compute job {job_result.get('job_id')!r} ended with status={status!r}. Error: {error}"
        )
    return job_result


def quality_gate(
    *,
    pipeline_id: str,
    metrics: dict[str, Any],
    quality_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    """
    Confront configured quality rules against metrics produced by the compute engine.

    Metrics are read from metrics.json written by the compute job.
    Raises RuntimeError if any rule is violated — marks task as quality_failed.
    """
    evaluator = QualityGateEvaluator()
    failures = evaluator.evaluate(metrics=metrics, rules=quality_rules)
    if failures:
        raise RuntimeError(
            f"Quality gate failed for pipeline {pipeline_id!r}. Violations: {failures}"
        )
    return {"quality_ok": True, "violations": [], "metrics": metrics}


def emit_monitoring_and_sla(
    *,
    pipeline_id: str,
    pipeline_name: str,
    sla_minutes: int,
    metrics: dict[str, Any],
    dag_run_start: str,
) -> None:
    """
    Emit pipeline execution metrics to the monitoring platform and evaluate SLA.

    Always runs (trigger_rule=all_done) to ensure metrics are emitted even on failure.
    Sends to configured monitoring adapter (Datadog, Cloud Monitoring, etc.).
    """
    adapter = MonitoringAdapter()
    adapter.emit_pipeline_metrics(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline_name,
        sla_minutes=sla_minutes,
        metrics=metrics,
        dag_run_start=dag_run_start,
    )


def success_notification(*, pipeline_id: str, pipeline_name: str, owner: str) -> None:
    """Send success notification to the pipeline owner after all tasks complete."""
    adapter = NoopNotificationAdapter()
    # Uses sync alert method — no asyncio.run()
    adapter.send_alert_sync(
        channel=owner,
        title=f"Pipeline '{pipeline_name}' completed successfully",
        message=f"pipeline_id={pipeline_id}",
        level="info",
    )


def alert_and_monitoring(context: dict[str, Any]) -> None:
    """
    Airflow on_failure_callback. Called when any task fails.

    Sends alert to the pipeline owner and emits failure metrics.
    Registered as on_failure_callback at the DAG level.
    """
    dag_run = context.get("dag_run")
    pipeline_id = context.get("params", {}).get("pipeline_id", "unknown")
    pipeline_name = context.get("params", {}).get("pipeline_name", "unknown")
    owner = context.get("params", {}).get("owner", "unknown")

    adapter = NoopNotificationAdapter()
    adapter.send_alert_sync(
        channel=owner,
        title=f"Pipeline '{pipeline_name}' FAILED",
        message=f"pipeline_id={pipeline_id}, run_id={dag_run.run_id if dag_run else 'unknown'}",
        level="error",
    )
