from __future__ import annotations

from unittest.mock import MagicMock, patch

from app.infrastructure.airflow_notifications.platform_notification import (
    PlatformFailureNotification,
)


def test_platform_failure_notification_calls_notify_failure() -> None:
    context = {
        "params": {"pipeline_id": "pipe-xyz"},
        "task_instance": MagicMock(task_id="compute_engine.submit_compute_job"),
    }
    with patch(
        "app.infrastructure.airflow_notifications.platform_notification.get_platform_client"
    ) as mock_client:
        client = MagicMock()
        mock_client.return_value = client
        PlatformFailureNotification().notify(context)
        client.notify_failure.assert_called_once_with(
            pipeline_id="pipe-xyz",
            failed_task="compute_engine.submit_compute_job",
        )


def test_platform_failure_notification_resolves_pipeline_id_from_dag_id_when_params_absent() -> (
    None
):
    notification = PlatformFailureNotification()
    mock_dag = MagicMock(dag_id="pipe_transformation_orders")
    mock_ti = MagicMock(task_id="run_transformations")
    context = {"dag": mock_dag, "task_instance": mock_ti}

    with patch(
        "app.infrastructure.airflow_notifications.platform_notification.get_platform_client"
    ) as mock_get:
        mock_client = MagicMock()
        mock_get.return_value = mock_client
        notification.notify(context)
        mock_client.notify_failure.assert_called_once_with(
            pipeline_id="pipe_transformation_orders",
            failed_task="run_transformations",
        )
