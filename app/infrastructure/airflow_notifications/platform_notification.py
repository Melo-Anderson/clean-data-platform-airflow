from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:

    class BaseNotification:
        def notify(self, context: dict[str, Any]) -> None: ...
else:
    try:
        from airflow.notifications.base import BaseNotification
    except ImportError:

        class BaseNotification:
            def notify(self, context: dict[str, Any]) -> None:
                pass


from app.infrastructure.platform_client import get_platform_client


class PlatformFailureNotification(BaseNotification):
    """Airflow 3 BaseNotification that notifies the platform API on DAG/task failure."""

    def notify(self, context: dict[str, Any]) -> None:
        params = context.get("params") or {}
        pipeline_id: str = params.get("pipeline_id") or ""
        if not pipeline_id:
            dag = context.get("dag")
            pipeline_id = getattr(dag, "dag_id", "") or "unknown_pipeline"

        ti = context.get("task_instance")
        task_id: str = getattr(ti, "task_id", "") or "unknown_task"

        get_platform_client().notify_failure(
            pipeline_id=pipeline_id,
            failed_task=task_id,
        )
