from __future__ import annotations

from typing import Any


class MonitoringAdapter:
    """Stub for MonitoringAdapter"""

    def emit_pipeline_metrics(
        self,
        pipeline_id: str,
        pipeline_name: str,
        sla_minutes: int,
        metrics: dict[str, Any],
        dag_run_start: str,
    ) -> None:
        pass
