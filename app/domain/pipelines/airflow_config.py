from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class AirflowConfig:
    """
    Airflow 3 DAG-level configuration.

    sla_minutes: emit_monitoring_and_sla fires a SLA breach alert if the DAG does not
      complete within this window from scheduled time.
    pool: Airflow worker pool. Controls resource concurrency per group.
    """

    retries: int = 3
    retry_delay_minutes: int = 5
    execution_timeout_minutes: int = 120
    sla_minutes: int = 90
    tags: tuple[str, ...] = field(default_factory=tuple)
    pool: str = "default_pool"
