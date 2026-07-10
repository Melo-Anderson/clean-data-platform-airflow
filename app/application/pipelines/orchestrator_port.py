from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class OrchestratorPort(Protocol):
    """Port para disparar execuções de DAGs no Airflow (ou qualquer compute engine)."""

    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
        pipeline_name: str,
    ) -> None: ...
