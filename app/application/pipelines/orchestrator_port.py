from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class OrchestratorPort(Protocol):
    """Port para disparar execuções de DAGs no motor de orquestração (Airflow ou outro).

    Adapters devem ser resilientes a falhas transitórias e garantir idempotência:
    re-disparar um dag_run_id já existente deve ser tratado sem erro (HTTP 409 ignorado).

    Example:
        orchestrator = AirflowOrchestratorAdapter()
        await orchestrator.trigger_dag(
            pipeline_id="p-001",
            run_id="run-abc123",
            dag_run_id="user__2024-01-01T00:00:00Z",
            pipeline_name="ingest_orders",
        )
    """

    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
        pipeline_name: str,
    ) -> None:
        """Dispara um DAG run no motor de orquestração.

        Args:
            pipeline_id: ID interno da plataforma para o Pipeline.
            run_id: ID do PipelineRun criado antes do trigger.
            dag_run_id: ID único do run no Airflow (formato: '{triggered_by}__{iso_timestamp}').
            pipeline_name: Nome do DAG no Airflow (usado como dag_id).

        Raises:
            RuntimeError: Se o motor de orquestração estiver inacessível após retries.
        """
        ...
