import pytest

from app.infrastructure.adapters.orchestration.logging_orchestrator_adapter import (
    LoggingOrchestratorAdapter,
)


@pytest.mark.asyncio
async def test_trigger_dag_does_not_raise() -> None:
    adapter = LoggingOrchestratorAdapter()
    # Deve logar e não lançar exceção
    await adapter.trigger_dag(
        pipeline_id="pipe-001",
        run_id="run-001",
        dag_run_id="manual__2026-01-01T00:00:00+00:00",
    )
