from __future__ import annotations

from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.pipelines.trigger_pipeline_run import TriggerPipelineRunUseCase
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_run import PipelineRun
from app.domain.pipelines.pipeline_run_status import PipelineRunStatus
from app.domain.pipelines.pipeline_type import PipelineType


def _make_existing_run(pipeline_id: str, idempotency_key: str) -> PipelineRun:
    return PipelineRun(
        id="run-existing-1",
        pipeline_id=pipeline_id,
        pipeline_name="sample_pipeline",
        pipeline_type="ingestion",
        dag_run_id="manual__20260922_000000",
        status=PipelineRunStatus.RUNNING,
        started_at=datetime(2026, 9, 22, tzinfo=UTC),
        idempotency_key=idempotency_key,
    )


def _make_mock_pipeline(pipeline_id: str) -> MagicMock:
    mock = MagicMock(spec=Pipeline)
    mock.id = pipeline_id
    mock.name = "sample_pipeline"
    mock.type = PipelineType.INGESTION
    return mock


def _make_mock_uow(pipeline: MagicMock, existing_run: PipelineRun | None) -> MagicMock:
    mock_uow = MagicMock()
    mock_uow.__aenter__ = AsyncMock(return_value=mock_uow)
    mock_uow.__aexit__ = AsyncMock(return_value=None)
    mock_uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    mock_uow.pipeline_runs.find_by_idempotency_key = AsyncMock(return_value=existing_run)
    return mock_uow


@pytest.mark.asyncio
async def test_returns_existing_run_when_idempotency_key_matches() -> None:
    pipeline_id = "test-pipeline-123"
    idempotency_key = "req-key-abc-123"

    existing_run = _make_existing_run(pipeline_id, idempotency_key)
    mock_uow = _make_mock_uow(_make_mock_pipeline(pipeline_id), existing_run)

    mock_orchestrator = MagicMock()
    mock_orchestrator.trigger_dag = AsyncMock()

    use_case = TriggerPipelineRunUseCase(
        uow=mock_uow,
        orchestrator=mock_orchestrator,
        yaml_generator=MagicMock(),
        dag_generator=MagicMock(),
    )

    result = await use_case.execute(
        pipeline_id=pipeline_id,
        triggered_by="api_user",
        idempotency_key=idempotency_key,
    )

    assert result.id == "run-existing-1"
    mock_orchestrator.trigger_dag.assert_not_called()


@pytest.mark.asyncio
async def test_creates_new_run_when_no_idempotency_key_provided() -> None:
    """When idempotency_key=None, the use case always triggers a new run."""
    pipeline_id = "test-pipeline-456"
    mock_pipeline = _make_mock_pipeline(pipeline_id)
    mock_uow = _make_mock_uow(mock_pipeline, existing_run=None)
    new_run = _make_existing_run(pipeline_id, "unused")
    mock_uow.pipeline_runs.save = AsyncMock(return_value=new_run)
    mock_uow.commit = AsyncMock()

    mock_orchestrator = MagicMock()
    mock_orchestrator.trigger_dag = AsyncMock()
    mock_yaml_gen = MagicMock()
    mock_yaml_gen.generate = MagicMock(return_value="yaml_str")
    mock_dag_gen = MagicMock()
    mock_dag_gen.generate = MagicMock(return_value="# dag code")

    use_case = TriggerPipelineRunUseCase(
        uow=mock_uow,
        orchestrator=mock_orchestrator,
        yaml_generator=mock_yaml_gen,
        dag_generator=mock_dag_gen,
        dags_path="/tmp/test_dags",
    )

    result = await use_case.execute(
        pipeline_id=pipeline_id,
        triggered_by="api_user",
        idempotency_key=None,
    )

    mock_orchestrator.trigger_dag.assert_called_once()
    assert result.id == "run-existing-1"
