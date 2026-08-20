from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from app.application.pipelines.report_pipeline_run_use_case import ReportPipelineRunUseCase
from app.application.shared.ports.quality_gate_port import QualityGatePort
from app.domain.shared.exceptions import PlatformNotFoundError


@pytest.mark.asyncio
async def test_report_uses_injected_quality_gate() -> None:
    """Quality gate deve ser injetado, não instanciado internamente."""
    mock_gate = MagicMock(spec=QualityGatePort)
    mock_gate.evaluate.return_value = []

    uow = AsyncMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=False)

    run = MagicMock(id="run-1", pipeline_id="pipe-1")
    pipeline = MagicMock(quality_rules=[])
    uow.pipeline_runs.find_by_id = AsyncMock(return_value=run)
    uow.pipelines.find_by_id = AsyncMock(return_value=pipeline)
    uow.pipeline_runs.save = AsyncMock(return_value=run)

    use_case = ReportPipelineRunUseCase(uow=uow, quality_gate=mock_gate)
    await use_case.execute("run-1", {"row_count": 100})

    mock_gate.evaluate.assert_called_once()


@pytest.mark.asyncio
async def test_report_raises_when_run_not_found() -> None:
    uow = AsyncMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=False)
    uow.pipeline_runs.find_by_id = AsyncMock(return_value=None)

    use_case = ReportPipelineRunUseCase(uow=uow, quality_gate=MagicMock())
    with pytest.raises(PlatformNotFoundError, match="PipelineRun not found: missing-run"):
        await use_case.execute("missing-run", {"row_count": 100})


@pytest.mark.asyncio
async def test_report_raises_when_pipeline_not_found() -> None:
    uow = AsyncMock()
    uow.__aenter__ = AsyncMock(return_value=uow)
    uow.__aexit__ = AsyncMock(return_value=False)
    run = MagicMock(id="run-1", pipeline_id="missing-pipe")
    uow.pipeline_runs.find_by_id = AsyncMock(return_value=run)
    uow.pipelines.find_by_id = AsyncMock(return_value=None)

    use_case = ReportPipelineRunUseCase(uow=uow, quality_gate=MagicMock())
    with pytest.raises(PlatformNotFoundError, match="Pipeline not found: missing-pipe"):
        await use_case.execute("run-1", {"row_count": 100})
