from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from httpx import AsyncClient

from app.application.pipelines.register_pipeline import RegisterPipelineUseCase
from app.infrastructure.http.dependencies import get_register_pipeline_use_case


@pytest.mark.asyncio
async def test_pipeline_router_uses_injected_use_case(app, ae_client: AsyncClient) -> None:
    mock_use_case = AsyncMock(spec=RegisterPipelineUseCase)
    mock_pipeline = AsyncMock()
    mock_pipeline.id = "p-123"
    mock_pipeline.name = "Injected Pipeline"
    mock_pipeline.type.value = "ingestion"
    mock_pipeline.owner.value = "admin@co.com"
    mock_pipeline.source_asset = "src"
    mock_pipeline.destination_asset = "dst"
    mock_pipeline.schedule.cron_schedule = None
    mock_use_case.execute.return_value = mock_pipeline

    app.dependency_overrides[get_register_pipeline_use_case] = lambda: mock_use_case

    try:
        payload = {
            "name": "Injected Pipeline",
            "pipeline_type": "ingestion",
            "owner_email": "admin@co.com",
            "source_asset": "src",
        }
        res = await ae_client.post("/v1/pipelines/", json=payload)
        assert res.status_code == 201
        assert res.json()["id"] == "p-123"
        mock_use_case.execute.assert_awaited_once()
    finally:
        app.dependency_overrides.clear()
