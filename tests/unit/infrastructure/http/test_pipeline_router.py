from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.http.schemas.pipeline_schemas import (
    CreatePipelineRequest,
    ExtractionObjectRequest,
)


@pytest.mark.asyncio
async def test_router_serializes_source_objects_for_use_case() -> None:
    """Router deve serializar source_objects via .model_dump() antes de passar ao use case."""
    body = CreatePipelineRequest(
        name="test_pipe",
        pipeline_type="ingestion",
        owner_email="eng@co.com",
        source_asset_id="asset-001",
        cron_schedule="0 * * * *",
        source_objects=[
            ExtractionObjectRequest(
                object_id="demo_orders",
                extraction_query="SELECT id FROM demo_orders",
            )
        ],
    )

    returned_pipeline = Pipeline(
        id="pipe-xx",
        name="test_pipe",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 * * * *")),
        source_asset_id="asset-001",
        schema_version="1.0",
    )

    with patch(
        "app.infrastructure.http.routers.pipeline_router.RegisterPipelineUseCase"
    ) as MockUseCase:
        mock_uc = MagicMock()
        mock_uc.execute = AsyncMock(return_value=returned_pipeline)
        MockUseCase.return_value = mock_uc

        source_objs_raw = (
            [o.model_dump() for o in body.source_objects] if body.source_objects else None
        )
        await mock_uc.execute(
            name=body.name,
            pipeline_type=body.pipeline_type,
            owner_email=body.owner_email,
            source_asset_id=body.source_asset_id,
            cron_schedule=body.cron_schedule,
            destination_asset_id=body.destination_asset_id or "",
            destination_objects=body.destination_objects,
            source_objects=source_objs_raw,
            compute=body.compute.model_dump() if body.compute else None,
            quality_rules=[r.model_dump() for r in body.quality_rules]
            if body.quality_rules
            else None,
            airflow_config=body.airflow_config.model_dump() if body.airflow_config else None,
        )

        call_kwargs = mock_uc.execute.call_args.kwargs
        assert call_kwargs["source_objects"] == [
            {
                "object_id": "demo_orders",
                "load_strategy": "full_load",
                "watermark_column": None,
                "page_size": 1000,
                "partition_column": None,
                "compression": "snappy",
                "encoding": "utf-8",
                "extraction_query": "SELECT id FROM demo_orders",
            }
        ]
