from unittest.mock import AsyncMock, MagicMock

import pytest

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.persistence.repositories.sql_pipeline_repository import (
    SqlPipelineRepository,
)


def make_pipeline() -> Pipeline:
    return Pipeline(
        id="pipe-001",
        name="ingest-e2e-asset",
        type=PipelineType.INGESTION,
        owner=EmailAddress("e2e@co.com"),
        schedule=ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule("0 0 * * *"),
        ),
        source_asset_id="asset-001",
        schema_version="1.0",
    )


@pytest.mark.asyncio
async def test_save_returns_pipeline_with_id() -> None:
    session = MagicMock()
    session.add = MagicMock()
    session.flush = AsyncMock()
    repo = SqlPipelineRepository(session)
    p = make_pipeline()
    result = await repo.save(p)
    assert result.id == "pipe-001"
    assert result.name == "ingest-e2e-asset"
    session.add.assert_called_once()


def test_to_domain_restores_source_objects_with_extraction_query() -> None:
    """_to_domain deve reconstruir source_objects incluindo extraction_query."""
    from app.domain.pipelines.load_strategy import LoadStrategy
    from app.infrastructure.persistence.models.pipeline_model import PipelineModel
    from app.infrastructure.persistence.repositories.sql_pipeline_repository import _to_domain

    model = PipelineModel(
        id="pipe-001",
        name="ingest_orders",
        type="ingestion",
        owner_email="eng@co.com",
        schema_version="1.0",
        source_asset_id="asset-001",
        destination_asset_id="",
        schedule={"mode": "cron", "cron_schedule": {"expression": "0 * * * *"}},
        source_objects=[
            {
                "object_id": "demo_orders",
                "load_strategy": "full_load",
                "watermark_column": None,
                "page_size": 1000,
                "partition_column": None,
                "compression": "snappy",
                "encoding": "utf-8",
                "extraction_query": "SELECT id FROM demo_orders",
                "sensor": None,
            }
        ],
        destination_objects=[],
        transform={"engine": "none", "ref": None},
        compute={
            "engine": "duckdb",
            "num_workers": 1,
            "machine_type": "n1-standard-2",
            "staging_bucket": "",
        },
        quality_rules=[],
        airflow={
            "retries": 3,
            "retry_delay_minutes": 5,
            "execution_timeout_minutes": 120,
            "sla_minutes": 90,
            "tags": [],
            "pool": "default_pool",
        },
        discovery_task={"enabled": True, "on_critical_change": "warn"},
    )

    pipeline = _to_domain(model)

    assert len(pipeline.source_objects) == 1
    obj = pipeline.source_objects[0]
    assert obj.object_id == "demo_orders"
    assert obj.extraction_query == "SELECT id FROM demo_orders"
    assert obj.load_strategy == LoadStrategy.FULL_LOAD
