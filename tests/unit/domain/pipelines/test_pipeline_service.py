from __future__ import annotations

import uuid

import pytest

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_service import PipelineService
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.pipelines.sensor_config import SensorConfig
from app.domain.shared.value_objects import CronSchedule, EmailAddress


class FakePipelineRepository:
    def __init__(self) -> None:
        self._store: dict[str, Pipeline] = {}

    async def save(self, p: Pipeline) -> Pipeline:
        self._store[p.id] = p
        return p

    async def find_by_id(self, pid: str) -> Pipeline | None:
        return self._store.get(pid)

    async def find_all(self) -> list[Pipeline]:
        return list(self._store.values())

    async def update_schema_version(self, pid: str, sv: str) -> Pipeline:
        self._store[pid].schema_version = sv
        return self._store[pid]


def _pipeline(**kwargs) -> Pipeline:  # noqa: ANN003
    return Pipeline(
        id=kwargs.get("id", str(uuid.uuid4())),
        name=kwargs.get("name", "test"),
        type=kwargs.get("type", PipelineType.INGESTION),
        owner=kwargs.get("owner", EmailAddress("ae@co.com")),
        schedule=kwargs.get(
            "schedule",
            ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        ),
        source_objects=kwargs.get("source_objects", []),
        airflow=kwargs.get("airflow", AirflowConfig()),
    )


@pytest.mark.asyncio
async def test_pipeline_schedule_is_required() -> None:
    """Pipeline must be constructed with an explicit schedule - no default."""
    with pytest.raises(TypeError):
        Pipeline(id="x", name="x", type=PipelineType.INGESTION, owner=EmailAddress("ae@co.com"))  # type: ignore[call-arg]


@pytest.mark.asyncio
async def test_sensor_timeout_exceeding_execution_timeout_raises() -> None:
    service = PipelineService(repo=FakePipelineRepository())
    sensor = SensorConfig(query="SELECT 1", timeout_minutes=200)
    extraction = ExtractionConfig(
        object_id="obj-1", load_strategy=LoadStrategy.INCREMENTAL, sensor=sensor
    )
    pipeline = _pipeline(
        source_objects=[extraction], airflow=AirflowConfig(execution_timeout_minutes=120)
    )
    with pytest.raises(ValueError, match="sensor.*timeout"):
        await service.register(pipeline)


@pytest.mark.asyncio
async def test_dataset_uri_follows_convention() -> None:
    service = PipelineService(repo=FakePipelineRepository())
    p = _pipeline()
    saved = await service.register(p)
    assert saved.dataset_uri == f"platform://pipeline/{saved.id}"
