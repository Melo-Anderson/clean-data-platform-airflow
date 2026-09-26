# tests/unit/fakes.py
from __future__ import annotations

import uuid
from typing import Any, Self

from app.domain.assets.data_asset import DataAsset
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig, ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress


def build_fake_pipeline(
    name: str = "test-pipeline",
    pipeline_type: PipelineType | str = PipelineType.INGESTION,
    owner: str = "test@example.com",
    id: str | None = None,
    source_asset_name: str = "src-asset",
    destination_asset_name: str = "dst-asset",
    cron_schedule: str | None = None,
) -> Pipeline:
    p_type = (
        pipeline_type if isinstance(pipeline_type, PipelineType) else PipelineType(pipeline_type)
    )
    cron = cron_schedule or "0 0 * * *"
    sched = ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule(cron))

    return Pipeline(
        id=id or str(uuid.uuid4()),
        name=name,
        type=p_type,
        owner=EmailAddress(owner),
        schedule=sched,
        source_asset_name=source_asset_name,
        destination_asset_name=destination_asset_name,
    )


class FakePipelineRepo:
    def __init__(self) -> None:
        self._store: dict[str, Pipeline] = {}

    async def save(self, pipeline: Pipeline) -> Pipeline:
        self._store[pipeline.id] = pipeline
        return pipeline

    async def find_by_id(self, pipeline_id: str) -> Pipeline | None:
        return self._store.get(pipeline_id)

    async def find_all(self) -> list[Pipeline]:
        return list(self._store.values())

    async def find_by_name(self, name: str) -> Pipeline | None:
        return next((p for p in self._store.values() if p.name == name), None)


class FakePipelineRunRepo:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def save(self, run: Any) -> Any:
        self._store[run.id] = run
        return run

    async def find_by_id(self, run_id: str) -> Any | None:
        return self._store.get(run_id)

    async def find_by_pipeline_id(self, pipeline_id: str) -> list[Any]:
        return [r for r in self._store.values() if getattr(r, "pipeline_id", None) == pipeline_id]


class FakeAssetRepo:
    def __init__(self) -> None:
        self._store: dict[str, DataAsset] = {}

    async def save(self, asset: DataAsset) -> DataAsset:
        self._store[asset.id] = asset
        return asset

    async def find_by_id(self, asset_id: str) -> DataAsset | None:
        return self._store.get(asset_id)

    async def find_by_name(self, name: str) -> DataAsset | None:
        return next((a for a in self._store.values() if a.name == name), None)


class FakeEndpointRepo:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def save(self, endpoint: Any) -> Any:
        self._store[endpoint.id] = endpoint
        return endpoint

    async def find_by_id(self, endpoint_id: str) -> Any | None:
        return self._store.get(endpoint_id)

    async def find_by_name(self, name: str) -> Any | None:
        return next((e for e in self._store.values() if getattr(e, "name", None) == name), None)


class FakeDataObjectRepo:
    def __init__(self) -> None:
        self._store: dict[str, Any] = {}

    async def save(self, obj: Any) -> Any:
        self._store[obj.id] = obj
        return obj

    async def find_by_asset_id(self, asset_id: str) -> list[Any]:
        return [o for o in self._store.values() if getattr(o, "asset_id", None) == asset_id]


class FakeUnitOfWork:
    def __init__(self) -> None:
        self.pipelines = FakePipelineRepo()
        self.pipeline_runs = FakePipelineRunRepo()
        self.assets = FakeAssetRepo()
        self.endpoints = FakeEndpointRepo()
        self.objects = FakeDataObjectRepo()
        self.lineage = None
        self.discovery_runs = None
        self.drift_approvals = None
        self.audit_logs = None
        self.committed = False
        self.rolled_back = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        if exc_type is not None:
            self.rolled_back = True

    async def commit(self) -> None:
        self.committed = True

    async def rollback(self) -> None:
        self.rolled_back = True
