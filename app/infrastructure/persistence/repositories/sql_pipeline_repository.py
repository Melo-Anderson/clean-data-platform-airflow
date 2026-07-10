from __future__ import annotations

import dataclasses
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.persistence.models.pipeline_model import PipelineModel


def _to_model(p: Pipeline) -> PipelineModel:
    return PipelineModel(
        id=p.id,
        name=p.name,
        type=p.type.value,
        owner_email=p.owner.value,
        schema_version=p.schema_version,
        source_asset_id=p.source_asset_id,
        destination_asset_id=p.destination_asset_id,
        schedule=dataclasses.asdict(p.schedule),
        source_objects=[dataclasses.asdict(o) for o in p.source_objects],
        destination_objects=[dataclasses.asdict(o) for o in p.destination_objects],
        transform=dataclasses.asdict(p.transform),
        compute=dataclasses.asdict(p.compute),
        quality_rules=[dataclasses.asdict(r) for r in p.quality_rules],
        airflow=dataclasses.asdict(p.airflow),
        discovery_task=dataclasses.asdict(p.discovery_task),
    )


def _to_domain(m: PipelineModel) -> Pipeline:
    sched_dict = m.schedule
    mode = ScheduleMode(sched_dict["mode"])
    cron_dict = sched_dict.get("cron_schedule")
    cron_expr = cron_dict.get("expression") if isinstance(cron_dict, dict) else cron_dict
    schedule = ScheduleConfig(
        mode=mode,
        cron_schedule=CronSchedule(cron_expr) if cron_expr else None,
    )
    return Pipeline(
        id=m.id,
        name=m.name,
        type=PipelineType(m.type),
        owner=EmailAddress(m.owner_email),
        schema_version=m.schema_version,
        source_asset_id=m.source_asset_id,
        destination_asset_id=m.destination_asset_id,
        schedule=schedule,
        created_at=m.created_at,
        updated_at=m.updated_at,
    )


class SqlPipelineRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, p: Pipeline) -> Pipeline:
        model = _to_model(p)
        self._session.add(model)
        await self._session.flush()
        return p

    async def find_by_id(self, pid: str) -> Pipeline | None:
        result = await self._session.execute(select(PipelineModel).where(PipelineModel.id == pid))
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_by_name(self, name: str) -> Pipeline | None:
        result = await self._session.execute(
            select(PipelineModel).where(PipelineModel.name == name)
        )
        m = result.scalar_one_or_none()
        return _to_domain(m) if m else None

    async def find_all(self) -> list[Pipeline]:
        result = await self._session.execute(select(PipelineModel))
        return [_to_domain(m) for m in result.scalars().all()]

    async def update_schema_version(self, pid: str, sv: str) -> Pipeline:
        p = await self.find_by_id(pid)
        if p is None:
            raise ValueError(f"Pipeline not found: {pid}")
        result = await self._session.execute(select(PipelineModel).where(PipelineModel.id == pid))
        m = result.scalar_one()
        m.schema_version = sv
        await self._session.flush()
        return _to_domain(m)
