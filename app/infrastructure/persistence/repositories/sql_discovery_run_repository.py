from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.discovery.discovery_run import DiscoveryRun
from app.infrastructure.persistence.mappers.discovery_run_mapper import DiscoveryRunMapper
from app.infrastructure.persistence.models.discovery_run_model import DiscoveryRunModel


class SqlDiscoveryRunRepository:

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, run: DiscoveryRun) -> DiscoveryRun:
        # Check if exists
        m = await self._session.get(DiscoveryRunModel, run.id)
        if m is None:
            m = DiscoveryRunModel(id=run.id)
            self._session.add(m)
            
        m.asset_id = run.asset_id
        m.triggered_by = run.triggered_by
        m.status = run.status.value
        m.started_at = run.started_at
        m.completed_at = run.completed_at
        m.error_message = run.error_message
        m.objects_discovered = run.objects_discovered
        m.fields_discovered = run.fields_discovered
        m.soft_failures = run.soft_failures
        m.snapshots_json = [DiscoveryRunMapper.snapshot_to_dict(s) for s in run.snapshots]
        m.drift_events_json = [
            {
                "object_id": e.object_id,
                "change_type": e.change_type.value,
                "description": e.description,
                "field_name": e.field_name,
                "previous_value": e.previous_value,
                "current_value": e.current_value,
            }
            for e in run.drift_events
        ]
        m.policy_suggestions_json = [
            {
                "field_name": s.field_name,
                "suggested_tag": s.suggested_tag.value,
                "confidence": s.confidence.value,
                "matched_pattern": s.matched_pattern,
                "auto_generated_description": s.auto_generated_description,
            }
            for s in run.policy_tag_suggestions
        ]
        m.auto_descriptions_json = run.auto_generated_descriptions

        await self._session.flush()
        await self._session.refresh(m)
        return DiscoveryRunMapper.to_domain(m)

    async def find_by_id(self, run_id: str) -> DiscoveryRun | None:
        result = await self._session.execute(
            select(DiscoveryRunModel).where(DiscoveryRunModel.id == run_id)
        )
        m = result.scalar_one_or_none()
        return DiscoveryRunMapper.to_domain(m) if m else None

    async def find_latest_by_asset_id(self, asset_id: str) -> DiscoveryRun | None:
        result = await self._session.execute(
            select(DiscoveryRunModel)
            .where(DiscoveryRunModel.asset_id == asset_id)
            .where(DiscoveryRunModel.status.in_(["completed", "partial"]))
            .order_by(DiscoveryRunModel.completed_at.desc())
            .limit(1)
        )
        m = result.scalar_one_or_none()
        return DiscoveryRunMapper.to_domain(m) if m else None

    async def find_all_by_asset_id(
        self, asset_id: str, *, limit: int = 20
    ) -> list[DiscoveryRun]:
        result = await self._session.execute(
            select(DiscoveryRunModel)
            .where(DiscoveryRunModel.asset_id == asset_id)
            .order_by(DiscoveryRunModel.created_at.desc())
            .limit(limit)
        )
        return [DiscoveryRunMapper.to_domain(m) for m in result.scalars().all()]
