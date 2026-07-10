from __future__ import annotations

from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.shared.policy_tag import PolicyTag
from app.infrastructure.persistence.models.discovery_run_model import DiscoveryRunModel


class DiscoveryRunMapper:
    """
    Handles mapping between the DiscoveryRun domain aggregate and its ORM model/JSON representation.
    """

    @staticmethod
    def snapshot_from_dict(d: dict) -> SchemaSnapshot:
        from datetime import datetime

        return SchemaSnapshot(
            object_id=d["object_id"],
            object_name=d.get("object_name", ""),
            runner_type=d.get("runner_type", ""),
            captured_at=datetime.fromisoformat(d["captured_at"]),
            row_count_estimate=d.get("row_count_estimate"),
            fields=[
                SchemaField(
                    name=f["name"],
                    source_type=f["source_type"],
                    normalized_type=f["normalized_type"],
                    nullable=f.get("nullable", True),
                    is_primary_key=f.get("is_primary_key", False),
                    description=f.get("description"),
                    extra=f.get("extra", {}),
                )
                for f in d.get("fields", [])
            ],
        )

    @staticmethod
    def snapshot_to_dict(s: SchemaSnapshot) -> dict:
        return {
            "object_id": s.object_id,
            "object_name": s.object_name,
            "runner_type": s.runner_type,
            "captured_at": s.captured_at.isoformat(),
            "row_count_estimate": s.row_count_estimate,
            "fields": [
                {
                    "name": f.name,
                    "source_type": f.source_type,
                    "normalized_type": f.normalized_type,
                    "nullable": f.nullable,
                    "is_primary_key": f.is_primary_key,
                    "description": f.description,
                    "extra": f.extra,
                }
                for f in s.fields
            ],
        }

    @staticmethod
    def to_domain(m: DiscoveryRunModel) -> DiscoveryRun:
        run = DiscoveryRun(
            id=m.id,
            asset_id=m.asset_id,
            triggered_by=m.triggered_by,
            status=DiscoveryRunStatus(m.status),
            started_at=m.started_at,
            completed_at=m.completed_at,
            error_message=m.error_message,
            objects_discovered=m.objects_discovered,
            fields_discovered=m.fields_discovered,
            soft_failures=m.soft_failures,
            created_at=m.created_at,
            updated_at=m.updated_at,
        )
        run.snapshots = [DiscoveryRunMapper.snapshot_from_dict(d) for d in (m.snapshots_json or [])]
        run.drift_events = [
            DriftEvent(
                object_id=e["object_id"],
                change_type=DriftChangeType(e["change_type"]),
                description=e["description"],
                field_name=e.get("field_name"),
                previous_value=e.get("previous_value"),
                current_value=e.get("current_value"),
            )
            for e in (m.drift_events_json or [])
        ]
        run.policy_tag_suggestions = [
            PolicyTagSuggestion(
                field_name=s["field_name"],
                suggested_tag=PolicyTag(s["suggested_tag"]),
                confidence=PolicyTagConfidence(s["confidence"]),
                matched_pattern=s["matched_pattern"],
                auto_generated_description=s.get("auto_generated_description"),
            )
            for s in (m.policy_suggestions_json or [])
        ]
        run.auto_generated_descriptions = m.auto_descriptions_json or {}
        return run
