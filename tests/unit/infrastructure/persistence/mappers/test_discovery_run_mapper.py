from datetime import UTC, datetime

from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.infrastructure.persistence.mappers.discovery_run_mapper import DiscoveryRunMapper
from app.infrastructure.persistence.models.discovery_run_model import DiscoveryRunModel


def test_snapshot_from_dict() -> None:
    d = {
        "object_id": "obj1",
        "object_name": "table1",
        "runner_type": "database",
        "captured_at": "2026-07-02T12:00:00+00:00",
        "row_count_estimate": 1000,
        "fields": [
            {
                "name": "id",
                "source_type": "INT",
                "normalized_type": "integer",
                "nullable": False,
                "is_primary_key": True,
                "description": "Primary key",
                "extra": {"length": 4},
            }
        ],
    }
    snapshot = DiscoveryRunMapper.snapshot_from_dict(d)

    assert snapshot.object_id == "obj1"
    assert snapshot.object_name == "table1"
    assert snapshot.captured_at.year == 2026
    assert snapshot.row_count_estimate == 1000
    assert len(snapshot.fields) == 1
    assert snapshot.fields[0].name == "id"
    assert snapshot.fields[0].normalized_type == "integer"
    assert not snapshot.fields[0].nullable
    assert snapshot.fields[0].is_primary_key


def test_snapshot_to_dict() -> None:
    snapshot = SchemaSnapshot(
        object_id="obj1",
        object_name="table1",
        runner_type="database",
        captured_at=datetime(2026, 7, 2, 12, 0, 0, tzinfo=UTC),
        row_count_estimate=1000,
        fields=[
            SchemaField(
                name="id",
                source_type="INT",
                normalized_type="integer",
                nullable=False,
                is_primary_key=True,
                description="Primary key",
                extra={"length": 4},
            )
        ],
    )

    d = DiscoveryRunMapper.snapshot_to_dict(snapshot)

    assert d["object_id"] == "obj1"
    assert d["object_name"] == "table1"
    assert "2026-07-02T12:00:00+00:00" in d["captured_at"]
    assert d["row_count_estimate"] == 1000
    assert len(d["fields"]) == 1
    assert d["fields"][0]["name"] == "id"
    assert d["fields"][0]["normalized_type"] == "integer"
    assert not d["fields"][0]["nullable"]
    assert d["fields"][0]["is_primary_key"]


def test_to_domain() -> None:
    m = DiscoveryRunModel(
        id="run1",
        asset_id="asset1",
        triggered_by="user1",
        status=DiscoveryRunStatus.COMPLETED.value,
        started_at=datetime.now(UTC),
        completed_at=datetime.now(UTC),
        snapshots_json=[
            {
                "object_id": "obj1",
                "object_name": "table1",
                "runner_type": "database",
                "captured_at": "2026-07-02T12:00:00+00:00",
                "row_count_estimate": 1000,
                "fields": [],
            }
        ],
        drift_events_json=[
            {
                "object_id": "obj1",
                "change_type": "field_added",
                "description": "Added field",
                "field_name": "new_field",
            }
        ],
        policy_suggestions_json=[],
        auto_descriptions_json={},
    )

    run = DiscoveryRunMapper.to_domain(m)

    assert run.id == "run1"
    assert run.asset_id == "asset1"
    assert run.status == DiscoveryRunStatus.COMPLETED
    assert len(run.snapshots) == 1
    assert run.snapshots[0].object_id == "obj1"
    assert len(run.drift_events) == 1
    assert run.drift_events[0].change_type.value == "field_added"
    assert run.drift_events[0].field_name == "new_field"
