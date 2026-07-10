import pytest

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_differ import SchemaDiffer
from datetime import datetime, timezone


@pytest.fixture
def differ() -> SchemaDiffer:
    return SchemaDiffer()


def test_schema_differ_no_changes(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[
            SchemaField(
                name="id", source_type="INTEGER", normalized_type="integer", is_primary_key=True
            ),
            SchemaField(
                name="name", source_type="VARCHAR", normalized_type="string", nullable=True
            ),
        ],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[
            SchemaField(
                name="id", source_type="INTEGER", normalized_type="integer", is_primary_key=True
            ),
            SchemaField(
                name="name", source_type="VARCHAR", normalized_type="string", nullable=True
            ),
        ],
    )

    events = differ.diff(prev, current)
    assert len(events) == 0


def test_schema_differ_field_added(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="new_field", source_type="INT", normalized_type="integer")],
    )

    events = differ.diff(prev, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.FIELD_ADDED
    assert events[0].field_name == "new_field"
    assert not events[0].is_critical


def test_schema_differ_field_removed(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="old_field", source_type="INT", normalized_type="integer")],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[],
    )

    events = differ.diff(prev, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.FIELD_REMOVED
    assert events[0].field_name == "old_field"
    assert events[0].is_critical


def test_schema_differ_type_widening(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="amount", source_type="INT", normalized_type="integer")],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="amount", source_type="FLOAT", normalized_type="float")],
    )

    events = differ.diff(prev, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.TYPE_WIDENED
    assert events[0].field_name == "amount"
    assert not events[0].is_critical


def test_schema_differ_type_incompatible(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="amount", source_type="FLOAT", normalized_type="float")],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="amount", source_type="INT", normalized_type="integer")],
    )

    events = differ.diff(prev, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.TYPE_INCOMPATIBLE
    assert events[0].field_name == "amount"
    assert events[0].is_critical


def test_schema_differ_nullable_changed(differ: SchemaDiffer):
    prev = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[
            SchemaField(name="name", source_type="VARCHAR", normalized_type="string", nullable=True)
        ],
    )
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[
            SchemaField(
                name="name", source_type="VARCHAR", normalized_type="string", nullable=False
            )
        ],
    )

    events = differ.diff(prev, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.NULLABLE_TO_REQUIRED
    assert events[0].field_name == "name"
    assert events[0].is_critical


def test_schema_differ_first_run(differ: SchemaDiffer):
    current = SchemaSnapshot(
        object_id="asset123",
        captured_at=datetime.now(timezone.utc),
        fields=[SchemaField(name="id", source_type="INT", normalized_type="integer")],
    )

    events = differ.diff(None, current)
    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.OBJECT_ADDED
    assert not events[0].is_critical
