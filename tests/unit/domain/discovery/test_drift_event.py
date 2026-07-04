from __future__ import annotations

import pytest

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.drift_severity import DriftSeverity
from app.domain.discovery.schema_field import SchemaField


@pytest.mark.parametrize("change_type,expected_severity", [
    (DriftChangeType.FIELD_ADDED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.FIELD_REMOVED, DriftSeverity.CRITICAL),
    (DriftChangeType.TYPE_WIDENED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.TYPE_INCOMPATIBLE, DriftSeverity.CRITICAL),
    (DriftChangeType.NULLABLE_TO_REQUIRED, DriftSeverity.CRITICAL),
    (DriftChangeType.REQUIRED_TO_NULLABLE, DriftSeverity.INFORMATIVE),
    (DriftChangeType.OBJECT_ADDED, DriftSeverity.INFORMATIVE),
    (DriftChangeType.OBJECT_REMOVED, DriftSeverity.CRITICAL),
])
def test_drift_event_severity_is_deterministic(
    change_type: DriftChangeType,
    expected_severity: DriftSeverity,
) -> None:
    event = DriftEvent(object_id="obj-1", change_type=change_type, description="test")
    assert event.severity == expected_severity


def test_critical_event_requires_approval() -> None:
    event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.TYPE_INCOMPATIBLE,
        description="STRING → INTEGER",
    )
    assert event.is_critical is True
    assert event.requires_approval is True


def test_informative_event_does_not_require_approval() -> None:
    event = DriftEvent(
        object_id="obj-1",
        change_type=DriftChangeType.FIELD_ADDED,
        description="new field added",
    )
    assert event.is_critical is False
    assert event.requires_approval is False


def test_schema_field_compatible_with_same_type() -> None:
    f1 = SchemaField(name="id", source_type="INT", normalized_type="integer")
    f2 = SchemaField(name="id", source_type="INT", normalized_type="integer")
    assert f2.is_compatible_with(f1) is True


def test_schema_field_widening_is_compatible() -> None:
    f_old = SchemaField(name="id", source_type="INT", normalized_type="integer")
    f_new = SchemaField(name="id", source_type="BIGINT", normalized_type="bigint")
    assert f_new.is_compatible_with(f_old) is True


def test_schema_field_incompatible_type_change() -> None:
    f_old = SchemaField(name="code", source_type="VARCHAR", normalized_type="string")
    f_new = SchemaField(name="code", source_type="INT", normalized_type="integer")
    assert f_new.is_compatible_with(f_old) is False


def test_schema_field_complex_types_fallback() -> None:
    # They are not in widening map, so they are only compatible if exactly the same.
    f_struct1 = SchemaField(name="meta", source_type="STRUCT", normalized_type="struct")
    f_struct2 = SchemaField(name="meta", source_type="STRUCT", normalized_type="struct")
    assert f_struct2.is_compatible_with(f_struct1) is True
    
    f_array = SchemaField(name="meta", source_type="ARRAY", normalized_type="array")
    assert f_array.is_compatible_with(f_struct1) is False
