from __future__ import annotations

from unittest.mock import MagicMock

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_confidence import PolicyTagConfidence
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.schema_drift_service import SchemaDriftService
from app.domain.shared.policy_tag import PolicyTag


def _make_snapshot(object_id: str, fields: list[SchemaField]) -> SchemaSnapshot:
    return SchemaSnapshot(object_id=object_id, fields=fields)


def _make_field(name: str, normalized_type: str = "string", nullable: bool = True) -> SchemaField:
    return SchemaField(
        name=name, source_type=normalized_type, normalized_type=normalized_type, nullable=nullable
    )


def test_compute_drifts_no_previous_snapshot_returns_object_added_event() -> None:
    """Sem snapshot anterior, retorna evento OBJECT_ADDED para o objeto."""
    differ = MagicMock()
    differ.diff.return_value = [
        DriftEvent(object_id="obj-1", change_type=DriftChangeType.OBJECT_ADDED, description="added")
    ]
    tag_inferrer = MagicMock()
    tag_inferrer.infer.return_value = None

    service = SchemaDriftService(schema_differ=differ, tag_inferrer=tag_inferrer)
    snap = _make_snapshot("obj-1", [_make_field("id")])
    events, suggestions = service.compute_drifts_and_tags(prev_snapshots={}, snapshots=[snap])

    assert len(events) == 1
    assert events[0].change_type == DriftChangeType.OBJECT_ADDED
    assert suggestions == []


def test_compute_drifts_infers_policy_tags_for_sensitive_fields() -> None:
    """Campos sensíveis devem gerar PolicyTagSuggestion."""
    differ = MagicMock()
    differ.diff.return_value = []
    suggestion = PolicyTagSuggestion(
        field_name="email",
        suggested_tag=PolicyTag.PII,
        confidence=PolicyTagConfidence.HIGH,
        matched_pattern="email",
    )
    tag_inferrer = MagicMock()
    tag_inferrer.infer.side_effect = lambda name: suggestion if name == "email" else None

    service = SchemaDriftService(schema_differ=differ, tag_inferrer=tag_inferrer)
    snap = _make_snapshot("obj-1", [_make_field("id"), _make_field("email")])
    events, suggestions = service.compute_drifts_and_tags(prev_snapshots={}, snapshots=[snap])

    assert len(suggestions) == 1
    assert suggestions[0].suggested_tag == PolicyTag.PII


def test_compute_drifts_uses_previous_snapshot_from_dict() -> None:
    """O snapshot anterior do dict deve ser passado ao SchemaDiffer."""
    differ = MagicMock()
    differ.diff.return_value = []
    tag_inferrer = MagicMock()
    tag_inferrer.infer.return_value = None

    service = SchemaDriftService(schema_differ=differ, tag_inferrer=tag_inferrer)
    prev_snap = _make_snapshot("obj-1", [_make_field("id")])
    curr_snap = _make_snapshot("obj-1", [_make_field("id"), _make_field("name")])

    service.compute_drifts_and_tags(
        prev_snapshots={"obj-1": prev_snap},
        snapshots=[curr_snap],
    )

    differ.diff.assert_called_once_with(prev_snap, curr_snap)


def test_compute_drifts_multiple_snapshots_aggregates_all_events() -> None:
    """Múltiplos snapshots têm seus eventos acumulados."""
    differ = MagicMock()
    event_a = DriftEvent(
        object_id="obj-a", change_type=DriftChangeType.FIELD_ADDED, description="a added"
    )
    event_b = DriftEvent(
        object_id="obj-b", change_type=DriftChangeType.FIELD_REMOVED, description="b removed"
    )
    differ.diff.side_effect = [[event_a], [event_b]]
    tag_inferrer = MagicMock()
    tag_inferrer.infer.return_value = None

    service = SchemaDriftService(schema_differ=differ, tag_inferrer=tag_inferrer)
    snaps = [
        _make_snapshot("obj-a", [_make_field("x")]),
        _make_snapshot("obj-b", [_make_field("y")]),
    ]
    events, _ = service.compute_drifts_and_tags(prev_snapshots={}, snapshots=snaps)

    assert len(events) == 2
