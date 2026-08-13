from __future__ import annotations

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.schema_field import SchemaField
from app.domain.discovery.schema_snapshot import SchemaSnapshot


def _check_added_and_changed_fields(
    previous: SchemaSnapshot, current: SchemaSnapshot
) -> list[DriftEvent]:
    events: list[DriftEvent] = []
    prev_fields = {f.name: f for f in previous.fields}
    curr_fields = {f.name: f for f in current.fields}

    for name, curr_f in curr_fields.items():
        prev_f = prev_fields.get(name)
        if prev_f is None:
            events.append(
                DriftEvent(
                    object_id=current.object_id,
                    field_name=name,
                    change_type=DriftChangeType.FIELD_ADDED,
                    description=f"Field {name} added.",
                    current_value=curr_f.normalized_type,
                )
            )
            continue
        events.extend(_check_type_change(current.object_id, prev_f, curr_f))
        events.extend(_check_nullability_change(current.object_id, prev_f, curr_f))
    return events


def _check_type_change(
    object_id: str, prev_f: SchemaField, curr_f: SchemaField
) -> list[DriftEvent]:
    if curr_f.normalized_type == prev_f.normalized_type:
        return []
    if curr_f.is_compatible_with(prev_f):
        return [
            DriftEvent(
                object_id=object_id,
                field_name=curr_f.name,
                change_type=DriftChangeType.TYPE_WIDENED,
                description=f"Type widened from {prev_f.normalized_type} to {curr_f.normalized_type}.",
                previous_value=prev_f.normalized_type,
                current_value=curr_f.normalized_type,
            )
        ]
    return [
        DriftEvent(
            object_id=object_id,
            field_name=curr_f.name,
            change_type=DriftChangeType.TYPE_INCOMPATIBLE,
            description=f"Incompatible type change from {prev_f.normalized_type} to {curr_f.normalized_type}.",
            previous_value=prev_f.normalized_type,
            current_value=curr_f.normalized_type,
        )
    ]


def _check_nullability_change(
    object_id: str, prev_f: SchemaField, curr_f: SchemaField
) -> list[DriftEvent]:
    if prev_f.nullable and not curr_f.nullable:
        return [
            DriftEvent(
                object_id=object_id,
                field_name=curr_f.name,
                change_type=DriftChangeType.NULLABLE_TO_REQUIRED,
                description=f"Field {curr_f.name} changed from nullable to required.",
            )
        ]
    if not prev_f.nullable and curr_f.nullable:
        return [
            DriftEvent(
                object_id=object_id,
                field_name=curr_f.name,
                change_type=DriftChangeType.REQUIRED_TO_NULLABLE,
                description=f"Field {curr_f.name} changed from required to nullable.",
            )
        ]
    return []


def _check_removed_fields(previous: SchemaSnapshot, current: SchemaSnapshot) -> list[DriftEvent]:
    events: list[DriftEvent] = []
    prev_fields = {f.name: f for f in previous.fields}
    curr_fields = {f.name: f for f in current.fields}
    for name, prev_f in prev_fields.items():
        if name not in curr_fields:
            events.append(
                DriftEvent(
                    object_id=previous.object_id,
                    field_name=name,
                    change_type=DriftChangeType.FIELD_REMOVED,
                    description=f"Field {name} removed.",
                    previous_value=prev_f.normalized_type,
                )
            )
    return events


class SchemaDiffer:
    def diff(
        self, previous: SchemaSnapshot | None, current: SchemaSnapshot | None
    ) -> list[DriftEvent]:
        if previous is None and current is not None:
            return [
                DriftEvent(
                    object_id=current.object_id,
                    change_type=DriftChangeType.OBJECT_ADDED,
                    description=f"Object {current.object_id} added to source.",
                )
            ]
        if current is None and previous is not None:
            return [
                DriftEvent(
                    object_id=previous.object_id,
                    change_type=DriftChangeType.OBJECT_REMOVED,
                    description=f"Object {previous.object_id} removed from source.",
                )
            ]
        if previous is None or current is None:
            return []

        return _check_added_and_changed_fields(previous, current) + _check_removed_fields(
            previous, current
        )
