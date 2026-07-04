from __future__ import annotations

from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.schema_snapshot import SchemaSnapshot


class SchemaDiffer:
    """
    Service to compute schema drift between two snapshots.
    """

    def diff(
        self, previous: SchemaSnapshot | None, current: SchemaSnapshot | None
    ) -> list[DriftEvent]:
        events: list[DriftEvent] = []

        if previous is None and current is not None:
            events.append(
                DriftEvent(
                    object_id=current.object_id,
                    change_type=DriftChangeType.OBJECT_ADDED,
                    description=f"Object {current.object_id} added to source.",
                )
            )
            return events

        if current is None and previous is not None:
            events.append(
                DriftEvent(
                    object_id=previous.object_id,
                    change_type=DriftChangeType.OBJECT_REMOVED,
                    description=f"Object {previous.object_id} removed from source.",
                )
            )
            return events

        if previous is None or current is None:
            return events

        prev_fields = {f.name: f for f in previous.fields}
        curr_fields = {f.name: f for f in current.fields}

        # Check for added fields and type/nullability changes
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

            if curr_f.normalized_type != prev_f.normalized_type:
                if curr_f.is_compatible_with(prev_f):
                    events.append(
                        DriftEvent(
                            object_id=current.object_id,
                            field_name=name,
                            change_type=DriftChangeType.TYPE_WIDENED,
                            description=f"Type widened from {prev_f.normalized_type} to {curr_f.normalized_type}.",
                            previous_value=prev_f.normalized_type,
                            current_value=curr_f.normalized_type,
                        )
                    )
                else:
                    events.append(
                        DriftEvent(
                            object_id=current.object_id,
                            field_name=name,
                            change_type=DriftChangeType.TYPE_INCOMPATIBLE,
                            description=f"Incompatible type change from {prev_f.normalized_type} to {curr_f.normalized_type}.",
                            previous_value=prev_f.normalized_type,
                            current_value=curr_f.normalized_type,
                        )
                    )

            if prev_f.nullable and not curr_f.nullable:
                events.append(
                    DriftEvent(
                        object_id=current.object_id,
                        field_name=name,
                        change_type=DriftChangeType.NULLABLE_TO_REQUIRED,
                        description=f"Field {name} changed from nullable to required.",
                    )
                )
            elif not prev_f.nullable and curr_f.nullable:
                events.append(
                    DriftEvent(
                        object_id=current.object_id,
                        field_name=name,
                        change_type=DriftChangeType.REQUIRED_TO_NULLABLE,
                        description=f"Field {name} changed from required to nullable.",
                    )
                )

        # Check for removed fields
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
