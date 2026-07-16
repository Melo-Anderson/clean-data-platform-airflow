from __future__ import annotations

from app.domain.discovery.drift_event import DriftEvent
from app.domain.discovery.policy_tag_suggestion import PolicyTagSuggestion
from app.domain.discovery.schema_snapshot import SchemaSnapshot
from app.domain.discovery.services.policy_tag_inferrer import PolicyTagInferrer
from app.domain.discovery.services.schema_differ import SchemaDiffer


class SchemaDriftService:
    """Pure domain service: computes schema drifts and infers policy tags.

    Has zero infrastructure dependencies. Accepts SchemaDiffer and
    PolicyTagInferrer via constructor injection for testability.
    """

    def __init__(self, schema_differ: SchemaDiffer, tag_inferrer: PolicyTagInferrer) -> None:
        self._schema_differ = schema_differ
        self._tag_inferrer = tag_inferrer

    def compute_drifts_and_tags(
        self,
        prev_snapshots: dict[str, SchemaSnapshot],
        snapshots: list[SchemaSnapshot],
    ) -> tuple[list[DriftEvent], list[PolicyTagSuggestion]]:
        """Compute all drift events and inferred policy tag suggestions.

        Args:
            prev_snapshots: Map of object_id -> previous SchemaSnapshot (may be empty).
            snapshots: Current snapshots returned by the discovery runner.

        Returns:
            A tuple of (drift_events, policy_tag_suggestions).
        """
        drift_events: list[DriftEvent] = []
        suggestions: list[PolicyTagSuggestion] = []

        for snap in snapshots:
            prev = prev_snapshots.get(snap.object_id)
            drift_events.extend(self._schema_differ.diff(prev, snap))
            for field in snap.fields:
                sug = self._tag_inferrer.infer(field.name)
                if sug:
                    suggestions.append(sug)

        return drift_events, suggestions
