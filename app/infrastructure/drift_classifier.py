from __future__ import annotations

from typing import Any


class DriftClassifier:
    """Stub for DriftClassifier"""

    def classify(self, schema_snapshot: dict[str, Any], policy: str) -> dict[str, Any]:
        return {"can_proceed": True, "blocked_reason": ""}

    def classify_models(self, source_models: dict[str, Any]) -> dict[str, Any]:
        return {"can_proceed": True, "blocked_reason": ""}
