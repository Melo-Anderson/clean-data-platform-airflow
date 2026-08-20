from __future__ import annotations

import math
from typing import Any

from app.domain.pipelines.quality_rule_strategy import QualityRuleStrategy

_SKIPPED_METRIC_SENTINEL = object()


class RowCountMinStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        actual = metrics.get("row_count", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION row_count_min: metric not computed/missing"
        if isinstance(actual, float) and math.isnan(actual):
            return f"VIOLATION row_count_min: got NaN, expected >= {rule.get('value', 0)}"
        threshold = rule.get("value", 0)
        if actual < threshold:
            return f"VIOLATION row_count_min: got {actual}, expected >= {threshold}"
        return None


class NotNullStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        col = rule.get("column", "")
        actual = metrics.get(f"null_count_{col}", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION not_null: metric not computed/missing"
        if actual > 0:
            return f"VIOLATION not_null: column '{col}' has {actual} null(s)"
        return None


class UniqueStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        col = rule.get("column", "")
        actual = metrics.get(f"duplicate_count_{col}", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION unique: metric not computed/missing"
        if actual > 0:
            return f"VIOLATION unique: column '{col}' has {actual} duplicate(s)"
        return None


class AcceptedValuesStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        col = rule.get("column", "")
        actual = metrics.get(f"invalid_value_count_{col}", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION accepted_values: metric not computed/missing"
        if actual > 0:
            return f"VIOLATION accepted_values: column '{col}' has {actual} invalid value(s)"
        return None


class ReferentialIntegrityStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        col = rule.get("column", "")
        actual = metrics.get(f"orphan_count_{col}", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION referential_integrity: metric not computed/missing"
        if actual > 0:
            return f"VIOLATION referential_integrity: column '{col}' has {actual} orphan record(s)"
        return None


class ChecksumStrategy:
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        actual = metrics.get("checksum", _SKIPPED_METRIC_SENTINEL)
        if actual is _SKIPPED_METRIC_SENTINEL:
            return "VIOLATION checksum: metric not computed/missing"
        expected = rule.get("value", "")
        if str(actual) != str(expected):
            return f"VIOLATION checksum: got {actual}, expected {expected}"
        return None


_DEFAULT_STRATEGIES: dict[str, QualityRuleStrategy] = {
    "row_count_min": RowCountMinStrategy(),
    "not_null": NotNullStrategy(),
    "unique": UniqueStrategy(),
    "accepted_values": AcceptedValuesStrategy(),
    "referential_integrity": ReferentialIntegrityStrategy(),
    "checksum": ChecksumStrategy(),
}


class QualityGateEvaluator:
    """
    Evaluates pipeline quality rules against compute metrics.
    Pure domain logic residing in domain layer.
    """

    def __init__(self, strategies: dict[str, QualityRuleStrategy] | None = None) -> None:
        self._strategies = strategies or _DEFAULT_STRATEGIES

    def evaluate(self, metrics: dict[str, Any], rules: list[dict[str, Any]]) -> list[str]:
        violations: list[str] = []
        for rule in rules:
            rule_type = rule.get("type", "")
            strategy = self._strategies.get(rule_type)
            if strategy:
                violation = strategy.evaluate(rule, metrics)
                if violation:
                    violations.append(violation)
        return violations
