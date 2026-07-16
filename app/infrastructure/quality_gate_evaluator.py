from __future__ import annotations

import math
from typing import Any

_SKIPPED_METRIC_SENTINEL = object()


class QualityGateEvaluator:
    """
    Evaluates pipeline quality rules against compute metrics.

    Metric key convention (emitted by compute engine to metrics.json):
      row_count_min  → metrics["row_count"]
      not_null       → metrics["null_count_{column}"]
      unique         → metrics["duplicate_count_{column}"]
      accepted_values→ metrics["invalid_value_count_{column}"]
      referential_integrity → metrics["orphan_count_{column}"]
      checksum       → metrics["checksum"]

    If a metric key is missing, the rule FAILS with a violation (no silent skip).
    """

    def evaluate(self, metrics: dict[str, Any], rules: list[dict[str, Any]]) -> list[str]:
        violations: list[str] = []
        for rule in rules:
            rule_type = rule.get("type", "")
            violation = self._evaluate_rule(rule_type, rule, metrics)
            if violation:
                violations.append(violation)
        return violations

    def _evaluate_rule(self, rule_type: str, rule: dict, metrics: dict) -> str | None:
        if rule_type == "row_count_min":
            actual = metrics.get("row_count", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            if isinstance(actual, float) and math.isnan(actual):
                return f"VIOLATION row_count_min: got NaN, expected >= {rule.get('value', 0)}"
            threshold = rule.get("value", 0)
            if actual < threshold:
                return f"VIOLATION row_count_min: got {actual}, expected >= {threshold}"

        elif rule_type == "not_null":
            col = rule.get("column", "")
            actual = metrics.get(f"null_count_{col}", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            if actual > 0:
                return f"VIOLATION not_null: column '{col}' has {actual} null(s)"

        elif rule_type == "unique":
            col = rule.get("column", "")
            actual = metrics.get(f"duplicate_count_{col}", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            if actual > 0:
                return f"VIOLATION unique: column '{col}' has {actual} duplicate(s)"

        elif rule_type == "accepted_values":
            col = rule.get("column", "")
            actual = metrics.get(f"invalid_value_count_{col}", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            if actual > 0:
                return f"VIOLATION accepted_values: column '{col}' has {actual} invalid value(s)"

        elif rule_type == "referential_integrity":
            col = rule.get("column", "")
            actual = metrics.get(f"orphan_count_{col}", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            if actual > 0:
                return (
                    f"VIOLATION referential_integrity: column '{col}' has {actual} orphan record(s)"
                )

        elif rule_type == "checksum":
            actual = metrics.get("checksum", _SKIPPED_METRIC_SENTINEL)
            if actual is _SKIPPED_METRIC_SENTINEL:
                return f"VIOLATION {rule_type}: metric not computed/missing"
            expected = rule.get("value", "")
            if str(actual) != str(expected):
                return f"VIOLATION checksum: got {actual}, expected {expected}"

        return None
