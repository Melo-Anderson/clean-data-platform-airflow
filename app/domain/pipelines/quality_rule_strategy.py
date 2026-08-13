from typing import Any, Protocol


class QualityRuleStrategy(Protocol):
    def evaluate(self, rule: dict[str, Any], metrics: dict[str, Any]) -> str | None:
        """Evaluate a single rule against a set of metrics, returning an error message if violated."""
        ...
