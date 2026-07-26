from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class QualityGatePort(Protocol):
    """Port for evaluating data quality rules against compute metrics.

    Implementations live in app.infrastructure.quality_gate_evaluator.
    """

    def evaluate(self, metrics: dict[str, Any], rules: list[dict[str, Any]]) -> list[str]: ...
