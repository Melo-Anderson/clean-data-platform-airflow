from __future__ import annotations

from dataclasses import dataclass

from app.domain.pipelines.quality_rule_type import QualityRuleType


@dataclass(frozen=True)
class QualityRule:
    """
    Quality assertion applied by quality_gate after read_compute_metrics.

    quality_gate confronts configured rules with metrics produced by the compute engine
    (written to metrics.json). Failure marks task as quality_failed, blocking downstream.
    """

    type: QualityRuleType
    column: str | None = None
    value: int | float | None = None
