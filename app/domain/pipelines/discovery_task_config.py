from __future__ import annotations

from dataclasses import dataclass

from app.domain.pipelines.on_critical_change import OnCriticalChange


@dataclass(frozen=True)
class DiscoveryTaskConfig:
    """
    Controls validate_source_and_discovery + classify_changes_and_plan_actions tasks.

    on_critical_change applies to informative changes only.
    CRITICAL changes (nullable->required, incompatible type, table removed) always block.
    """

    enabled: bool = True
    on_critical_change: OnCriticalChange = OnCriticalChange.BLOCK
