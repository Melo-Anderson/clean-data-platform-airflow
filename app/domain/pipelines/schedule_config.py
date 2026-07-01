from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.pipelines.pipeline_dependency import PipelineDependency
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.shared.value_objects import CronSchedule


@dataclass(frozen=True)
class ScheduleConfig:
    """
    Scheduling configuration for a Pipeline.

    Always required - no default in Pipeline. Caller must explicitly declare schedule intent.

    mode=cron: cron_schedule required. No depends_on.
    mode=trigger: depends_on required (DATASET type). cron_schedule ignored.
    mode=trigger_with_gate: both cron_schedule and depends_on required.
      Airflow 3: schedule=[Asset(...)], plus ShortCircuitOperator enforcing require_same_day.

    Only DependencyType.DATASET is implemented in this plan version.
    EXTERNAL_EVENT and MANUAL are reserved for future plans.
    """

    mode: ScheduleMode
    cron_schedule: CronSchedule | None = None
    depends_on: tuple[PipelineDependency, ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.mode == ScheduleMode.CRON and self.cron_schedule is None:
            raise ValueError("ScheduleConfig(mode='cron') requires cron_schedule")
        if self.mode == ScheduleMode.TRIGGER and not self.depends_on:
            raise ValueError(
                "ScheduleConfig(mode='trigger') requires at least one depends_on entry"
            )
        if self.mode == ScheduleMode.TRIGGER_WITH_GATE:
            if self.cron_schedule is None:
                raise ValueError("ScheduleConfig(mode='trigger_with_gate') requires cron_schedule")
            if not self.depends_on:
                raise ValueError(
                    "ScheduleConfig(mode='trigger_with_gate') requires at least one depends_on entry"
                )
