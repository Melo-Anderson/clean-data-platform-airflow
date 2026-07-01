from __future__ import annotations

import pytest

from app.domain.pipelines.dependency_type import DependencyType
from app.domain.pipelines.pipeline_dependency import PipelineDependency
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode


def test_cron_without_expression_raises() -> None:
    with pytest.raises(ValueError, match="requires cron_schedule"):
        ScheduleConfig(mode=ScheduleMode.CRON)


def test_trigger_without_depends_on_raises() -> None:
    with pytest.raises(ValueError, match="requires at least one depends_on"):
        ScheduleConfig(mode=ScheduleMode.TRIGGER)


def test_trigger_with_gate_without_cron_raises() -> None:
    dep = PipelineDependency(pipeline_id="uuid-1", require_same_day=True)
    with pytest.raises(ValueError, match="requires cron_schedule"):
        ScheduleConfig(mode=ScheduleMode.TRIGGER_WITH_GATE, depends_on=(dep,))


def test_dependency_type_defaults_to_dataset() -> None:
    dep = PipelineDependency(pipeline_id="uuid-1")
    assert dep.dependency_type == DependencyType.DATASET


def test_external_event_dependency_type_can_be_declared() -> None:
    dep = PipelineDependency(
        pipeline_id="uuid-2",
        dependency_type=DependencyType.EXTERNAL_EVENT,
        require_same_day=False,
    )
    assert dep.dependency_type == DependencyType.EXTERNAL_EVENT
