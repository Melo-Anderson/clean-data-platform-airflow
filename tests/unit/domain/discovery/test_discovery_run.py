from __future__ import annotations

import uuid
import pytest

from app.domain.discovery.discovery_run import DiscoveryRun
from app.domain.discovery.discovery_run_status import DiscoveryRunStatus
from app.domain.discovery.drift_change_type import DriftChangeType
from app.domain.discovery.drift_event import DriftEvent


def _run(**kwargs) -> DiscoveryRun:
    return DiscoveryRun(id=str(uuid.uuid4()), asset_id="asset-1", triggered_by="manual", **kwargs)


def test_start_transitions_to_running() -> None:
    run = _run()
    run.start()
    assert run.status == DiscoveryRunStatus.RUNNING
    assert run.started_at is not None


def test_start_from_non_pending_raises() -> None:
    run = _run()
    run.start()
    with pytest.raises(ValueError, match="Cannot start"):
        run.start()


def test_complete_with_no_soft_failures_is_completed() -> None:
    run = _run()
    run.start()
    run.complete(
        snapshots=[], drift_events=[], policy_tag_suggestions={}, auto_generated_descriptions={}
    )
    assert run.status == DiscoveryRunStatus.COMPLETED
    assert run.completed_at is not None


def test_complete_with_soft_failures_is_partial() -> None:
    run = _run()
    run.start()
    run.complete(
        snapshots=[],
        drift_events=[],
        policy_tag_suggestions={},
        auto_generated_descriptions={},
        soft_failures=["PolicyTagInferrer: connection timeout"],
    )
    assert run.status == DiscoveryRunStatus.PARTIAL


def test_fail_records_error() -> None:
    run = _run()
    run.start()
    run.fail("Connection refused to endpoint")
    assert run.status == DiscoveryRunStatus.FAILED
    assert run.error_message == "Connection refused to endpoint"


def test_has_critical_drift_detects_correctly() -> None:
    run = _run()
    run.start()
    events = [
        DriftEvent(
            object_id="obj-1",
            change_type=DriftChangeType.TYPE_INCOMPATIBLE,
            description="STRING → INTEGER",
        ),
    ]
    run.complete(
        snapshots=[], drift_events=events, policy_tag_suggestions=[], auto_generated_descriptions={}
    )
    assert run.has_critical_drift is True
    assert len(run.critical_events) == 1
    assert len(run.informative_events) == 0


def test_duration_seconds_computed_correctly() -> None:
    run = _run()
    assert run.duration_seconds() is None
    run.start()
    run.fail("test")
    assert run.duration_seconds() is not None
