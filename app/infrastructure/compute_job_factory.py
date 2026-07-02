from __future__ import annotations

from typing import Any

from app.infrastructure.airflow_callbacks.compute_job_adapter import ComputeJobAdapter


class DummyComputeAdapter:
    def submit_job(self, pipeline_id: str, pipeline_type: str, config: dict[str, Any]) -> str:
        return "dummy-job-123"

    def poll_job_status(self, job_id: str) -> Any:
        pass

    def cancel_job(self, job_id: str) -> None:
        pass


def get_compute_adapter(engine: str) -> ComputeJobAdapter:
    return DummyComputeAdapter()


def get_transform_adapter(engine: str) -> ComputeJobAdapter:
    return DummyComputeAdapter()
