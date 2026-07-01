from __future__ import annotations

from dataclasses import dataclass

from app.domain.pipelines.transform_engine import TransformEngine


@dataclass(frozen=True)
class TransformConfig:
    """
    engine=dbt: submit_transformation_job runs `dbt run --select {ref}`.
    engine=dataform: uses DataformCreateWorkflowInvocationOperator (native GCP).
    engine=none: no transformation job - object passes through unchanged.
    """

    engine: TransformEngine = TransformEngine.NONE
    ref: str | None = None

    def __post_init__(self) -> None:
        if self.engine != TransformEngine.NONE and not self.ref:
            raise ValueError(f"TransformConfig(engine={self.engine!r}) requires ref")
