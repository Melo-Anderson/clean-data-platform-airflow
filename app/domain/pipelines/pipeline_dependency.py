from __future__ import annotations

from dataclasses import dataclass

from app.domain.pipelines.dependency_type import DependencyType


@dataclass(frozen=True)
class PipelineDependency:
    """
    Represents a dependency of this pipeline on an upstream pipeline or event.

    dependency_type determines how the dependency is implemented in Airflow:
      DATASET -> schedule=[Asset("platform://pipeline/{pipeline_id}")]
      EXTERNAL_EVENT -> reserved for future event-based triggers
      MANUAL -> reserved for manual gate tasks

    require_same_day is only meaningful for DATASET dependencies:
      True -> ShortCircuitOperator gate enforces same calendar day (UTC) success.
    """

    pipeline_id: str
    require_same_day: bool = False
    dependency_type: DependencyType = DependencyType.DATASET
