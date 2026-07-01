from __future__ import annotations

from enum import StrEnum


class DependencyType(StrEnum):
    """
    How this pipeline depends on upstream work.

    DATASET: Triggered by Airflow Asset event when upstream DAG completes.
      Implemented via schedule=[Asset("platform://pipeline/{id}")].
    EXTERNAL_EVENT: Triggered by an external webhook or event bus.
      Future: EventBridge, Pub/Sub, custom sensor. Reserved for extensibility.
    MANUAL: Requires explicit human trigger. Not automated.
      Implemented via schedule=None with manual trigger only.
    """

    DATASET = "dataset"
    EXTERNAL_EVENT = "external_event"
    MANUAL = "manual"
