from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SensorConfig:
    """
    Pre-extraction readiness sensor configuration.

    Encapsulates all sensor-related settings independently of extraction settings.
    Separation of concern: sensor logic is about 'can I start?' while
    ExtractionConfig is about 'how do I extract?'.

    Implemented in Airflow 3 via @task.sensor with mode="reschedule".

    query: SQL that returns a truthy value (1, TRUE, non-empty) when the source is ready.
    timeout_minutes: sensor fails if the query does not return truthy within this window.
    poke_interval_seconds: interval between query re-executions. Use reschedule mode
      to free worker slots between pokes.
    """

    query: str
    timeout_minutes: int = 60
    poke_interval_seconds: int = 60

    def __post_init__(self) -> None:
        if not self.query.strip():
            raise ValueError("SensorConfig.query cannot be empty")
        if self.timeout_minutes <= 0:
            raise ValueError("SensorConfig.timeout_minutes must be > 0")
        if self.poke_interval_seconds <= 0:
            raise ValueError("SensorConfig.poke_interval_seconds must be > 0")

    def has_sensor_query(self) -> bool:
        return bool(self.query.strip())
