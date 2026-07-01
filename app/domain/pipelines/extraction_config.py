from __future__ import annotations

from dataclasses import dataclass

from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.sensor_config import SensorConfig


@dataclass(frozen=True)
class ExtractionConfig:
    """
    Extraction configuration for one source DataObject within a Pipeline.

    Replaces SourceObjectConfig from v1, with sensor settings extracted to SensorConfig.

    extraction_query: When provided, replaces the auto-generated query.
      AE must include the watermark filter manually when using incremental load.
    sensor: When provided, a readiness sensor task is generated before extraction.
      None means no sensor - extraction starts immediately after classify_changes_and_plan_actions.

    XCom policy: the extract task writes data to GCS/S3 and passes only the path via XCom.
    """

    object_id: str
    load_strategy: LoadStrategy = LoadStrategy.FULL_LOAD
    watermark_column: str | None = None
    page_size: int = 1000
    partition_column: str | None = None
    compression: str = "snappy"
    encoding: str = "utf-8"
    extraction_query: str | None = None
    sensor: SensorConfig | None = None

    def has_sensor(self) -> bool:
        return self.sensor is not None
