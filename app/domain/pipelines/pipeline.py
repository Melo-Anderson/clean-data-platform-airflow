from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.discovery_task_config import DiscoveryTaskConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.transform_config import TransformConfig
from app.domain.shared.auditable import Auditable
from app.domain.shared.value_objects import EmailAddress

CURRENT_SCHEMA_VERSION = "1.0"


@dataclass(kw_only=True)
class Pipeline(Auditable):
    """
    Pipeline aggregate. Represents a DAG in Airflow.

    schedule has no default - must be explicitly declared at construction.
    This prevents accidental pipelines with unspecified scheduling intent.

    dataset_uri: Airflow 3 Asset URI for this pipeline.
      Used as DAG outlet (what this pipeline produces) and as inlet in downstream deps.
      Convention: platform://pipeline/{id}
    """

    id: str
    name: str
    type: PipelineType
    owner: EmailAddress
    schedule: ScheduleConfig  # Required - no default. Caller must declare scheduling intent.
    schema_version: str = CURRENT_SCHEMA_VERSION
    source_asset_id: str = ""
    source_objects: list[ExtractionConfig] = field(default_factory=list)
    destination_asset_id: str = ""
    destination_objects: list[DestinationObjectConfig] = field(default_factory=list)
    transform: TransformConfig = field(default_factory=TransformConfig)
    compute: ComputeConfig = field(default_factory=ComputeConfig)
    quality_rules: list[QualityRule] = field(default_factory=list)
    airflow: AirflowConfig = field(default_factory=AirflowConfig)
    discovery_task: DiscoveryTaskConfig = field(default_factory=DiscoveryTaskConfig)

    @property
    def dataset_uri(self) -> str:
        """Airflow 3 Asset URI. Declared as outlet on every generated DAG."""
        return f"platform://pipeline/{self.id}"
