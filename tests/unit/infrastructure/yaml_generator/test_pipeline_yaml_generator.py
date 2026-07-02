from __future__ import annotations

import uuid

import yaml

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig, ComputeEngine
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import CronSchedule, ScheduleConfig, ScheduleMode
from app.domain.pipelines.sensor_config import SensorConfig
from app.domain.pipelines.transform_config import TransformConfig, TransformEngine
from app.domain.shared.value_objects import EmailAddress
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


def test_pipeline_yaml_generator() -> None:
    generator = PipelineYamlGenerator()

    sensor = SensorConfig(query="SELECT 1", timeout_minutes=30, poke_interval_seconds=60)
    extraction = ExtractionConfig(
        object_id="obj-1",
        load_strategy=LoadStrategy.INCREMENTAL,
        watermark_column="updated_at",
        sensor=sensor,
    )

    pipeline = Pipeline(
        id=str(uuid.uuid4()),
        name="test-pipeline",
        type=PipelineType.INGESTION,
        owner=EmailAddress("owner@co.com"),
        schema_version="v2",
        source_asset_id="asset-src",
        destination_asset_id="asset-dest",
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *")),
        source_objects=[extraction],
        destination_objects=[],
        transform=TransformConfig(engine=TransformEngine.NONE),
        compute=ComputeConfig(engine=ComputeEngine.DEFAULT, staging_bucket="s3://bucket"),
        airflow=AirflowConfig(retries=3),
    )

    yaml_str = generator.generate(pipeline)

    # Verify the generated YAML can be parsed and contains the expected keys
    parsed = yaml.safe_load(yaml_str)

    assert parsed["schema_version"] == "v2"
    assert parsed["pipeline"]["name"] == "test-pipeline"
    assert parsed["pipeline"]["schedule"]["mode"] == "cron"
    assert parsed["pipeline"]["schedule"]["cron"] == "0 0 * * *"
    assert parsed["pipeline"]["source"]["asset_id"] == "asset-src"

    source_obj = parsed["pipeline"]["source"]["objects"][0]
    assert source_obj["object_id"] == "obj-1"
    assert source_obj["load_strategy"] == "incremental"
    assert source_obj["watermark_column"] == "updated_at"
    assert source_obj["sensor"]["query"] == "SELECT 1"
