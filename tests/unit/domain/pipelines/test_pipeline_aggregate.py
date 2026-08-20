from __future__ import annotations

import pytest

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import CronSchedule, ScheduleConfig, ScheduleMode
from app.domain.pipelines.sensor_config import SensorConfig
from app.domain.shared.value_objects import EmailAddress


def test_pipeline_sensor_timeout_exceeding_dag_timeout_raises_value_error() -> None:
    with pytest.raises(
        ValueError, match="sensor timeout .* cannot exceed pipeline execution timeout"
    ):
        Pipeline(
            id="pipe-invalid",
            name="invalid_pipe",
            type=PipelineType.INGESTION,
            owner=EmailAddress("eng@co.com"),
            schedule=ScheduleConfig(
                mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")
            ),
            airflow=AirflowConfig(execution_timeout_minutes=30),
            source_objects=[
                ExtractionConfig(
                    object_id="users",
                    sensor=SensorConfig(query="SELECT 1", timeout_minutes=60),
                )
            ],
        )


def test_pipeline_valid_invariants_constructs_successfully() -> None:
    pipe = Pipeline(
        id="pipe-valid",
        name="valid_pipe",
        type=PipelineType.INGESTION,
        owner=EmailAddress("eng@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        airflow=AirflowConfig(execution_timeout_minutes=60),
        source_objects=[
            ExtractionConfig(
                object_id="users",
                sensor=SensorConfig(query="SELECT 1", timeout_minutes=30),
            )
        ],
    )
    assert pipe.name == "valid_pipe"
    assert pipe.dataset_uri == "platform://pipeline/pipe-valid"
