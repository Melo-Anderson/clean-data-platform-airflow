from __future__ import annotations

from app.domain.objects.data_element import DataElement
from app.domain.objects.data_object import DataObject
from app.domain.objects.element_type import ElementType
from app.domain.objects.object_type import ObjectType
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.pipelines.sensor_config import SensorConfig
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.dag_generator.ci_validator import CiValidator
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


def test_end_to_end_pipeline_generation() -> None:
    # 2. Create a DataObject that uses this Endpoint
    element = DataElement(
        id="el-1",
        object_id="obj-customers",
        name="customer_id",
        source_type=ElementType.STRING,
        destination_type=ElementType.STRING,
    )
    data_object = DataObject(
        id="obj-customers",
        asset_id="asset-1",
        name="customers",
        type=ObjectType.TABLE,
        elements=[element],
    )

    # 3. Create an Ingestion Pipeline for this DataObject
    pipeline = Pipeline(
        id="pipe-ingest-customers",
        name="ingest-customers",
        type=PipelineType.INGESTION,
        owner=EmailAddress("data-eng@co.com"),
        schedule=ScheduleConfig(
            mode=ScheduleMode.CRON,
            cron_schedule=CronSchedule("0 2 * * *")
        ),
        source_objects=[
            ExtractionConfig(
                object_id=data_object.id,
                load_strategy=LoadStrategy.INCREMENTAL,
                sensor=SensorConfig(
                    query="SELECT 1 FROM sync_log WHERE table='customers'",
                    timeout_minutes=30
                )
            )
        ],
    )

    # 4. Generate the YAML representation
    yaml_generator = PipelineYamlGenerator()
    yaml_content = yaml_generator.generate(pipeline)
    
    assert "ingest-customers" in yaml_content
    assert "obj-customers" in yaml_content

    # 5. Validate the YAML with CiValidator
    ci_validator = CiValidator()
    errors = ci_validator.validate_yaml(yaml_content)
    assert not errors, f"YAML validation failed: {errors}"

    # 6. Generate the Airflow DAG Code from the YAML
    dag_generator = DagGenerator()
    dag_code = dag_generator.generate(yaml_content)

    # 7. Assert DAG contains expected elements
    assert "ingest_customers_dag" in dag_code
    assert "@dag(" in dag_code
    assert 'group_id="source_readiness"' in dag_code
    assert "source_readiness_sensor_obj_customers" in dag_code
