from __future__ import annotations

import uuid

from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.pipelines.sensor_config import SensorConfig
from app.domain.pipelines.transform_config import TransformConfig
from app.domain.pipelines.transform_engine import TransformEngine
from app.domain.shared.value_objects import CronSchedule, EmailAddress
from app.infrastructure.dag_generator.dag_generator import DagGenerator
from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator


def _yaml(pipeline: Pipeline) -> str:
    return PipelineYamlGenerator().generate(pipeline)


def _make_pipeline(pipeline_type: PipelineType, sensor: bool = False) -> Pipeline:
    sensor_cfg = SensorConfig(query="SELECT 1 FROM batch WHERE done=1") if sensor else None
    extraction = ExtractionConfig(
        object_name="obj-1",
        load_strategy=LoadStrategy.INCREMENTAL,
        sensor=sensor_cfg,
    )
    return Pipeline(
        id=str(uuid.uuid4()),
        name=f"test_{pipeline_type.value}",
        type=pipeline_type,
        owner=EmailAddress("ae@co.com"),
        schedule=ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 6 * * *")),
        source_objects=[extraction] if pipeline_type != PipelineType.ETL else [],
        transform=TransformConfig(engine=TransformEngine.DBT, ref="models/test.sql")
        if pipeline_type == PipelineType.ETL
        else TransformConfig(),
    )


def test_ingestion_dag_uses_airflow3_dag_decorator() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert "@dag(" in dag_code
    assert "from airflow.sdk import" in dag_code
    assert "Asset(" in dag_code


def test_ingestion_dag_has_all_15_tasks() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    required_tasks = [
        "check_dependencies",
        "validate_source_and_discovery",
        "classify_changes_and_plan_actions",
        "submit_compute_job",
        "monitor_compute_job",
        "validate_compute_execution",
        "read_compute_metrics",
        "quality_gate",
        "emit_raw_lineage",
        "load_to_data_warehouse",
        "post_load_validation",
        "emit_final_lineage",
        "emit_monitoring_and_sla",
        "success_notification",
    ]
    for task_name in required_tasks:
        assert task_name in dag_code, f"Missing task: {task_name}"


def test_ingestion_dag_with_sensor_generates_task_sensor() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION, sensor=True)))
    assert "@task.sensor" in dag_code
    assert "source_readiness_sensor" in dag_code
    assert 'mode="reschedule"' in dag_code


def test_etl_dag_uses_dag_decorator_and_has_12_tasks() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.ETL)))
    assert "@dag(" in dag_code
    required = [
        "check_dependencies",
        "validate_source_models",
        "classify_schema_changes",
        "submit_transformation_job",
        "monitor_transformation_job",
        "validate_transformation_execution",
        "read_transformation_metrics",
        "quality_gate",
        "publish_documentation",
        "emit_lineage",
        "emit_monitoring_and_sla",
        "success_notification",
    ]
    for t in required:
        assert t in dag_code


def test_export_dag_has_on_failure_callback() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.EXPORT)))
    assert "on_failure_callback=alert_and_monitoring" in dag_code


def test_all_dags_have_outlets_pipeline_asset() -> None:
    for ptype in [PipelineType.INGESTION, PipelineType.ETL, PipelineType.EXPORT]:
        dag_code = DagGenerator().generate(_yaml(_make_pipeline(ptype)))
        assert "_PIPELINE_ASSET" in dag_code
        assert "outlets=[" in dag_code
        assert "platform://pipeline/" in dag_code


def test_transformation_dag_uses_airflow_sdk_not_decorators() -> None:
    """transformation_dag deve usar 'from airflow.sdk', não 'from airflow.decorators'."""
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.TRANSFORMATION)))
    assert "from airflow.decorators import" not in dag_code
    assert "from airflow.sdk import" in dag_code


def test_transformation_dag_has_no_hardcoded_start_date() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.TRANSFORMATION)))
    assert "start_date=datetime(" not in dag_code


def test_ingestion_dag_sensor_does_not_use_manual_xcom_push() -> None:
    """Monitor sensor deve retornar job_result diretamente, sem xcom_push manual."""
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert 'xcom_push(key="job_result"' not in dag_code, (
        "monitor_compute_job usa xcom_push manual — use retorno de valor no Airflow 3"
    )


def test_etl_dag_sensor_does_not_use_manual_xcom_push() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.ETL)))
    assert 'xcom_push(key="job_result"' not in dag_code


def test_export_dag_sensor_does_not_use_manual_xcom_push() -> None:
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.EXPORT)))
    assert 'xcom_push(key="job_result"' not in dag_code


def test_generated_dags_do_not_use_get_task_instances() -> None:
    """DAGs geradas não devem usar get_task_instances nem get_task_instance."""
    gen = DagGenerator()
    for ptype in (PipelineType.INGESTION, PipelineType.ETL, PipelineType.EXPORT):
        dag_code = gen.generate(_yaml(_make_pipeline(ptype)))
        assert "get_task_instances" not in dag_code, f"{ptype} contém get_task_instances"
        assert "get_task_instance" not in dag_code, f"{ptype} contém get_task_instance"


def test_ingestion_dag_declares_asset_alias_for_type_group() -> None:
    """Ingestion DAGs devem declarar AssetAlias para o grupo por pipeline type."""
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert "AssetAlias(" in dag_code
    assert "platform://group/ingestion" in dag_code


def test_ingestion_dag_with_sensor_uses_dynamic_task_mapping() -> None:
    """Com sensores configurados, deve usar .expand() — não loop de templates."""
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION, sensor=True)))
    assert ".expand(" in dag_code, "DAG com sensor deve usar Dynamic Task Mapping (.expand())"
    assert "_source_readiness_sensor_2" not in dag_code


def test_generated_dags_use_platform_failure_notification() -> None:
    """DAGs geradas devem usar PlatformFailureNotification em default_args."""
    dag_code = DagGenerator().generate(_yaml(_make_pipeline(PipelineType.INGESTION)))
    assert "PlatformFailureNotification()" in dag_code


def test_success_notification_not_downstream_of_monitoring() -> None:
    """success_notification não deve depender de emit_monitoring_and_sla (all_done)."""
    gen = DagGenerator()
    for ptype in (PipelineType.INGESTION, PipelineType.ETL):
        dag_code = gen.generate(_yaml(_make_pipeline(ptype)))
        assert "monitoring >> notification" not in dag_code
        assert "monitoring >> notif" not in dag_code
