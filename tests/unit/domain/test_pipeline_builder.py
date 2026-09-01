from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.pipeline_builder import PipelineBuilder
from app.domain.pipelines.pipeline_type import PipelineType
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.quality_rule_type import QualityRuleType
from app.domain.shared.value_objects import EmailAddress


def test_pipeline_builder_creates_valid_pipeline() -> None:
    pipeline = (
        PipelineBuilder(name="Platform_Silver_ETL")
        .with_id("pipe-silver-etl")
        .with_type(PipelineType.TRANSFORMATION)
        .with_owner(EmailAddress("data@platform.com"))
        .from_asset("platform_bronze")
        .to_asset("platform_silver")
        .with_compute_engine(ComputeEngine.DBT, staging_bucket="/opt/airflow/logs/dbt_outputs")
        .with_cron_schedule("0 3 * * *")
        .with_sla_minutes(90)
        .with_quality_rule(QualityRule(type=QualityRuleType.ROW_COUNT_MIN, value=1))
        .build()
    )

    assert pipeline.id == "pipe-silver-etl"
    assert pipeline.name == "Platform_Silver_ETL"
    assert pipeline.type == PipelineType.TRANSFORMATION
    assert pipeline.source_asset == "platform_bronze"
    assert pipeline.destination_asset == "platform_silver"
    assert pipeline.compute.engine == ComputeEngine.DBT
    assert pipeline.airflow.sla_minutes == 90
    assert len(pipeline.quality_rules) == 1


def test_pipeline_builder_to_yaml_output() -> None:
    from app.infrastructure.yaml_generator.pipeline_yaml_generator import PipelineYamlGenerator

    builder = (
        PipelineBuilder(name="Platform_Gold_Analytics")
        .with_type(PipelineType.TRANSFORMATION)
        .from_asset("platform_silver")
        .to_asset("platform_gold")
        .with_compute_engine(ComputeEngine.DBT)
        .with_cron_schedule("0 4 * * *")
    )

    yaml_str = PipelineYamlGenerator.from_builder(builder)
    assert "name: Platform_Gold_Analytics" in yaml_str
    assert "type: transformation" in yaml_str
    assert "engine: dbt" in yaml_str
