import pytest

from app.application.pipelines.pipeline_normalizer import PipelineNormalizer
from app.domain.shared.platform_defaults import PlatformDefaults


@pytest.fixture
def normalizer() -> PipelineNormalizer:
    return PipelineNormalizer(defaults=PlatformDefaults())


def test_normalize_airflow_uses_platform_defaults_when_empty(
    normalizer: PipelineNormalizer,
) -> None:
    result = normalizer.normalize_airflow({})
    assert result.retries == 3
    assert result.sla_minutes == 90
    assert result.pool == "default_pool"


def test_normalize_airflow_overrides_specific_fields(normalizer: PipelineNormalizer) -> None:
    result = normalizer.normalize_airflow({"retries": 5, "sla_minutes": 60})
    assert result.retries == 5
    assert result.sla_minutes == 60
    assert result.pool == "default_pool"


def test_normalize_extraction_applies_defaults(normalizer: PipelineNormalizer) -> None:
    result = normalizer.normalize_extraction([{"object_name": "orders"}])
    assert result[0].page_size == 1000
    assert result[0].compression == "snappy"
    assert result[0].encoding == "utf-8"


def test_normalize_extraction_overrides_fields(normalizer: PipelineNormalizer) -> None:
    result = normalizer.normalize_extraction(
        [{"object_name": "orders", "page_size": 500, "compression": "gzip"}]
    )
    assert result[0].page_size == 500
    assert result[0].compression == "gzip"


def test_normalize_compute_applies_defaults(normalizer: PipelineNormalizer) -> None:
    result = normalizer.normalize_compute({})
    assert result.num_workers == 1
    assert result.machine_type == "n1-standard-2"
    assert result.engine.value == "duckdb"


def test_normalize_compute_handles_nested_config(
    normalizer: PipelineNormalizer,
) -> None:
    raw = {
        "engine": "dbt",
        "staging_bucket": "gs://staging",
        "config": {
            "num_workers": 4,
            "machine_type": "n2-standard-4",
            "format": "parquet",
        },
    }
    result = normalizer.normalize_compute(raw)
    assert result.engine.value == "dbt"
    assert result.staging_bucket == "gs://staging"
    assert result.num_workers == 4
    assert result.machine_type == "n2-standard-4"
    assert result.format == "parquet"

    engine_cfg = result.to_engine_config()
    assert engine_cfg["num_workers"] == 4
    assert engine_cfg["machine_type"] == "n2-standard-4"
    assert engine_cfg["format"] == "parquet"


def test_normalize_compute_handles_flat_config(normalizer: PipelineNormalizer) -> None:
    raw = {
        "engine": "omnibeam",
        "source_type": "storage",
        "format": "csv",
        "driver": "s3",
        "endpoint": "https://s3.amazonaws.com",
    }
    result = normalizer.normalize_compute(raw)
    assert result.engine.value == "omnibeam"
    assert result.source_type == "storage"
    assert result.format == "csv"
    assert result.driver == "s3"
    assert result.endpoint == "https://s3.amazonaws.com"

    engine_cfg = result.to_engine_config()
    assert engine_cfg["source_type"] == "storage"
    assert engine_cfg["format"] == "csv"
    assert engine_cfg["driver"] == "s3"
    assert engine_cfg["endpoint"] == "https://s3.amazonaws.com"
