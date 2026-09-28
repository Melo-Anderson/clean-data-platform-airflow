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
