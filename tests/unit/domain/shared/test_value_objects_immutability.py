from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from app.domain.objects.data_object_metadata import (
    CompositeForeignKey,
    CompositeIndex,
    DataObjectMetadata,
)
from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.discovery_task_config import DiscoveryTaskConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.on_critical_change import OnCriticalChange
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.quality_rule_type import QualityRuleType


def test_airflow_config_is_immutable() -> None:
    cfg = AirflowConfig(execution_timeout_minutes=45)
    with pytest.raises(FrozenInstanceError):
        cfg.execution_timeout_minutes = 60  # type: ignore[misc]


def test_compute_config_is_immutable() -> None:
    cfg = ComputeConfig(engine=ComputeEngine.DUCKDB)
    with pytest.raises(FrozenInstanceError):
        cfg.engine = ComputeEngine.SPARK  # type: ignore[misc]


def test_quality_rule_is_immutable() -> None:
    rule = QualityRule(type=QualityRuleType.ROW_COUNT_MIN, value=100)
    with pytest.raises(FrozenInstanceError):
        rule.value = 200  # type: ignore[misc]


def test_extraction_config_is_immutable() -> None:
    cfg = ExtractionConfig(object_id="orders")
    with pytest.raises(FrozenInstanceError):
        cfg.object_id = "customers"  # type: ignore[misc]


def test_destination_object_config_is_immutable() -> None:
    cfg = DestinationObjectConfig(object_name="orders_dest")
    with pytest.raises(FrozenInstanceError):
        cfg.object_name = "other"  # type: ignore[misc]


def test_discovery_task_config_is_immutable() -> None:
    cfg = DiscoveryTaskConfig(enabled=True, on_critical_change=OnCriticalChange.BLOCK)
    with pytest.raises(FrozenInstanceError):
        cfg.enabled = False  # type: ignore[misc]


def test_composite_index_is_immutable() -> None:
    idx = CompositeIndex(name="idx_users_email", columns=["email"], unique=True)
    with pytest.raises(FrozenInstanceError):
        idx.name = "other_name"  # type: ignore[misc]


def test_composite_foreign_key_is_immutable() -> None:
    fk = CompositeForeignKey(
        name="fk_orders_customer",
        constrained_columns=["customer_id"],
        referred_table="customers",
        referred_columns=["id"],
    )
    with pytest.raises(FrozenInstanceError):
        fk.name = "other_fk"  # type: ignore[misc]


def test_data_object_metadata_is_immutable() -> None:
    meta = DataObjectMetadata(partition_key="date")
    with pytest.raises(FrozenInstanceError):
        meta.partition_key = "id"  # type: ignore[misc]
