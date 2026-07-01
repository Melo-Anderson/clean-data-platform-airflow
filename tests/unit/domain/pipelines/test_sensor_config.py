from __future__ import annotations

import pytest

from app.domain.pipelines.sensor_config import SensorConfig


def test_valid_sensor_config_creates_instance() -> None:
    sensor = SensorConfig(query="SELECT 1 FROM batch WHERE done=1", timeout_minutes=60)
    assert sensor.has_sensor_query()


def test_empty_query_raises() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        SensorConfig(query="   ")


def test_zero_timeout_raises() -> None:
    with pytest.raises(ValueError, match="must be > 0"):
        SensorConfig(query="SELECT 1", timeout_minutes=0)
