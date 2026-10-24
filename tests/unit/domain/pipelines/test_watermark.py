from __future__ import annotations

import pytest

from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.pipeline import Pipeline
from app.domain.pipelines.pipeline_type import PipelineType as DomainPipelineType
from app.domain.pipelines.schedule_config import ScheduleConfig
from app.domain.pipelines.schedule_mode import ScheduleMode
from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.domain.shared.value_objects import CronSchedule, EmailAddress


def _make_schedule() -> ScheduleConfig:
    return ScheduleConfig(mode=ScheduleMode.CRON, cron_schedule=CronSchedule("0 0 * * *"))


def test_integer_literal_is_unquoted() -> None:
    wm = Watermark.parse(10, WatermarkType.INTEGER)
    assert wm.as_sql_literal() == "10"
    assert wm.as_param_value() == "10"


def test_float_literal_is_unquoted() -> None:
    wm = Watermark.parse(12.345, WatermarkType.FLOAT)
    assert wm.as_sql_literal() == "12.345"
    assert wm.as_param_value() == "12.345"


def test_string_literal_escapes_single_quotes() -> None:
    wm = Watermark.parse("O'Brien", WatermarkType.STRING)
    assert wm.as_sql_literal() == "'O''Brien'"
    assert wm.as_param_value() == "O'Brien"


def test_timestamp_literal_validates_iso_and_quotes() -> None:
    wm = Watermark.parse("2026-10-24T10:00:00+00:00", WatermarkType.TIMESTAMP)
    assert wm.as_sql_literal() == "'2026-10-24T10:00:00+00:00'"
    assert wm.as_param_value() == "2026-10-24T10:00:00+00:00"


def test_date_literal_validates_iso_and_quotes() -> None:
    wm = Watermark.parse("2026-10-24", WatermarkType.DATE)
    assert wm.as_sql_literal() == "'2026-10-24'"
    assert wm.as_param_value() == "2026-10-24"


def test_invalid_integer_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Cannot parse 'abc' as integer"):
        Watermark.parse("abc", WatermarkType.INTEGER)


def test_invalid_float_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Cannot parse 'invalid' as float"):
        Watermark.parse("invalid", WatermarkType.FLOAT)


def test_invalid_timestamp_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Cannot parse 'not-a-timestamp' as timestamp"):
        Watermark.parse("not-a-timestamp", WatermarkType.TIMESTAMP)


def test_invalid_date_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Cannot parse 'not-a-date' as date"):
        Watermark.parse("not-a-date", WatermarkType.DATE)


def test_incremental_without_column_or_type_rejected() -> None:
    with pytest.raises(ValueError, match="watermark_column and watermark_type are required"):
        Pipeline(
            id="p-1",
            name="p1",
            type=DomainPipelineType.INGESTION,
            owner=EmailAddress("test@example.com"),
            schedule=_make_schedule(),
            source_objects=[
                ExtractionConfig(
                    object_name="users",
                    load_strategy=LoadStrategy.INCREMENTAL,
                )
            ],
        )


def test_incremental_initial_value_must_match_type() -> None:
    with pytest.raises(ValueError, match="Cannot parse 'invalid_num' as integer"):
        Pipeline(
            id="p-1",
            name="p1",
            type=DomainPipelineType.INGESTION,
            owner=EmailAddress("test@example.com"),
            schedule=_make_schedule(),
            source_objects=[
                ExtractionConfig(
                    object_name="users",
                    load_strategy=LoadStrategy.INCREMENTAL,
                    watermark_column="id",
                    watermark_type=WatermarkType.INTEGER,
                    watermark_initial_value="invalid_num",
                )
            ],
        )


def test_multiple_incremental_objects_rejected() -> None:
    with pytest.raises(ValueError, match="At most one incremental source object"):
        Pipeline(
            id="p-1",
            name="p1",
            type=DomainPipelineType.INGESTION,
            owner=EmailAddress("test@example.com"),
            schedule=_make_schedule(),
            source_objects=[
                ExtractionConfig(
                    object_name="o1",
                    load_strategy=LoadStrategy.INCREMENTAL,
                    watermark_column="id",
                    watermark_type=WatermarkType.INTEGER,
                ),
                ExtractionConfig(
                    object_name="o2",
                    load_strategy=LoadStrategy.INCREMENTAL,
                    watermark_column="id",
                    watermark_type=WatermarkType.INTEGER,
                ),
            ],
        )
