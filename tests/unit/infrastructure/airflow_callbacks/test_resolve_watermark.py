from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.infrastructure.adapters.platform.pipeline_run_api_client import PipelineRunApiClient
from app.infrastructure.airflow_callbacks.ingestion_callbacks import resolve_watermark_context


def test_override_takes_precedence() -> None:
    mock_client = MagicMock(spec=PipelineRunApiClient)
    mock_client.get_last_watermark.return_value = Watermark("50", WatermarkType.INTEGER)

    ctx = resolve_watermark_context(
        pipeline_id="p-1",
        column="id",
        declared_type=WatermarkType.INTEGER,
        override="999",
        initial_value="10",
        client=mock_client,
    )
    assert ctx == {"column": "id", "value": "999", "type": "integer"}
    mock_client.get_last_watermark.assert_not_called()


def test_stored_watermark_used_when_no_override() -> None:
    mock_client = MagicMock(spec=PipelineRunApiClient)
    mock_client.get_last_watermark.return_value = Watermark("250", WatermarkType.INTEGER)

    ctx = resolve_watermark_context(
        pipeline_id="p-1",
        column="id",
        declared_type=WatermarkType.INTEGER,
        override=None,
        initial_value="10",
        client=mock_client,
    )
    assert ctx == {"column": "id", "value": "250", "type": "integer"}
    mock_client.get_last_watermark.assert_called_once_with("p-1")


def test_initial_value_used_when_no_override_and_no_stored() -> None:
    mock_client = MagicMock(spec=PipelineRunApiClient)
    mock_client.get_last_watermark.return_value = None

    ctx = resolve_watermark_context(
        pipeline_id="p-1",
        column="id",
        declared_type=WatermarkType.INTEGER,
        override=None,
        initial_value="10",
        client=mock_client,
    )
    assert ctx == {"column": "id", "value": "10", "type": "integer"}


def test_returns_none_when_no_watermark_available() -> None:
    mock_client = MagicMock(spec=PipelineRunApiClient)
    mock_client.get_last_watermark.return_value = None

    ctx = resolve_watermark_context(
        pipeline_id="p-1",
        column="id",
        declared_type=WatermarkType.INTEGER,
        override=None,
        initial_value=None,
        client=mock_client,
    )
    assert ctx is None


def test_stored_type_mismatch_raises_value_error() -> None:
    mock_client = MagicMock(spec=PipelineRunApiClient)
    mock_client.get_last_watermark.return_value = Watermark("abc", WatermarkType.STRING)

    with pytest.raises(ValueError, match="does not match declared type"):
        resolve_watermark_context(
            pipeline_id="p-1",
            column="id",
            declared_type=WatermarkType.INTEGER,
            override=None,
            initial_value=None,
            client=mock_client,
        )


def test_invalid_override_raises_value_error() -> None:
    with pytest.raises(ValueError, match="Cannot parse"):
        resolve_watermark_context(
            pipeline_id="p-1",
            column="id",
            declared_type=WatermarkType.INTEGER,
            override="invalid_num",
            initial_value=None,
            client=MagicMock(),
        )
