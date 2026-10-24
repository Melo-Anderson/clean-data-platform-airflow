from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from app.domain.pipelines.watermark import Watermark, WatermarkType
from app.infrastructure.adapters.platform.pipeline_run_api_client import PipelineRunApiClient


def test_get_last_watermark_returns_watermark_when_present() -> None:
    client = PipelineRunApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"value": "2026-10-24T10:00:00", "type": "timestamp"}
        mock_get.return_value = mock_response

        wm = client.get_last_watermark("p-1")
        assert wm == Watermark("2026-10-24T10:00:00", WatermarkType.TIMESTAMP)
        mock_get.assert_called_once_with("/v1/pipelines/p-1/watermark")


def test_get_last_watermark_returns_none_when_null_or_empty() -> None:
    client = PipelineRunApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = None
        mock_get.return_value = mock_response

        assert client.get_last_watermark("p-1") is None


def test_get_last_watermark_returns_none_on_404() -> None:
    client = PipelineRunApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        assert client.get_last_watermark("p-not-found") is None


def test_get_last_watermark_raises_on_500_fail_fast() -> None:
    client = PipelineRunApiClient(base_url="http://mock-api:8000")
    with patch("httpx.Client.get") as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 500
        mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Internal Server Error",
            request=MagicMock(),
            response=mock_response,
        )
        mock_get.return_value = mock_response

        with pytest.raises(httpx.HTTPStatusError):
            client.get_last_watermark("p-1")
