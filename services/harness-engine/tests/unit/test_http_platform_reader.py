"""Tests para o HTTP adapter que busca contratos da plataforma."""

from unittest.mock import MagicMock, patch


@patch("src.infrastructure.adapters.http_platform_reader.httpx.get")
def test_get_json_schema_fetches_from_url(mock_get: MagicMock) -> None:
    from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"type": "object", "properties": {"pipeline_id": {}}}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    reader = HttpPlatformReader("http://schema-url", "http://examples-url")
    schema = reader.get_json_schema()

    assert schema == {"type": "object", "properties": {"pipeline_id": {}}}
    mock_get.assert_called_once_with("http://schema-url", timeout=10.0)


@patch("src.infrastructure.adapters.http_platform_reader.httpx.get")
def test_get_gold_examples_fetches_from_url(mock_get: MagicMock) -> None:
    from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader

    mock_resp = MagicMock()
    mock_resp.json.return_value = {"ingestion": "yaml...", "etl": "yaml..."}
    mock_resp.raise_for_status.return_value = None
    mock_get.return_value = mock_resp

    reader = HttpPlatformReader("http://schema-url", "http://examples-url")
    examples = reader.get_gold_examples()

    assert "ingestion" in examples
    mock_get.assert_called_once_with("http://examples-url", timeout=10.0)


@patch("src.infrastructure.adapters.http_platform_reader.httpx.get")
def test_get_json_schema_returns_fallback_on_error(mock_get: MagicMock) -> None:
    from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader

    mock_get.side_effect = Exception("Connection refused")
    reader = HttpPlatformReader("http://down-url", "http://examples-url")
    schema = reader.get_json_schema()

    # Fallback deve retornar um dict vazio (não lançar exceção)
    assert isinstance(schema, dict)


@patch("src.infrastructure.adapters.http_platform_reader.httpx.get")
def test_get_gold_examples_returns_fallback_on_error(mock_get: MagicMock) -> None:
    from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader

    mock_get.side_effect = Exception("Timeout")
    reader = HttpPlatformReader("http://schema-url", "http://down-url")
    examples = reader.get_gold_examples()

    assert isinstance(examples, dict)
