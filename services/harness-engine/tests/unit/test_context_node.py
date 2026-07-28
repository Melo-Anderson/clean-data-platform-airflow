"""Tests para context_node com injeção de platform contracts."""

from unittest.mock import MagicMock

from src.domain.ports import MetadataPort, MetricsPort, PlatformExamplesPort, PlatformSchemaPort


def _make_ports(schema: dict | None = None, examples: dict | None = None) -> tuple:
    metadata = MagicMock(spec=MetadataPort)
    metrics = MagicMock(spec=MetricsPort)
    schema_port = MagicMock(spec=PlatformSchemaPort)
    schema_port.get_json_schema.return_value = schema or {"type": "object"}
    examples_port = MagicMock(spec=PlatformExamplesPort)
    examples_port.get_gold_examples.return_value = examples or {"etl": "yaml..."}
    return metadata, metrics, schema_port, examples_port


def test_context_node_injects_platform_schema() -> None:
    from src.application.graph.nodes.context_node import make_context_node

    _, _, sp, ep = _make_ports(schema={"type": "object", "properties": {"pipeline_id": {}}})
    meta, metr = MagicMock(), MagicMock()
    node = make_context_node(meta, metr, sp, ep)

    result = node({"user_prompt": "create etl", "context": {}})
    expected = {"type": "object", "properties": {"pipeline_id": {}}}
    assert result["context"]["platform_schema"] == expected


def test_context_node_injects_gold_examples() -> None:
    from src.application.graph.nodes.context_node import make_context_node

    _, _, sp, ep = _make_ports(examples={"ingestion": "yaml_ing...", "etl": "yaml_etl..."})
    meta, metr = MagicMock(), MagicMock()
    node = make_context_node(meta, metr, sp, ep)

    result = node({"user_prompt": "create pipeline", "context": {}})
    assert result["context"]["gold_examples"] == {"ingestion": "yaml_ing...", "etl": "yaml_etl..."}


def test_context_node_preserves_existing_fields() -> None:
    """few_shot_examples e platform_rules estáticos devem continuar presentes."""
    from src.application.graph.nodes.context_node import make_context_node

    _, _, sp, ep = _make_ports()
    meta, metr = MagicMock(), MagicMock()
    node = make_context_node(meta, metr, sp, ep)

    result = node({"user_prompt": "test", "context": {}})
    ctx = result["context"]
    # Campos legados ainda presentes
    assert "few_shot_examples" in ctx
    assert "platform_rules" in ctx
    # Campos novos
    assert "platform_schema" in ctx
    assert "gold_examples" in ctx


def test_context_node_calls_ports_once() -> None:
    """Deve chamar cada port exatamente uma vez por invocação."""
    from src.application.graph.nodes.context_node import make_context_node

    _, _, sp, ep = _make_ports()
    meta, metr = MagicMock(), MagicMock()
    node = make_context_node(meta, metr, sp, ep)
    node({"user_prompt": "test", "context": {}})

    sp.get_json_schema.assert_called_once()
    ep.get_gold_examples.assert_called_once()
