"""Testa o endpoint de contratos do Harness Engine (schema + gold examples)."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_get_harness_schema_returns_valid_jsonschema(client: AsyncClient) -> None:
    r = await client.get("/v1/harness/schema")
    assert r.status_code == 200
    body = r.json()
    # PipelineSpec.model_json_schema() sempre gera 'properties' ou '$defs'
    assert "properties" in body or "$defs" in body
    # Campos obrigatorios do contrato
    props = body.get("properties", {})
    assert "pipeline_id" in props
    assert "type" in props
    assert "owner" in props


@pytest.mark.anyio
async def test_get_harness_gold_examples_returns_all_types(client: AsyncClient) -> None:
    r = await client.get("/v1/harness/gold-examples")
    assert r.status_code == 200
    body = r.json()
    assert isinstance(body, dict)
    assert "ingestion" in body
    assert "etl" in body
    assert "export" in body
    # Cada valor é uma string YAML não vazia
    for pipeline_type, yaml_str in body.items():
        assert isinstance(yaml_str, str), f"Expected str for {pipeline_type}, got {type(yaml_str)}"
        assert len(yaml_str) > 10, f"YAML for {pipeline_type} is suspiciously short"
