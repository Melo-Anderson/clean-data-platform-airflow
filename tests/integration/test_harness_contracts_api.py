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
async def test_get_harness_gold_examples_returns_ingestion(client: AsyncClient) -> None:
    r = await client.get("/v1/harness/gold-examples?type=ingestion")
    assert r.status_code == 200
    body = r.json()
    assert body["pipeline_type"] == "ingestion"
    assert "examples" in body
    assert len(body["examples"]) >= 1
    for example in body["examples"]:
        assert isinstance(example["yaml_snippet"], str)
        assert len(example["yaml_snippet"]) > 10
        assert "quality" not in example["yaml_snippet"]
