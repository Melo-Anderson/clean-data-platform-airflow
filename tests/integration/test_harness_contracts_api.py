"""Testa o endpoint de contratos do Harness Engine (schema + gold examples)."""

from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.anyio
async def test_get_harness_schema_returns_valid_jsonschema(client: AsyncClient) -> None:
    r = await client.get("/v1/harness/schema?pipeline_type=ingestion&endpoint_type=relational")
    assert r.status_code == 200
    body = r.json()
    assert "properties" in body or "$defs" in body
    assert "$schema" in body
    assert "source_asset" in body.get("required", [])
    assert "source_objects" in body.get("required", [])


@pytest.mark.anyio
async def test_validate_endpoint_returns_pydantic_errors_for_missing_fields(
    client: AsyncClient,
) -> None:
    yaml_missing = (
        "name: p\npipeline_type: ingestion\nowner_email: a@b.com\ncron_schedule: '@daily'"
    )
    r = await client.post(
        "/v1/harness/validate",
        json={
            "pipeline_yaml": yaml_missing,
            "pipeline_type": "ingestion",
            "endpoint_type": "relational",
        },
    )
    assert r.status_code == 200
    data = r.json()
    assert not data["is_valid"]
    assert any(e["json_pointer"] == "/source_asset" for e in data["errors"])
    assert any(e["error_code"] == "MISSING_OR_INVALID_FIELD" for e in data["errors"])


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
