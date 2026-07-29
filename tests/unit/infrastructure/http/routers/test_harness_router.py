from __future__ import annotations

import pytest
from httpx import AsyncClient


@pytest.mark.asyncio
async def test_validate_endpoint_success(ae_client: AsyncClient) -> None:
    payload = {"pipeline_yaml": "pipeline_id: test\ntype: ingestion", "pipeline_type": "relational"}
    response = await ae_client.post("/v1/harness/validate", json=payload)
    assert response.status_code == 200


@pytest.mark.asyncio
async def test_get_schema(ae_client: AsyncClient) -> None:
    response = await ae_client.get("/v1/harness/schema?type=relational")
    assert response.status_code == 200
    assert response.json()["type"] == "object"


@pytest.mark.asyncio
async def test_get_gold_examples(ae_client: AsyncClient) -> None:
    response = await ae_client.get("/v1/harness/gold-examples?type=relational")
    assert response.status_code == 200
    assert len(response.json()["examples"]) > 0
