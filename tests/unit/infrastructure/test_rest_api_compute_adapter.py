from __future__ import annotations

import json
import pathlib
from unittest.mock import patch

import pytest

from app.infrastructure.adapters.compute.rest_api_compute_adapter import (
    RestApiComputeAdapter,
)


class MockSecretManager:
    async def resolve(self, ref: str) -> dict[str, str]:
        return {"token": "fake-token"}


@pytest.mark.asyncio
async def test_extract_async_includes_extraction_query_params(tmp_path: pathlib.Path) -> None:
    """_extract_async deve fazer merge do extraction_query (quando JSON dict) nos params HTTP."""
    adapter = RestApiComputeAdapter(
        secret_manager=MockSecretManager(),
        output_base_dir=str(tmp_path),
    )

    captured_params: list[dict] = []

    async def fake_fetch(client, path: str, params: dict) -> list:
        captured_params.append(params)
        return [{"id": 1, "name": "item1"}]

    config = {
        "base_url": "http://api.fake.com",
        "resource_path": "/products",
        "source_objects": [
            {
                "object_id": "products",
                "extraction_query": json.dumps({"category": "electronics", "status": "active"}),
            }
        ],
    }

    output_dir = tmp_path / "pipe-rest" / "job-rest"

    with patch.object(adapter, "_fetch_page", side_effect=fake_fetch):
        await adapter._extract_async(
            job_id="job-rest",
            config=config,
            output_dir=output_dir,
        )

    assert len(captured_params) >= 1
    assert captured_params[0]["category"] == "electronics"
    assert captured_params[0]["status"] == "active"
