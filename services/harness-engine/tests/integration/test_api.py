"""API integration tests with mocked graph."""

import os

os.environ["OPENAI_API_KEY"] = "test_key"
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from src.infrastructure.api.app import create_app


@pytest.fixture
def client() -> TestClient:
    return TestClient(create_app())


def _result(status: str = "approved") -> dict:  # type: ignore[type-arg]
    return {
        "status": status,
        "generated_yaml": "schema_version: '1.0'\n",
        "validation_errors": [],
        "iteration_count": 1,
    }


def test_sync_endpoint_200(client: TestClient) -> None:
    with patch("src.infrastructure.api.routes._graph") as g:
        g.invoke.return_value = _result()
        r = client.post("/api/v1/generate-yaml", json={"prompt": "Ingest sales"})
    assert r.status_code == 200
    assert r.json()["status"] == "approved"


def test_sync_endpoint_failed_status(client: TestClient) -> None:
    with patch("src.infrastructure.api.routes._graph") as g:
        g.invoke.return_value = _result("failed_max_iterations")
        r = client.post("/api/v1/generate-yaml", json={"prompt": "broken"})
    assert r.json()["status"] == "failed_max_iterations"
