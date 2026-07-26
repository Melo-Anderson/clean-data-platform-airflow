import os

import httpx
import pytest

pytestmark = pytest.mark.e2e

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_mock_host = os.getenv("MOCK_API_HOST", "mock-api" if _in_docker else "127.0.0.1")


def _base_url() -> str:
    return f"http://{_mock_host}:8081"


@pytest.mark.asyncio
async def test_health_returns_ok():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_openapi_schema_has_expected_paths():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/openapi.json")
    assert response.status_code == 200
    paths = response.json()["paths"]
    assert "/api/v1/customers" in paths
    assert "/api/v1/products" in paths
    assert "/api/v1/orders" in paths


@pytest.mark.asyncio
async def test_customers_returns_seeded_data_with_pagination():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/customers?page=1&limit=10")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body
    assert body["pagination"]["total_records"] >= 20
    assert body["pagination"]["page"] == 1
    assert len(body["data"]) <= 10


@pytest.mark.asyncio
async def test_products_returns_seeded_data():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/products")
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total_records"] >= 15


@pytest.mark.asyncio
async def test_orders_returns_seeded_data():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.get("/api/v1/orders")
    assert response.status_code == 200
    body = response.json()
    assert body["pagination"]["total_records"] >= 50


@pytest.mark.asyncio
async def test_batch_create_customers():
    payload = [
        {"full_name": "E2E Customer 1", "email": "e2ecust1@test.com"},
        {"full_name": "E2E Customer 2", "email": "e2ecust2@test.com"},
    ]
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        response = await client.post("/api/v1/customers/batch", json=payload)
    assert response.status_code == 200
    body = response.json()
    assert body["inserted"] == 2
    assert len(body["ids"]) == 2


@pytest.mark.asyncio
async def test_batch_create_orders():
    async with httpx.AsyncClient(base_url=_base_url()) as client:
        customers_resp = await client.get("/api/v1/customers?limit=1")
        customer_id = customers_resp.json()["data"][0]["id"]

        payload = [
            {"customer_id": customer_id, "total_amount": 199.99, "status": "PENDING"},
        ]
        response = await client.post("/api/v1/orders/batch", json=payload)

    assert response.status_code == 200
    body = response.json()
    assert body["inserted"] == 1
    assert len(body["ids"]) == 1
