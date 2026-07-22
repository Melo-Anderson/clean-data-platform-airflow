from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient


@pytest.fixture()
def mock_db_session():
    session = AsyncMock()
    session.add_all = MagicMock()
    return session


@pytest.fixture()
def client(mock_db_session):
    from services.mock_store_api.database import get_db
    from services.mock_store_api.main import app

    async def override_get_db():
        yield mock_db_session

    app.dependency_overrides[get_db] = override_get_db
    with patch("services.mock_store_api.main.seed_data_if_empty", new_callable=AsyncMock):
        with TestClient(app, raise_server_exceptions=False) as c:
            yield c
    app.dependency_overrides.clear()


def test_health_returns_ok(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "service": "mock_store_api"}


def test_customers_endpoint_returns_paginated_structure(client, mock_db_session):
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []

    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/customers")
    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "pagination" in body
    assert body["pagination"]["page"] == 1


def test_products_endpoint_returns_paginated_structure(client, mock_db_session):
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []
    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/products")
    assert response.status_code == 200
    assert "data" in response.json()


def test_orders_endpoint_returns_paginated_structure(client, mock_db_session):
    mock_result_items = MagicMock()
    mock_result_items.scalars.return_value.all.return_value = []
    mock_db_session.execute = AsyncMock(return_value=mock_result_items)
    mock_db_session.scalar = AsyncMock(return_value=0)

    response = client.get("/api/v1/orders")
    assert response.status_code == 200
    assert "data" in response.json()


def test_customers_batch_endpoint_exists(client, mock_db_session):
    mock_db_session.commit = AsyncMock()
    response = client.post(
        "/api/v1/customers/batch", json=[{"full_name": "Jane", "email": "jane@test.com"}]
    )
    assert response.status_code in (200, 201)
