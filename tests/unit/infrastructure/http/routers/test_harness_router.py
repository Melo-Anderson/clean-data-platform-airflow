from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.infrastructure.http.routers.harness_router import router

app = FastAPI()
app.include_router(router)
client = TestClient(app)


def test_validate_endpoint_success() -> None:
    payload = {"pipeline_yaml": "pipeline_id: valid", "pipeline_type": "relational"}
    response = client.post("/v1/harness/validate", json=payload)
    assert response.status_code == 200
    assert response.json()["is_valid"] is True


def test_get_schema() -> None:
    response = client.get("/v1/harness/schema?type=relational")
    assert response.status_code == 200
    assert "type" in response.json()


def test_get_gold_examples() -> None:
    response = client.get("/v1/harness/gold-examples?type=relational")
    assert response.status_code == 200
    assert "examples" in response.json()
