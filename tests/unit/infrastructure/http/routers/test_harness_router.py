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
