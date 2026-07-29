from __future__ import annotations

from app.infrastructure.http.schemas.harness_schemas import (
    HarnessGoldExamplesResponse,
    HarnessSchemaResponse,
    ValidationErrorDetail,
    ValidationRequest,
    ValidationResponse,
)


def test_validation_request_schema():
    req = ValidationRequest(pipeline_yaml="id: test", pipeline_type="relational")
    assert req.pipeline_type == "relational"


def test_validation_response_schema():
    detail = ValidationErrorDetail(
        json_pointer="/", error_code="TEST", message="msg", suggestion="fix"
    )
    res = ValidationResponse(is_valid=False, errors=[detail])
    assert not res.is_valid
    assert res.errors[0].error_code == "TEST"


def test_harness_schema_response():
    res = HarnessSchemaResponse(type="object", properties={"id": {"type": "string"}})
    assert res.type == "object"


def test_harness_gold_examples_response():
    res = HarnessGoldExamplesResponse(examples=[{"description": "desc", "yaml_snippet": "yaml"}])
    assert len(res.examples) == 1
