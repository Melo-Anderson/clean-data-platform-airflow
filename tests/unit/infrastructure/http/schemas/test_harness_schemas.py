from app.infrastructure.http.schemas.harness_schemas import ValidationRequest, ValidationResponse


def test_validation_request() -> None:
    req = ValidationRequest(pipeline_yaml="pipeline_id: test", pipeline_type="relational")
    assert req.pipeline_type == "relational"


def test_validation_response() -> None:
    resp = ValidationResponse(
        is_valid=False,
        errors=[{"json_pointer": "/a", "error_code": "E1", "message": "msg", "suggestion": "sug"}],
    )
    assert not resp.is_valid
    assert resp.errors[0].error_code == "E1"
