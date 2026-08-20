from __future__ import annotations

from typing import Any

from app.application.shared.ports.schema_provider_port import SchemaProviderPort
from app.infrastructure.http.schemas.harness_schemas import SCHEMA_FACTORY
from app.infrastructure.http.schemas.pipeline_schemas import CreatePipelineRequest


class PydanticSchemaProvider(SchemaProviderPort):
    """Infrastructure provider that resolves JSON Schema from Pydantic models."""

    async def get_json_schema(self, pipeline_type: str, endpoint_type: str) -> dict[str, Any]:
        spec_cls = SCHEMA_FACTORY.get((pipeline_type, endpoint_type))
        schema_dict = (
            spec_cls.model_json_schema() if spec_cls else CreatePipelineRequest.model_json_schema()
        )
        schema_dict["$schema"] = "http://json-schema.org/draft-07/schema#"
        return schema_dict
