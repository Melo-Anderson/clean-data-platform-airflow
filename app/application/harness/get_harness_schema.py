from __future__ import annotations

from app.infrastructure.http.schemas.harness_schemas import (
    SCHEMA_FACTORY,
    HarnessSchemaResponse,
)
from app.infrastructure.http.schemas.pipeline_schemas import CreatePipelineRequest


class GetHarnessSchemaUseCase:
    async def execute(
        self, pipeline_type: str = "ingestion", endpoint_type: str = "relational"
    ) -> HarnessSchemaResponse:
        spec_cls = SCHEMA_FACTORY.get((pipeline_type, endpoint_type))
        schema_dict = (
            spec_cls.model_json_schema() if spec_cls else CreatePipelineRequest.model_json_schema()
        )
        schema_dict["$schema"] = "http://json-schema.org/draft-07/schema#"

        return HarnessSchemaResponse(**schema_dict)
