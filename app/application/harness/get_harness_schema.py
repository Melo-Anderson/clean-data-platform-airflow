from __future__ import annotations

from app.infrastructure.http.schemas.harness_schemas import HarnessSchemaResponse


class GetHarnessSchemaUseCase:
    async def execute(self, pipeline_type: str = "all") -> HarnessSchemaResponse:
        return HarnessSchemaResponse(
            type="object",
            properties={
                "pipeline_id": {"type": "string"},
                "type": {"type": "string", "enum": ["ingestion", "etl", "export"]},
                "source_query": {"type": "string"},
            },
        )
