from __future__ import annotations

from app.infrastructure.http.schemas.harness_schemas import HarnessGoldExamplesResponse


class GetHarnessGoldExamplesUseCase:
    async def execute(self, pipeline_type: str = "all") -> HarnessGoldExamplesResponse:
        return HarnessGoldExamplesResponse(
            examples=[
                {
                    "description": "Standard Ingestion Pipeline",
                    "yaml_snippet": "pipeline_id: raw_orders\ntype: ingestion\nsource_query: SELECT * FROM orders",
                }
            ]
        )
