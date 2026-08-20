from __future__ import annotations

from typing import Any

from app.application.shared.ports.schema_provider_port import SchemaProviderPort


class GetHarnessSchemaUseCase:
    def __init__(self, schema_provider: SchemaProviderPort | None = None) -> None:
        self._schema_provider = schema_provider

    async def execute(
        self, pipeline_type: str = "ingestion", endpoint_type: str = "relational"
    ) -> dict[str, Any]:
        if self._schema_provider is None:
            raise RuntimeError("SchemaProviderPort is required to get schema")
        return await self._schema_provider.get_json_schema(pipeline_type, endpoint_type)
