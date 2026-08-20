from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class SchemaProviderPort(Protocol):
    """Port for providing JSON Schemas for pipeline definitions."""

    async def get_json_schema(self, pipeline_type: str, endpoint_type: str) -> dict[str, Any]: ...
