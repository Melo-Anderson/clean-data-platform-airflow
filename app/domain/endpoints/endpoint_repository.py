from __future__ import annotations

from typing import Protocol, runtime_checkable

from app.domain.endpoints.endpoint import AnyEndpoint


@runtime_checkable
class EndpointRepository(Protocol):
    """Repository interface for Endpoint persistence. Implemented in infrastructure."""

    async def save(self, endpoint: AnyEndpoint) -> AnyEndpoint: ...

    async def find_by_id(self, endpoint_id: str) -> AnyEndpoint | None: ...

    async def find_by_asset_id(self, asset_id: str) -> AnyEndpoint | None: ...
