from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.endpoints.endpoint import (
    AnyEndpoint,
    CloudBucketEndpoint,
    DatabaseEndpoint,
    EtlFlowEndpoint,
    RestApiEndpoint,
    SftpEndpoint,
)
from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.persistence.models.endpoint_model import EndpointModel

_BASE_FIELDS = {
    "id",
    "name",
    "credential_ref",
    "technical_description",
    "type",
    "created_at",
    "updated_at",
}


from typing import Any


def _to_domain(m: EndpointModel) -> AnyEndpoint:
    """Map ORM model → typed domain Endpoint subclass. No business logic."""
    base: dict[str, Any] = {
        "id": m.id,
        "name": m.name,
        "credential_ref": CredentialReference(m.credential_ref),
        "technical_description": m.technical_description,
        "created_at": m.created_at,
        "updated_at": m.updated_at,
        **m.subtype_data,
    }
    match EndpointType(m.type):
        case EndpointType.DATABASE:
            return DatabaseEndpoint(**base)
        case EndpointType.REST_API:
            return RestApiEndpoint(**base)
        case EndpointType.SFTP:
            return SftpEndpoint(**base)
        case EndpointType.CLOUD_BUCKET:
            return CloudBucketEndpoint(**base)
        case EndpointType.ETL_FLOW:
            return EtlFlowEndpoint(**base)
        case _:
            raise ValueError(f"Unknown EndpointType: {m.type!r}")


def _to_model(endpoint: AnyEndpoint) -> EndpointModel:
    """Map typed domain Endpoint → ORM model. Separates base fields from subtype_data."""
    all_fields = {
        k: v for k, v in vars(endpoint).items() if k not in _BASE_FIELDS and not k.startswith("_")
    }
    return EndpointModel(
        id=endpoint.id,
        name=endpoint.name,
        type=endpoint.type.value,
        credential_ref=endpoint.credential_ref.path,
        technical_description=endpoint.technical_description,
        subtype_data=all_fields,
    )


class SqlEndpointRepository:
    """SQLAlchemy implementation of EndpointRepository. Infrastructure only."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def save(self, endpoint: AnyEndpoint) -> AnyEndpoint:
        model = _to_model(endpoint)
        self._session.add(model)
        await self._session.flush()
        await self._session.refresh(model)
        return _to_domain(model)

    async def find_by_id(self, endpoint_id: str) -> AnyEndpoint | None:
        result = await self._session.execute(
            select(EndpointModel).where(EndpointModel.id == endpoint_id)
        )
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None

    async def find_by_name(self, name: str) -> AnyEndpoint | None:
        result = await self._session.execute(
            select(EndpointModel).where(EndpointModel.name == name)
        )
        model = result.scalar_one_or_none()
        return _to_domain(model) if model else None
