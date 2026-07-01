from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_role
from app.auth.role import Role
from app.domain.endpoints.endpoint import (
    DatabaseEndpoint,
)
from app.domain.endpoints.endpoint_service import EndpointService
from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.persistence.database import get_db
from app.infrastructure.persistence.repositories.sql_endpoint_repository import (
    SqlEndpointRepository,
)

router = APIRouter()


class EndpointResponse(BaseModel):
    """HTTP response: only id and type are visible. Sensitive fields never exposed."""

    id: str
    asset_id: str
    type: EndpointType


class DatabaseEndpointCreateRequest(BaseModel):
    asset_id: str
    credential_ref: str
    technical_description: str = ""
    host: str
    port: int
    database: str
    driver: str


@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    session: AsyncSession = Depends(get_db),
    _: CurrentUser = Depends(require_role(Role.SRE)),
) -> EndpointResponse:
    """Provision a DatabaseEndpoint. SRE only."""
    service = EndpointService(repo=SqlEndpointRepository(session))
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()),
        asset_id=body.asset_id,
        credential_ref=CredentialReference(body.credential_ref),
        technical_description=body.technical_description,
        host=body.host,
        port=body.port,
        database=body.database,
        driver=body.driver,
    )
    saved = await service.provision(ep)
    return EndpointResponse(id=saved.id, asset_id=saved.asset_id, type=saved.type)
