from __future__ import annotations

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel

from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_role
from app.auth.role import Role
from app.domain.endpoints.endpoint_type import EndpointType
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter()


class EndpointResponse(BaseModel):
    """HTTP response: only id and type are visible. Sensitive fields never exposed."""

    id: str
    name: str
    type: EndpointType


class DatabaseEndpointCreateRequest(BaseModel):
    name: str
    credential_ref: str
    technical_description: str = ""


@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    _: CurrentUser = Depends(require_role(Role.SRE, Role.PO_PM)),
) -> EndpointResponse:
    """Provision a DatabaseEndpoint. SRE and PO_PM allowed."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ProvisionEndpointUseCase(uow=uow)

    saved = await use_case.execute_database(
        name=body.name,
        credential_ref=body.credential_ref,
        technical_description=body.technical_description,
    )
    return EndpointResponse(id=saved.id, name=saved.name, type=saved.type)
