from __future__ import annotations

from fastapi import APIRouter, BackgroundTasks, Depends, status
from pydantic import BaseModel

from app.application.endpoints.provision_endpoint import ProvisionEndpointUseCase
from app.auth.current_user import CurrentUser
from app.auth.dependencies import require_permission
from app.domain.endpoints.endpoint_type import EndpointType
from app.infrastructure.http.audit_helper import write_audit_log_task
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


class NoSqlEndpointCreateRequest(BaseModel):
    name: str
    credential_ref: str
    technical_description: str = ""


@router.post("/database", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_database_endpoint(
    body: DatabaseEndpointCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("catalog:sync")),
) -> EndpointResponse:
    """Provision a DatabaseEndpoint. SRE and PO_PM allowed."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ProvisionEndpointUseCase(uow=uow)

    saved = await use_case.execute_database(
        name=body.name,
        credential_ref=body.credential_ref,
        technical_description=body.technical_description,
    )

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="endpoint.database_created",
        entity_type="Endpoint",
        entity_id=saved.id,
        payload={"name": saved.name, "credential_ref": body.credential_ref},
        description="Database endpoint provisioned manually",
    )

    return EndpointResponse(id=saved.id, name=saved.name, type=saved.type)


@router.post("/nosql", response_model=EndpointResponse, status_code=status.HTTP_201_CREATED)
async def provision_nosql_endpoint(
    body: NoSqlEndpointCreateRequest,
    background_tasks: BackgroundTasks,
    current_user: CurrentUser = Depends(require_permission("catalog:sync")),
) -> EndpointResponse:
    """Provision a NoSqlEndpoint (MongoDB, DocumentDB, etc.). SRE and PO_PM allowed."""
    uow = SqlUnitOfWork(get_session_factory())
    use_case = ProvisionEndpointUseCase(uow=uow)

    saved = await use_case.execute_nosql(
        name=body.name,
        credential_ref=body.credential_ref,
        technical_description=body.technical_description,
    )

    background_tasks.add_task(
        write_audit_log_task,
        actor_id=current_user.id,
        actor_email=str(current_user.email),
        event_type="endpoint.nosql_created",
        entity_type="Endpoint",
        entity_id=saved.id,
        payload={"name": saved.name, "credential_ref": body.credential_ref},
        description="NoSQL endpoint provisioned manually",
    )

    return EndpointResponse(id=saved.id, name=saved.name, type=saved.type)
