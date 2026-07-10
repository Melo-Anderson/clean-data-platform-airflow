from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from app.application.lineage.get_lineage_graph import GetLineageGraphUseCase
from app.infrastructure.http.schemas.lineage_schemas import LineageGraphResponse, LineageNodeSchema
from app.infrastructure.persistence.database import get_session_factory
from app.infrastructure.persistence.sql_unit_of_work import SqlUnitOfWork

router = APIRouter(prefix="/lineage", tags=["Lineage"])


@router.get(
    "/trace",
    response_model=LineageGraphResponse,
    summary="Get column-level lineage (upstream/downstream/both)",
)
async def trace_lineage(
    object_id: str = Query(..., description="The DataObject ID to trace"),
    column_name: str = Query(..., description="The column name to trace"),
    direction: str = Query(
        "upstream", description="Direction to trace: upstream | downstream | both"
    ),
) -> LineageGraphResponse:
    """
    Trace column-level lineage. Returns nodes upstream (provenance)
    and/or downstream (impact analysis).
    """
    if direction not in ("upstream", "downstream", "both"):
        raise HTTPException(
            status_code=400, detail="Invalid direction. Choose 'upstream', 'downstream', or 'both'"
        )

    uow = SqlUnitOfWork(get_session_factory())
    use_case = GetLineageGraphUseCase(uow=uow)

    result = await use_case.execute(
        object_id=object_id,
        column_name=column_name,
        direction=direction,
    )
    return LineageGraphResponse(
        upstream=[LineageNodeSchema(**node) for node in result.get("upstream", [])],
        downstream=[LineageNodeSchema(**node) for node in result.get("downstream", [])],
    )
