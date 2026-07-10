from __future__ import annotations

from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter(tags=["observability"])

# Versao da API — deve ser mantida em sincronia com app/main.py
_API_VERSION = "1.0.0"


class HealthResponse(BaseModel):
    """Payload retornado pelo endpoint de saude da API."""

    status: str
    version: str


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe — verifica se a API esta respondendo",
    description=(
        "Endpoint leve para Kubernetes liveness probes e monitores externos. "
        "Nao verifica conectividade com banco ou servicos externos. "
        "Retorna 200 enquanto o processo Python estiver rodando."
    ),
)
async def health_check() -> HealthResponse:
    """Return API health status without checking downstream dependencies.

    Safe to use as a Kubernetes liveness probe because it never blocks
    on external I/O (DB, Vault, Airflow).
    """
    return HealthResponse(status="healthy", version=_API_VERSION)
