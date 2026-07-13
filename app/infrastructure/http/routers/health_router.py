from __future__ import annotations

from fastapi import APIRouter, Depends
import httpx
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import get_settings
from app.infrastructure.persistence.database import get_db

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


class ReadyResponse(BaseModel):
    """Payload for the readiness probe."""

    status: str
    components: dict[str, str]


@router.get(
    "/health/ready",
    response_model=ReadyResponse,
    summary="Readiness probe — verifies dependencies",
)
async def health_ready(db: AsyncSession = Depends(get_db)) -> ReadyResponse:
    """Check if the API is ready to handle traffic by verifying critical dependencies."""
    components: dict[str, str] = {}
    is_ready = True

    # Check Database
    try:
        await db.execute(text("SELECT 1"))
        components["database"] = "up"
    except Exception:
        components["database"] = "down"
        is_ready = False

    # Check Vault
    settings = get_settings()
    if not settings.vault_url:
        components["vault"] = "not_configured"
    else:
        try:
            async with httpx.AsyncClient(timeout=2.0) as client:
                res = await client.get(f"{settings.vault_url.rstrip('/')}/v1/sys/health")
                components["vault"] = "up" if res.status_code == 200 else "down"
                if res.status_code != 200:
                    is_ready = False
        except Exception:
            components["vault"] = "down"
            is_ready = False

    return ReadyResponse(
        status="ready" if is_ready else "unready",
        components=components,
    )
