from fastapi import APIRouter
from fastapi.responses import PlainTextResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

router = APIRouter(tags=["Metrics"])


@router.get("/metrics", response_class=PlainTextResponse)
async def metrics() -> str | bytes:
    """Exposes Prometheus metrics exposition endpoint."""
    response = PlainTextResponse(generate_latest())
    response.headers["Content-Type"] = CONTENT_TYPE_LATEST
    return response
