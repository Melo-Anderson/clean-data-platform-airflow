"""FastAPI REST and SSE streaming endpoints."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

from fastapi import APIRouter
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse

from src.application.graph.state import initial_state
from src.application.graph.workflow import build_graph
from src.config import settings
from src.infrastructure.adapters.db_schema_reader import DbSchemaReader
from src.infrastructure.adapters.http_platform_reader import HttpPlatformReader
from src.infrastructure.adapters.storage_metrics_reader import StorageMetricsReader
from src.infrastructure.llm_factory import get_llm

router = APIRouter()

_platform_reader = HttpPlatformReader(
    schema_url=settings.platform_schema_url,
    examples_url=settings.platform_examples_url,
)

_graph = build_graph(
    metadata_port=DbSchemaReader(db_url=settings.platform_db_url),  # type: ignore[arg-type]
    metrics_port=StorageMetricsReader(base_path=settings.metrics_storage_path),
    schema_port=_platform_reader,
    examples_port=_platform_reader,
    llm=get_llm(),
)


class GenerateRequest(BaseModel):
    prompt: str


class GenerateResponse(BaseModel):
    status: str
    generated_yaml: str | None
    validation_errors: list[str]
    iteration_count: int


@router.post("/generate-yaml", response_model=GenerateResponse)
async def generate_yaml_sync(req: GenerateRequest) -> GenerateResponse:
    """Sync endpoint. Platform gateway must set HTTP timeout >= 60s."""
    result: dict[str, Any] = await asyncio.to_thread(_graph.invoke, initial_state(req.prompt))
    return GenerateResponse(
        status=result.get("status", "unknown"),
        generated_yaml=result.get("generated_yaml"),
        validation_errors=result.get("validation_errors", []),
        iteration_count=result.get("iteration_count", 0),
    )


@router.post("/generate-yaml/stream")
async def generate_yaml_stream(req: GenerateRequest) -> EventSourceResponse:
    """SSE streaming endpoint. Emits node_started, guardrail_evaluated, completed events."""

    async def events() -> AsyncIterator[dict[str, str]]:
        async for ev in _graph.astream_events(initial_state(req.prompt), version="v2"):
            name = ev.get("name", "")
            etype = ev.get("event", "")
            if etype == "on_chain_start" and name in (
                "context_node",
                "generator_node",
                "guardrail_node",
            ):
                yield {"event": "node_started", "data": json.dumps({"node": name})}
            elif etype == "on_chain_end" and name == "guardrail_node":
                out = ev.get("data", {}).get("output", {})
                yield {
                    "event": "guardrail_evaluated",
                    "data": json.dumps(
                        {
                            "validation_errors": out.get("validation_errors", []),
                            "iteration_count": out.get("iteration_count", 0),
                        }
                    ),
                }
            elif etype == "on_chain_end" and name in ("approved_node", "failed_node"):
                out = ev.get("data", {}).get("output", {})
                yield {
                    "event": "completed",
                    "data": json.dumps({"status": out.get("status", "unknown")}),
                }

    return EventSourceResponse(
        events(),
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
