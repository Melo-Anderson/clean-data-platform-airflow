"""
LangGraph state for the Harness Engine.
`messages` uses add_messages reducer for accumulated conversation history across iterations.
All other fields are plain values replaced by direct assignment in each node.
"""

from __future__ import annotations

from typing import Annotated, Any

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages

from src.domain.schemas.pipeline_spec import PipelineSpec


class HarnessState(dict):
    """Typed LangGraph state. dict subclass for LangGraph compatibility."""

    messages: Annotated[list[AnyMessage], add_messages]
    user_prompt: str
    context: dict[str, Any]
    pipeline_spec: PipelineSpec | None
    generated_yaml: str | None
    validation_errors: list[str]
    iteration_count: int
    status: str  # in_progress | approved | failed_max_iterations


def initial_state(user_prompt: str) -> dict[str, Any]:
    """Return a clean initial state dict for graph.invoke()."""
    return {
        "messages": [],
        "user_prompt": user_prompt,
        "context": {},
        "pipeline_spec": None,
        "generated_yaml": None,
        "validation_errors": [],
        "iteration_count": 0,
        "status": "in_progress",
    }
