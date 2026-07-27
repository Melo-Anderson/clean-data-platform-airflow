"""Conditional routing edge for the Harness Engine LangGraph."""

from __future__ import annotations

from typing import Any

from src.config import settings


def routing_edge(state: dict[str, Any]) -> str:
    """Returns 'approved' | 'retry' | 'failed' based on validation state."""
    errors: list[str] = state.get("validation_errors", [])
    iteration: int = state.get("iteration_count", 0)
    max_iter: int = state.get("_max_iterations", settings.max_iterations)

    if not errors:
        return "approved"
    if iteration >= max_iter:
        return "failed"
    return "retry"
