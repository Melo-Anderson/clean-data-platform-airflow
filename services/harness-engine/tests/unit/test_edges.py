"""Tests for routing_edge."""

from src.application.graph.edges import routing_edge


def _s(errors: list[str], iteration: int, max_iter: int = 3) -> dict:  # type: ignore[type-arg]
    return {"validation_errors": errors, "iteration_count": iteration, "_max_iterations": max_iter}


def test_approved_when_no_errors() -> None:
    assert routing_edge(_s([], 1)) == "approved"


def test_retry_when_errors_under_limit() -> None:
    assert routing_edge(_s(["error"], 1)) == "retry"


def test_failed_at_max_iterations() -> None:
    assert routing_edge(_s(["error"], 3)) == "failed"


def test_failed_when_exceeds_max() -> None:
    assert routing_edge(_s(["e"], 4)) == "failed"


def test_approved_zero_errors_at_max() -> None:
    assert routing_edge(_s([], 3)) == "approved"
