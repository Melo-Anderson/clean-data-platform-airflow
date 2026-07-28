"""
LangGraph StateGraph compilation.

Topology:
    START -> context_node -> generator_node -> guardrail_node -> routing_edge
                                 ^                                    |
                                 +--------(retry)--------------------+
                                 approved -> approved_node -> END
                                 failed  -> failed_node   -> END
"""

from __future__ import annotations

from typing import Any

from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.graph import END, START, StateGraph

from src.application.graph.edges import routing_edge
from src.application.graph.nodes.context_node import make_context_node
from src.application.graph.nodes.generator_node import make_generator_node
from src.application.graph.nodes.guardrail_node import guardrail_node
from src.application.graph.state import HarnessState
from src.domain.ports import MetadataPort, MetricsPort, PlatformExamplesPort, PlatformSchemaPort


def build_graph(
    metadata_port: MetadataPort,
    metrics_port: MetricsPort,
    schema_port: PlatformSchemaPort | None = None,
    examples_port: PlatformExamplesPort | None = None,
    llm: BaseChatModel | None = None,
) -> Any:
    """Compile and return the Harness Engine LangGraph."""
    graph = StateGraph(HarnessState)
    graph.add_node(
        "context_node", make_context_node(metadata_port, metrics_port, schema_port, examples_port)
    )
    graph.add_node("generator_node", make_generator_node(llm=llm))
    graph.add_node("guardrail_node", guardrail_node)

    def set_approved(state: dict[str, Any]) -> dict[str, Any]:
        return {"status": "approved"}

    def set_failed(state: dict[str, Any]) -> dict[str, Any]:
        return {"status": "failed_max_iterations"}

    graph.add_node("approved_node", set_approved)
    graph.add_node("failed_node", set_failed)

    graph.add_edge(START, "context_node")
    graph.add_edge("context_node", "generator_node")
    graph.add_edge("generator_node", "guardrail_node")
    graph.add_conditional_edges(
        "guardrail_node",
        routing_edge,
        {"approved": "approved_node", "retry": "generator_node", "failed": "failed_node"},
    )
    graph.add_edge("approved_node", END)
    graph.add_edge("failed_node", END)

    return graph.compile()
