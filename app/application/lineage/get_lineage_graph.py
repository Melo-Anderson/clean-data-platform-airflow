from __future__ import annotations
from typing import Any

from app.application.unit_of_work import UnitOfWork
from app.domain.lineage.lineage_graph import LineageGraph


class GetLineageGraphUseCase:
    """
    Loads all lineage mappings from the database and calculates
    the upstream/downstream paths for a target column.
    """

    def __init__(self, uow: UnitOfWork) -> None:
        self._uow = uow

    async def execute(
        self,
        *,
        object_id: str,
        column_name: str,
        direction: str = "upstream",  # "upstream" | "downstream" | "both"
    ) -> dict[str, list[dict[str, Any]]]:
        async with self._uow as uow:
            # Load only the neighborhood graph for performance
            mappings = await uow.lineage.find_graph_neighborhood(
                object_id=object_id, direction=direction
            )

        graph = LineageGraph()
        graph.build_from_mappings(mappings)

        result: dict[str, list[dict[str, Any]]] = {}

        if direction in ("upstream", "both"):
            upstream_nodes = graph.trace_upstream(object_id, column_name)
            result["upstream"] = [
                {
                    "object_id": n.object_id,
                    "column_name": n.column_name,
                    "transformation": n.transformation,
                }
                for n in upstream_nodes
            ]

        if direction in ("downstream", "both"):
            downstream_nodes = graph.trace_downstream(object_id, column_name)
            result["downstream"] = [
                {
                    "object_id": n.object_id,
                    "column_name": n.column_name,
                    "transformation": n.transformation,
                }
                for n in downstream_nodes
            ]

        return result
