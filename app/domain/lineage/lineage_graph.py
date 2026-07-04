from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.lineage.lineage_mapping import LineageMapping


@dataclass(kw_only=True)
class LineageNode:
    """
    Represents a lineage graph node (a column of a specific object).
    Key format: "object_id.column_name"
    """
    object_id: str
    column_name: str
    transformation: str | None = None

    @property
    def key(self) -> str:
        return f"{self.object_id}.{self.column_name}"


@dataclass(kw_only=True)
class LineageGraph:
    """
    Directed Acyclic Graph (DAG) representing the column lineage in the platform.
    Enables downstream (impact) and upstream (provenance) traversal.
    """

    nodes: dict[str, LineageNode] = field(default_factory=dict)
    adjacency: dict[str, set[str]] = field(default_factory=dict)       # key -> set of target keys (downstream)
    incoming: dict[str, set[str]] = field(default_factory=dict)        # key -> set of source keys (upstream)

    def add_node(self, node: LineageNode) -> None:
        if node.key not in self.nodes:
            self.nodes[node.key] = node
            self.adjacency[node.key] = set()
            self.incoming[node.key] = set()

    def add_edge(self, source: LineageNode, target: LineageNode) -> None:
        self.add_node(source)
        self.add_node(target)
        self.adjacency[source.key].add(target.key)
        self.incoming[target.key].add(source.key)

    def build_from_mappings(self, mappings: list[LineageMapping]) -> None:
        """Populates the graph from a collection of database lineage mappings."""
        for mapping in mappings:
            for col_map in mapping.column_mappings:
                source = LineageNode(
                    object_id=mapping.source_object_id,
                    column_name=col_map.source_column,
                )
                target = LineageNode(
                    object_id=mapping.destination_object_id,
                    column_name=col_map.destination_column,
                    transformation=col_map.transformation_expression,
                )
                self.add_edge(source, target)

    def trace_upstream(self, object_id: str, column_name: str) -> list[LineageNode]:
        """Returns all upstream nodes from which the given column is derived (Depth-First Search)."""
        start_key = f"{object_id}.{column_name}"
        if start_key not in self.nodes:
            return []

        visited = set()
        stack = [start_key]
        result = []

        while stack:
            curr = stack.pop()
            if curr not in visited:
                visited.add(curr)
                if curr != start_key:
                    result.append(self.nodes[curr])
                stack.extend(self.incoming[curr])
        return result

    def trace_downstream(self, object_id: str, column_name: str) -> list[LineageNode]:
        """Returns all downstream nodes impacted by the given column (Depth-First Search)."""
        start_key = f"{object_id}.{column_name}"
        if start_key not in self.nodes:
            return []

        visited = set()
        stack = [start_key]
        result = []

        while stack:
            curr = stack.pop()
            if curr not in visited:
                visited.add(curr)
                if curr != start_key:
                    result.append(self.nodes[curr])
                stack.extend(self.adjacency[curr])
        return result
