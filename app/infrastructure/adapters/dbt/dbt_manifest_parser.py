from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class DbtColumnMetadata:
    name: str
    data_type: str
    description: str = ""


@dataclass(frozen=True)
class DbtNodeMetadata:
    unique_id: str
    name: str
    resource_type: str
    schema: str
    description: str = ""
    depends_on_nodes: list[str] = field(default_factory=list)
    columns: list[DbtColumnMetadata] = field(default_factory=list)


@dataclass(frozen=True)
class DbtParsedManifest:
    models: list[DbtNodeMetadata]
    sources: list[DbtNodeMetadata]
    lineage: dict[str, list[str]]


class DbtManifestParser:
    """Parses dbt compiled manifest.json extracting models, sources, columns, and lineage."""

    def parse_file(self, manifest_path: Path | str) -> DbtParsedManifest:
        path = Path(manifest_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return self.parse_dict(data)

    def parse_dict(self, data: dict[str, Any]) -> DbtParsedManifest:
        models = self._extract_nodes(data.get("nodes", {}), target_resource_type="model")
        sources = self._extract_nodes(data.get("sources", {}), target_resource_type="source")
        lineage = self._extract_lineage(data.get("nodes", {}))
        return DbtParsedManifest(models=models, sources=sources, lineage=lineage)

    def _extract_nodes(
        self, nodes_dict: dict[str, Any], target_resource_type: str
    ) -> list[DbtNodeMetadata]:
        result: list[DbtNodeMetadata] = []
        for unique_id, node_info in nodes_dict.items():
            if node_info.get("resource_type") != target_resource_type:
                continue
            cols = self._extract_columns(node_info.get("columns", {}))
            depends_on = node_info.get("depends_on", {}).get("nodes", [])
            result.append(
                DbtNodeMetadata(
                    unique_id=unique_id,
                    name=node_info.get("name", ""),
                    resource_type=target_resource_type,
                    schema=node_info.get("schema", ""),
                    description=node_info.get("description", ""),
                    depends_on_nodes=depends_on,
                    columns=cols,
                )
            )
        return result

    def _extract_columns(self, cols_dict: dict[str, Any]) -> list[DbtColumnMetadata]:
        columns: list[DbtColumnMetadata] = []
        for col_name, col_info in cols_dict.items():
            columns.append(
                DbtColumnMetadata(
                    name=col_name,
                    data_type=col_info.get("data_type") or "STRING",
                    description=col_info.get("description", ""),
                )
            )
        return columns

    def _extract_lineage(self, nodes_dict: dict[str, Any]) -> dict[str, list[str]]:
        lineage: dict[str, list[str]] = {}
        for unique_id, node_info in nodes_dict.items():
            if node_info.get("resource_type") == "model":
                parents = node_info.get("depends_on", {}).get("nodes", [])
                lineage[unique_id] = parents
        return lineage
