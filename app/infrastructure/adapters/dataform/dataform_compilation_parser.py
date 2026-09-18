from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from app.domain.objects.element_type import ElementType


def map_dataform_type_to_element_type(raw_type: str) -> ElementType:
    """Maps BigQuery/Dataform types to platform ElementType.

    ElementType values available: STRING, INTEGER, BIGINT, FLOAT, DECIMAL,
    DATE, TIMESTAMP, BOOLEAN, BYTES, JSON.
    RECORD/STRUCT is mapped to JSON (closest structural equivalent).
    """
    normalized = raw_type.upper()
    if any(t in normalized for t in ("INT64", "INTEGER", "SMALLINT")):
        return ElementType.INTEGER
    if "BIGINT" in normalized:
        return ElementType.BIGINT
    if any(t in normalized for t in ("NUMERIC", "BIGNUMERIC", "DECIMAL")):
        return ElementType.DECIMAL
    if "FLOAT" in normalized:
        return ElementType.FLOAT
    if any(t in normalized for t in ("TIMESTAMP", "DATETIME")):
        return ElementType.TIMESTAMP
    if "DATE" in normalized:
        return ElementType.DATE
    if any(t in normalized for t in ("BOOL", "BOOLEAN")):
        return ElementType.BOOLEAN
    if "BYTES" in normalized:
        return ElementType.BYTES
    if any(t in normalized for t in ("RECORD", "STRUCT", "JSON")):
        return ElementType.JSON
    return ElementType.STRING


@dataclass(frozen=True)
class DataformColumnMetadata:
    name: str
    data_type: str
    description: str = ""


@dataclass(frozen=True)
class DataformTableMetadata:
    name: str
    schema: str
    type: str
    description: str = ""
    dependency_targets: list[str] = field(default_factory=list)
    columns: list[DataformColumnMetadata] = field(default_factory=list)


@dataclass(frozen=True)
class DataformAssertionMetadata:
    name: str
    schema: str
    description: str = ""
    dependency_targets: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DataformParsedMetadata:
    tables: list[DataformTableMetadata]
    declarations: list[DataformTableMetadata]
    assertions: list[DataformAssertionMetadata]
    lineage: dict[str, list[str]]


class DataformCompilationParser:
    """Parses Dataform compilation JSON output into structured metadata."""

    def parse_file(self, compilation_path: Path | str) -> DataformParsedMetadata:
        path = Path(compilation_path)
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return self.parse_dict(data)

    def parse_dict(self, data: dict[str, Any]) -> DataformParsedMetadata:
        tables = self._extract_tables(data.get("tables", []))
        declarations = self._extract_declarations(data.get("declarations", []))
        assertions = self._extract_assertions(data.get("assertions", []))
        lineage = self._extract_lineage(tables)
        return DataformParsedMetadata(
            tables=tables,
            declarations=declarations,
            assertions=assertions,
            lineage=lineage,
        )

    def _extract_tables(self, raw_tables: list[dict[str, Any]]) -> list[DataformTableMetadata]:
        results: list[DataformTableMetadata] = []
        for item in raw_tables:
            target = item.get("target", {})
            name = target.get("name", "")
            schema = target.get("schema", "")
            table_type = item.get("type", "table")
            action_desc = item.get("actionDescriptor", {})
            description = action_desc.get("description", item.get("description", ""))
            columns = self._extract_columns(action_desc.get("columns", item.get("columns", [])))
            deps = [
                d.get("name", "")
                for d in item.get("dependencyTargets", [])
                if isinstance(d, dict) and d.get("name")
            ]
            results.append(
                DataformTableMetadata(
                    name=name,
                    schema=schema,
                    type=table_type,
                    description=description,
                    dependency_targets=deps,
                    columns=columns,
                )
            )
        return results

    def _extract_declarations(
        self, raw_declarations: list[dict[str, Any]]
    ) -> list[DataformTableMetadata]:
        results: list[DataformTableMetadata] = []
        for item in raw_declarations:
            target = item.get("target", {})
            name = target.get("name", "")
            schema = target.get("schema", "")
            action_desc = item.get("actionDescriptor", {})
            description = action_desc.get("description", item.get("description", ""))
            columns = self._extract_columns(action_desc.get("columns", item.get("columns", [])))
            results.append(
                DataformTableMetadata(
                    name=name,
                    schema=schema,
                    type="declaration",
                    description=description,
                    dependency_targets=[],
                    columns=columns,
                )
            )
        return results

    def _extract_assertions(
        self, raw_assertions: list[dict[str, Any]]
    ) -> list[DataformAssertionMetadata]:
        results: list[DataformAssertionMetadata] = []
        for item in raw_assertions:
            target = item.get("target", {})
            name = target.get("name", "")
            schema = target.get("schema", "")
            action_desc = item.get("actionDescriptor", {})
            description = action_desc.get("description", item.get("description", ""))
            deps = [
                d.get("name", "")
                for d in item.get("dependencyTargets", [])
                if isinstance(d, dict) and d.get("name")
            ]
            results.append(
                DataformAssertionMetadata(
                    name=name,
                    schema=schema,
                    description=description,
                    dependency_targets=deps,
                )
            )
        return results

    def _extract_columns(self, raw_columns: Any) -> list[DataformColumnMetadata]:
        columns: list[DataformColumnMetadata] = []
        if isinstance(raw_columns, list):
            for col in raw_columns:
                if isinstance(col, dict):
                    name = (
                        col.get("path", [""])[0]
                        if isinstance(col.get("path"), list) and col.get("path")
                        else col.get("name", "")
                    )
                    data_type = col.get("type", "STRING")
                    description = col.get("description", "")
                    columns.append(
                        DataformColumnMetadata(
                            name=name, data_type=data_type, description=description
                        )
                    )
        elif isinstance(raw_columns, dict):
            for col_name, col_info in raw_columns.items():
                if isinstance(col_info, dict):
                    columns.append(
                        DataformColumnMetadata(
                            name=col_name,
                            data_type=col_info.get("type", "STRING"),
                            description=col_info.get("description", ""),
                        )
                    )
        return columns

    def _extract_lineage(self, tables: list[DataformTableMetadata]) -> dict[str, list[str]]:
        return {t.name: t.dependency_targets for t in tables if t.dependency_targets}
