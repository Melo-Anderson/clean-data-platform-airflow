from __future__ import annotations

from dataclasses import dataclass, field

from app.domain.shared.auditable import Auditable


@dataclass(frozen=True)
class ElementLineage:
    """
    Explicit lineage mapping between a source and destination column.

    transformation_expression: SQL expression or natural language describing
      how source_column is transformed to produce destination_column.
      None means direct copy (no transformation).

    Example:
        ElementLineage(
            source_column="cpf",
            destination_column="document_hash",
            transformation_expression="SHA256(cpf)",
        )
    """

    source_column: str
    destination_column: str
    transformation_expression: str | None = None


@dataclass(kw_only=True)
class LineageMapping(Auditable):
    """
    Explicit column-level lineage between source and destination DataObjects within a Pipeline.

    Auto-populated by emit_raw_lineage task for ingestion (direct column mapping).
    Updated by emit_final_lineage / emit_lineage tasks for ETL/export (with transformations).

    Enables impact analysis: given a destination_column, trace back to source_column
    and all intermediate transformation_expressions across the pipeline chain.

    This entity is intentionally simple and evolvable:
    - Phase 1 (this plan): column mappings declared manually or inferred from schema matching.
    - Phase 2 (future): auto-generated from dbt graph or Dataform lineage API.
    - Phase 3 (future): cross-pipeline lineage graph traversal.
    """

    id: str
    pipeline_id: str
    source_object_id: str
    destination_object_id: str
    column_mappings: list[ElementLineage] = field(default_factory=list)

    def add_mapping(
        self,
        source_column: str,
        destination_column: str,
        transformation_expression: str | None = None,
    ) -> ElementLineage:
        """Add a column-level mapping and return the created ElementLineage."""
        mapping = ElementLineage(
            source_column=source_column,
            destination_column=destination_column,
            transformation_expression=transformation_expression,
        )
        self.column_mappings.append(mapping)
        self.touch()
        return mapping

    def direct_mappings(self) -> list[ElementLineage]:
        """Return only columns with no transformation (direct copy)."""
        return [m for m in self.column_mappings if m.transformation_expression is None]

    def transformed_mappings(self) -> list[ElementLineage]:
        """Return only columns with an explicit transformation expression."""
        return [m for m in self.column_mappings if m.transformation_expression is not None]
