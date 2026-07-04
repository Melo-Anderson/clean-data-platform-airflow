from __future__ import annotations

import uuid

from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class CatalogLineageModel(Base, TimestampMixin):
    """
    Represents a lineage edge between source and destination DataObjects.

    Rows are upserted: if an edge for (pipeline_id, source, destination) already
    exists, only column_mappings is updated; no duplicate edges are created.
    """

    __tablename__ = "catalog_lineages"

    id: Mapped[str] = mapped_column(
        String(36), primary_key=True, default=lambda: str(uuid.uuid4())
    )
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id"), nullable=False
    )
    source_object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), nullable=False
    )
    destination_object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), nullable=False
    )
    # Stores [{source_column, destination_column, expression}]
    column_mappings: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
