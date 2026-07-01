from __future__ import annotations

import uuid

from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class LineageMappingModel(Base, TimestampMixin):
    """ORM for LineageMapping. column_mappings stored as JSON array."""

    __tablename__ = "lineage_mappings"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    pipeline_id: Mapped[str] = mapped_column(String(36), ForeignKey("pipelines.id"), nullable=False)
    source_object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), nullable=False
    )
    destination_object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), nullable=False
    )
    column_mappings: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
