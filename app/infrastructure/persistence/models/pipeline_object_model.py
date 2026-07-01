from __future__ import annotations

from sqlalchemy import ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class PipelineObjectModel(Base, TimestampMixin):
    """
    Association table: Pipeline <-> DataObject (N:M).

    role_in_pipeline: contextual role - 'source' or 'destination' - is pipeline-specific,
    not an intrinsic property of the DataObject.
    """

    __tablename__ = "pipeline_objects"
    pipeline_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("pipelines.id"), primary_key=True
    )
    object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id"), primary_key=True
    )
    role_in_pipeline: Mapped[str] = mapped_column(
        String(20), nullable=False
    )  # "source" | "destination"
