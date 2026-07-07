from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import Boolean, DateTime, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class DataObjectModel(Base, TimestampMixin):
    """ORM model for DataObject. No pipeline_id, no role - relationship managed by PipelineObjectModel."""

    __tablename__ = "data_objects"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_assets.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    policy_tags_json: Mapped[str] = mapped_column(String(2000), nullable=False, default="[]")  # JSON string
    last_run: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_success: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    freshness_status: Mapped[str] = mapped_column(String(20), nullable=False, default="unknown")
    auto_generated_description: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    elements: Mapped[list["DataElementModel"]] = relationship(
        "DataElementModel", cascade="all, delete-orphan", lazy="selectin"
    )
