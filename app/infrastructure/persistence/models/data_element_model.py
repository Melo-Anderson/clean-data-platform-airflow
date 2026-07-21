from __future__ import annotations

import uuid

from sqlalchemy import Boolean, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class DataElementModel(Base, TimestampMixin):
    """Normalized persistence for DataElement domain entity."""

    __tablename__ = "data_elements"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    object_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("data_objects.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_type: Mapped[str | None] = mapped_column(String(50), nullable=True)
    destination_type: Mapped[str] = mapped_column(String(50), nullable=False)
    required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    nullable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    is_primary_key: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    policy_tag: Mapped[str | None] = mapped_column(String(100), nullable=True)
    auto_generated: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    is_computed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
