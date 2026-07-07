from __future__ import annotations

import uuid

from sqlalchemy import JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class PipelineModel(Base, TimestampMixin):
    """ORM for Pipeline."""

    __tablename__ = "pipelines"
    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    type: Mapped[str] = mapped_column(String(50), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_version: Mapped[str] = mapped_column(String(20), nullable=False)
    source_asset_id: Mapped[str] = mapped_column(String(36), nullable=False, default="")
    destination_asset_id: Mapped[str] = mapped_column(String(36), nullable=False, default="")

    # Store configs as JSON for flexibility, as they are value objects
    schedule: Mapped[dict] = mapped_column(JSON, nullable=False)
    source_objects: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    destination_objects: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    transform: Mapped[dict] = mapped_column(JSON, nullable=False)
    compute: Mapped[dict] = mapped_column(JSON, nullable=False)
    quality_rules: Mapped[list] = mapped_column(JSON, nullable=False, default=list)
    airflow: Mapped[dict] = mapped_column(JSON, nullable=False)
    discovery_task: Mapped[dict] = mapped_column(JSON, nullable=False)
