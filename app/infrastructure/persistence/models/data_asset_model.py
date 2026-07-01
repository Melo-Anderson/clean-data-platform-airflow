from __future__ import annotations

import uuid

from typing import Any
from sqlalchemy import ForeignKey, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class DataAssetModel(Base, TimestampMixin):
    """
    ORM model for DataAsset. Infrastructure only — no business logic.

    Mapping to/from the domain DataAsset entity is done in SqlAssetRepository.
    discovery_scope stored as JSON dict (serialized from DiscoveryScope Value Object).
    """

    __tablename__ = "data_assets"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    description: Mapped[str] = mapped_column(String(2000), nullable=False)
    owner_email: Mapped[str] = mapped_column(String(255), nullable=False)
    tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    policy_tags: Mapped[list[str]] = mapped_column(JSON, nullable=False, default=list)
    state: Mapped[str] = mapped_column(String(50), nullable=False, default="draft")
    discovery_schedule: Mapped[str] = mapped_column(String(100), nullable=False)
    discovery_scope: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    endpoint_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("endpoints.id", use_alter=True), nullable=True
    )
