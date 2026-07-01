from __future__ import annotations

import uuid
from typing import Any

from sqlalchemy import JSON, ForeignKey, String
from sqlalchemy.orm import Mapped, mapped_column

from app.infrastructure.persistence.base_model import Base, TimestampMixin


class EndpointModel(Base, TimestampMixin):
    """
    ORM model for all Endpoint subtypes.

    Uses single-table storage: `type` discriminates the subclass,
    `subtype_data` (JSON) stores the subtype-specific typed fields.
    This keeps the ORM schema simple while the domain uses typed subclasses.
    Repository handles domain ↔ ORM mapping.
    """

    __tablename__ = "endpoints"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    asset_id: Mapped[str] = mapped_column(String(36), ForeignKey("data_assets.id"), nullable=False)
    type: Mapped[str] = mapped_column(String(50), nullable=False)  # EndpointType value
    credential_ref: Mapped[str] = mapped_column(String(500), nullable=False)
    technical_description: Mapped[str] = mapped_column(String(2000), nullable=False, default="")
    # Stores typed subclass fields: host, port, base_url, bucket, etc.
    subtype_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
