"""add_pipeline_run_watermark_columns

Revision ID: a1b2c3d4e5f6
Revises: 96cef173ce8b
Create Date: 2026-10-24 10:57:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1b2c3d4e5f6"
down_revision: str | Sequence[str] | None = "96cef173ce8b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    with op.batch_alter_table("pipeline_runs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("watermark_value", sa.String(length=255), nullable=True))
        batch_op.add_column(sa.Column("watermark_type", sa.String(length=20), nullable=True))


def downgrade() -> None:
    """Downgrade schema."""
    with op.batch_alter_table("pipeline_runs", schema=None) as batch_op:
        batch_op.drop_column("watermark_type")
        batch_op.drop_column("watermark_value")
