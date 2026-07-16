import os

import pytest
from alembic import command
from alembic.config import Config


def test_migrations_up_and_down(tmp_path):
    db_path = tmp_path / "test_migrations.db"
    # Overwrite the DATABASE_URL environment variable to use the sqlite file.
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"

    alembic_cfg = Config("alembic.ini")

    try:
        command.upgrade(alembic_cfg, "head")
        command.downgrade(alembic_cfg, "base")
    except Exception as e:
        pytest.fail(f"Migrations failed: {e}")
