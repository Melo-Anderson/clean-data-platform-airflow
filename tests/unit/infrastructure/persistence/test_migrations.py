import os

import pytest
from alembic import command
from alembic.config import Config

from app.config import get_settings


def test_migrations_up_and_down(tmp_path):
    # Save original env
    orig_db_url = os.environ.get("PLATFORM_DATABASE_URL")

    db_path = tmp_path / "test_migrations.db"
    # Overwrite the PLATFORM_DATABASE_URL environment variable to use the sqlite file.
    os.environ["PLATFORM_DATABASE_URL"] = f"sqlite+aiosqlite:///{db_path}"
    get_settings.cache_clear()

    alembic_cfg = Config("alembic.ini")

    try:
        command.upgrade(alembic_cfg, "head")
        command.downgrade(alembic_cfg, "base")
    except Exception as e:
        pytest.fail(f"Migrations failed: {e}")
    finally:
        # Restore original env
        if orig_db_url is not None:
            os.environ["PLATFORM_DATABASE_URL"] = orig_db_url
        else:
            os.environ.pop("PLATFORM_DATABASE_URL", None)
        get_settings.cache_clear()
