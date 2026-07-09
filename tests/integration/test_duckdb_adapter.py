import os
import pytest
import duckdb
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy import text

# Defaults to localhost mappings since Docker is run manually by user
PLATFORM_DATABASE_URL = os.getenv("PLATFORM_DATABASE_URL", "postgresql+asyncpg://airflow:airflow@localhost:5432/platform_db")

@pytest.fixture(scope="module")
async def setup_postgres_table():
    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async with engine.begin() as conn:
        await conn.execute(text("DROP TABLE IF EXISTS e2e_source_table;"))
        await conn.execute(text("""
            CREATE TABLE e2e_source_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """))
        await conn.execute(text("INSERT INTO e2e_source_table (name) VALUES ('Test 1'), ('Test 2'), ('Test 3');"))
    await engine.dispose()
    yield
