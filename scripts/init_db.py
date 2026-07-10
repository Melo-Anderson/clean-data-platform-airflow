import asyncio
from app.infrastructure.persistence.database import _engine
from app.infrastructure.persistence.base_model import Base

# Import all models so SQLAlchemy knows about them before create_all
from app.infrastructure.persistence.models import (
    data_object_model,
    discovery_run_model,
    lineage_mapping_model,
    pipeline_model,
    pipeline_run_model,
)


async def init_db() -> None:
    print("Creating database tables...")
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created successfully!")


if __name__ == "__main__":
    asyncio.run(init_db())
