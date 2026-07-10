import asyncio

from app.infrastructure.persistence.base_model import Base
from app.infrastructure.persistence.database import _engine

# Import all models so SQLAlchemy knows about them before create_all


async def init_db() -> None:
    print("Creating database tables...")
    async with _engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Tables created successfully!")


if __name__ == "__main__":
    asyncio.run(init_db())
