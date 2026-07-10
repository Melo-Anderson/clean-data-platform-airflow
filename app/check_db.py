import asyncio
from app.infrastructure.persistence.database import get_session_factory
from sqlalchemy import text


async def main():
    f = get_session_factory()
    async with f() as s:
        pipelines = (await s.execute(text("select name from pipelines"))).all()
        print("PIPELINES:", pipelines)
        data_objects = (await s.execute(text("select name from data_objects"))).all()
        print("DATA_OBJECTS:", data_objects)
        discovery_runs = (await s.execute(text("select id, status from discovery_runs"))).all()
        print("DISCOVERY_RUNS:", discovery_runs)
        assets = (await s.execute(text("select name, discovery_scope from data_assets"))).all()
        print("ASSETS:", assets)
        res = (
            await s.execute(
                text(
                    "SELECT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'e2e_source_table')"
                )
            )
        ).scalar()
        print("E2E_SOURCE_TABLE EXISTS:", res)


asyncio.run(main())
