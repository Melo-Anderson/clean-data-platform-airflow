#!/usr/bin/env python
"""
Development utility: verify database connectivity.
Run with: uv run python scripts/check_db.py
NOT imported by production code.
"""

import asyncio

from sqlalchemy import text

from app.infrastructure.persistence.database import get_session_factory


async def main() -> None:
    f = get_session_factory()
    async with f() as s:
        pipelines = (await s.execute(text("select name from pipelines"))).all()
        print("PIPELINES:", pipelines)
        data_objects = (await s.execute(text("select name from data_objects"))).all()
        print("DATA_OBJECTS:", data_objects)
        discovery_runs = (await s.execute(text("select id, status from discovery_runs"))).all()
        print("DISCOVERY_RUNS:", discovery_runs)


asyncio.run(main())
