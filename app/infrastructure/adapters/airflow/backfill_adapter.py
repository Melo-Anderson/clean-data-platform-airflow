from __future__ import annotations

from datetime import date

import httpx


class AirflowBackfillAdapter:
    def __init__(self, airflow_url: str, username: str, password: str) -> None:
        self._base_url = airflow_url.rstrip("/")
        self._auth = (username, password)

    async def create_backfill(self, dag_id: str, from_date: date, to_date: date) -> str:
        async with httpx.AsyncClient(auth=self._auth, timeout=30.0) as client:
            response = await client.post(
                f"{self._base_url}/api/v2/backfills",
                json={
                    "dag_id": dag_id,
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "run_backwards": False,
                    "max_active_runs": 1,
                },
            )
            response.raise_for_status()
            data = response.json()
        return str(data.get("backfill_id", data.get("id", "")))
