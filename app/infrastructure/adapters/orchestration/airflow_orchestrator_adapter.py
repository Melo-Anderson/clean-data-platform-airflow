from __future__ import annotations

import asyncio
import logging

import httpx

logger = logging.getLogger(__name__)


class AirflowOrchestratorAdapter:
    """Triggers Airflow DAG runs via the Airflow REST API (v2)."""

    def __init__(
        self,
        airflow_url: str = "http://airflow-webserver:8080",
        username: str = "admin",
        password: str = "admin",
        max_retries: int = 10,
        retry_delay_seconds: float = 5.0,
    ) -> None:
        self._airflow_url = airflow_url.rstrip("/")
        self._auth = (username, password)
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        url = f"{self._airflow_url}/auth/token"
        payload = {"username": self._auth[0], "password": self._auth[1]}
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return str(resp.json()["access_token"])

    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
        pipeline_name: str = "",
    ) -> None:
        import datetime

        dag_id = pipeline_name or pipeline_id
        url = f"{self._airflow_url}/api/v2/dags/{dag_id}/dagRuns"
        logical_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date,
            "conf": {"run_id": run_id, "pipeline_id": pipeline_id},
        }

        async with httpx.AsyncClient(timeout=30.0) as client:
            token = await self._get_token(client)
            headers = {"Authorization": f"Bearer {token}"}
            for attempt in range(1, self._max_retries + 1):
                resp = await client.post(url, json=payload, headers=headers)
                if resp.status_code in (200, 201):
                    logger.info("DAG triggered: dag_id=%s dag_run_id=%s", dag_id, dag_run_id)
                    return
                if resp.status_code == 404 and attempt < self._max_retries:
                    logger.warning(
                        "DAG %r not yet parsed by scheduler (attempt %d/%d). "
                        "Triggering refresh and waiting %ss before retry...",
                        dag_id,
                        attempt,
                        self._max_retries,
                        self._retry_delay,
                    )
                    # Trigger a refresh via API so the webserver invalidates cache on the fly
                    try:
                        await client.post(
                            f"{self._airflow_url}/api/v2/dags/{dag_id}/refresh",
                            headers=headers,
                            timeout=5.0,
                        )
                    except Exception as e:
                        logger.warning("Could not trigger DAG refresh: %s", e)

                    await asyncio.sleep(self._retry_delay)
                    continue
                resp.raise_for_status()
