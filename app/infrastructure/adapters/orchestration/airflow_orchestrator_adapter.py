from __future__ import annotations
import asyncio
import logging
import httpx

logger = logging.getLogger(__name__)


class AirflowOrchestratorAdapter:
    """Triggers Airflow DAG runs via the Airflow REST API (v1)."""

    def __init__(
        self,
        airflow_url: str = "http://airflow-webserver:8080",
        username: str = "admin",
        password: str = "admin",
        max_retries: int = 5,
        retry_delay_seconds: float = 5.0,
    ) -> None:
        self._airflow_url = airflow_url.rstrip("/")
        self._auth = (username, password)
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds

    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
        pipeline_name: str = "",
    ) -> None:
        dag_id = pipeline_name or pipeline_id
        url = f"{self._airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {"dag_run_id": dag_run_id, "conf": {"run_id": run_id, "pipeline_id": pipeline_id}}

        async with httpx.AsyncClient(auth=self._auth, timeout=30.0) as client:
            for attempt in range(1, self._max_retries + 1):
                resp = await client.post(url, json=payload)
                if resp.status_code == 200:
                    logger.info("DAG triggered: dag_id=%s dag_run_id=%s", dag_id, dag_run_id)
                    return
                if resp.status_code == 404 and attempt < self._max_retries:
                    logger.warning("DAG %r not yet parsed (attempt %d). Retrying...", dag_id, attempt)
                    await asyncio.sleep(self._retry_delay)
                    continue
                resp.raise_for_status()
