from __future__ import annotations

import asyncio
import logging
import time

import httpx
import structlog.contextvars
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from app.application.shared.telemetry_port import TelemetryPort
from app.infrastructure.resilience.circuit_breaker import AsyncCircuitBreaker

logger = logging.getLogger(__name__)


class AirflowOrchestratorAdapter:
    """Triggers Airflow DAG runs via the Airflow REST API (v2).

    Wraps HTTP calls with an optional AsyncCircuitBreaker to protect against
    Airflow outages. Propagates the current correlation_id into the DAG conf
    payload for end-to-end traceability. Emits telemetry via TelemetryPort.
    """

    def __init__(
        self,
        airflow_url: str = "http://airflow-webserver:8080",
        username: str = "admin",
        password: str = "admin",
        max_retries: int = 10,
        retry_delay_seconds: float = 5.0,
        circuit_breaker: AsyncCircuitBreaker | None = None,
        telemetry: TelemetryPort | None = None,
    ) -> None:
        self._airflow_url = airflow_url.rstrip("/")
        self._auth = (username, password)
        self._max_retries = max_retries
        self._retry_delay = retry_delay_seconds
        self._circuit_breaker = circuit_breaker
        self._telemetry = telemetry

    async def _get_token(self, client: httpx.AsyncClient) -> str:
        url = f"{self._airflow_url}/auth/token"
        payload = {"username": self._auth[0], "password": self._auth[1]}
        resp = await client.post(url, json=payload)
        resp.raise_for_status()
        return str(resp.json()["access_token"])

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=5),
        retry=retry_if_exception_type(httpx.RequestError),
        reraise=True,
    )
    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
        pipeline_name: str = "",
    ) -> None:
        import datetime

        correlation_id = structlog.contextvars.get_contextvars().get("correlation_id", "")
        dag_id = pipeline_name or pipeline_id
        url = f"{self._airflow_url}/api/v2/dags/{dag_id}/dagRuns"
        logical_date = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date,
            "conf": {
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "correlation_id": correlation_id,
            },
        }

        start = time.monotonic()
        async with httpx.AsyncClient(timeout=30.0) as client:
            token = await self._get_token(client)
            headers = {"Authorization": f"Bearer {token}"}

            async def _do_trigger() -> None:
                for attempt in range(1, self._max_retries + 1):
                    resp = await client.post(url, json=payload, headers=headers)
                    if resp.status_code in (200, 201):
                        logger.info(
                            "DAG triggered: dag_id=%s dag_run_id=%s correlation_id=%s",
                            dag_id, dag_run_id, correlation_id,
                        )
                        return
                    if resp.status_code == 404 and attempt < self._max_retries:
                        logger.warning(
                            "DAG %r not yet parsed (attempt %d/%d). Waiting %ss...",
                            dag_id, attempt, self._max_retries, self._retry_delay,
                        )
                        try:
                            await client.post(
                                f"{self._airflow_url}/api/v2/dags/{dag_id}/refresh",
                                headers=headers, timeout=5.0,
                            )
                        except Exception as e:
                            logger.warning("Could not trigger DAG refresh: %s", e)
                        await asyncio.sleep(self._retry_delay)
                        continue
                    resp.raise_for_status()

            if self._circuit_breaker:
                await self._circuit_breaker.call(_do_trigger())
            else:
                await _do_trigger()

        duration_ms = (time.monotonic() - start) * 1000
        if self._telemetry:
            self._telemetry.record_metric(
                "airflow.dag_trigger.latency_ms", duration_ms, tags={"dag_id": dag_id}
            )
            self._telemetry.record_event(
                "airflow.dag_trigger.success",
                {"dag_id": dag_id, "dag_run_id": dag_run_id, "correlation_id": correlation_id},
            )
