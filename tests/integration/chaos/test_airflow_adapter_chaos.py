from __future__ import annotations

import httpx
import pytest
import respx

from app.domain.shared.exceptions import CircuitBreakerOpenError
from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
    AirflowOrchestratorAdapter,
)
from app.infrastructure.resilience.circuit_breaker import AsyncCircuitBreaker

AIRFLOW_URL = "http://fake-airflow:8080"
TOKEN_URL = f"{AIRFLOW_URL}/auth/token"
DAG_RUN_URL = f"{AIRFLOW_URL}/api/v2/dags/my_dag/dagRuns"


@pytest.fixture
def adapter_without_cb() -> AirflowOrchestratorAdapter:
    return AirflowOrchestratorAdapter(
        airflow_url=AIRFLOW_URL,
        username="admin",
        password="admin",
        max_retries=1,
        retry_delay_seconds=0.0,
    )


@pytest.fixture
def circuit_breaker() -> AsyncCircuitBreaker:
    return AsyncCircuitBreaker("airflow-test", failure_threshold=3, recovery_timeout_seconds=999)


@pytest.fixture
def adapter_with_cb(circuit_breaker: AsyncCircuitBreaker) -> AirflowOrchestratorAdapter:
    return AirflowOrchestratorAdapter(
        airflow_url=AIRFLOW_URL,
        username="admin",
        password="admin",
        max_retries=1,
        retry_delay_seconds=0.0,
        circuit_breaker=circuit_breaker,
    )


@pytest.mark.asyncio
@respx.mock
async def test_trigger_dag_succeeds_on_first_attempt(
    adapter_without_cb: AirflowOrchestratorAdapter,
) -> None:
    """When Airflow returns 201, trigger_dag completes without error."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(201))

    await adapter_without_cb.trigger_dag(
        pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
    )


@pytest.mark.asyncio
@respx.mock
async def test_circuit_breaker_opens_after_threshold_failures(
    adapter_with_cb: AirflowOrchestratorAdapter,
    circuit_breaker: AsyncCircuitBreaker,
) -> None:
    """After failure_threshold consecutive errors, the circuit opens and subsequent calls raise CircuitBreakerOpenError instantly."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(503))

    # Exhaust the threshold (3 failures)
    for _ in range(3):
        with pytest.raises(httpx.HTTPStatusError):
            await adapter_with_cb.trigger_dag(
                pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
            )

    assert circuit_breaker.state == "OPEN"

    # The next call must fail fast — no HTTP call should be made
    with pytest.raises(CircuitBreakerOpenError):
        await adapter_with_cb.trigger_dag(
            pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
        )


@pytest.mark.asyncio
@respx.mock
async def test_trigger_dag_raises_on_persistent_error(
    adapter_without_cb: AirflowOrchestratorAdapter,
) -> None:
    """Persistent 503 responses raise an httpx.HTTPStatusError (no silent failure)."""
    respx.post(TOKEN_URL).mock(return_value=httpx.Response(200, json={"access_token": "tok"}))
    respx.post(DAG_RUN_URL).mock(return_value=httpx.Response(503))

    with pytest.raises(httpx.HTTPStatusError):
        await adapter_without_cb.trigger_dag(
            pipeline_id="pid", run_id="rid", dag_run_id="drid", pipeline_name="my_dag"
        )
