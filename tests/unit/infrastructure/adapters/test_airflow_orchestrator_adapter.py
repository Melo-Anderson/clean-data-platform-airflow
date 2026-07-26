from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
import structlog.contextvars

from app.domain.shared.exceptions import CircuitBreakerOpenError
from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
    AirflowOrchestratorAdapter,
)
from app.infrastructure.resilience.circuit_breaker import AsyncCircuitBreaker


class FakeTelemetry:
    def __init__(self):
        self.metrics: list = []
        self.events: list = []

    def record_metric(self, name, value, tags=None):
        self.metrics.append((name, value, tags))

    def record_event(self, event_name, data):
        self.events.append((event_name, data))


async def _async_fail():
    raise RuntimeError("forced")


@pytest.fixture
def mock_httpx():
    mock_resp = MagicMock()
    mock_resp.status_code = 201
    mock_resp.raise_for_status = MagicMock()

    token_resp = MagicMock()
    token_resp.raise_for_status = MagicMock()
    token_resp.json.return_value = {"access_token": "fake-token"}

    with patch("httpx.AsyncClient") as mock_client_cls:
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=False)
        mock_client.post = AsyncMock(side_effect=[token_resp, mock_resp])
        mock_client_cls.return_value = mock_client
        yield mock_client


@pytest.mark.asyncio
async def test_trigger_dag_calls_airflow_api() -> None:
    mock_resp = httpx.Response(
        200, json={"dag_run_id": "run_123"}, request=httpx.Request("POST", "")
    )

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch.object(
            AirflowOrchestratorAdapter, "_get_token", new_callable=AsyncMock
        ) as mock_get_token,
    ):
        mock_get_token.return_value = "fake-token"
        mock_post.return_value = mock_resp

        adapter = AirflowOrchestratorAdapter(
            airflow_url="http://airflow-webserver:8080",
            username="admin",
            password="admin",
        )
        await adapter.trigger_dag(
            pipeline_id="p1",
            run_id="r1",
            dag_run_id="test__2026-01-01",
            pipeline_name="my-pipeline",
        )
        assert mock_post.called
        assert (
            mock_post.call_args[0][0]
            == "http://airflow-webserver:8080/api/v2/dags/my-pipeline/dagRuns"
        )


@pytest.mark.asyncio
async def test_trigger_dag_retries_on_404() -> None:
    """DAG may not be parsed yet — adapter should retry on 404."""
    mock_404 = httpx.Response(404, request=httpx.Request("POST", ""))
    mock_200 = httpx.Response(
        200, json={"dag_run_id": "run_123"}, request=httpx.Request("POST", "")
    )

    with (
        patch("httpx.AsyncClient.post", new_callable=AsyncMock) as mock_post,
        patch.object(
            AirflowOrchestratorAdapter, "_get_token", new_callable=AsyncMock
        ) as mock_get_token,
    ):
        mock_get_token.return_value = "fake-token"
        mock_post.side_effect = [mock_404, mock_200]

        adapter = AirflowOrchestratorAdapter(
            airflow_url="http://airflow-webserver:8080",
            username="admin",
            password="admin",
            retry_delay_seconds=0,  # no sleep in tests
        )
        await adapter.trigger_dag(
            pipeline_id="p1",
            run_id="r1",
            dag_run_id="test__2026-01-01",
            pipeline_name="my-pipeline",
        )
        assert mock_post.call_count == 2


@pytest.mark.asyncio
async def test_trigger_dag_injects_correlation_id(mock_httpx):
    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(correlation_id="test-correlation-123")
    adapter = AirflowOrchestratorAdapter()
    await adapter.trigger_dag("pipe-1", "run-1", "run-id-1", "my_dag")
    call_kwargs = mock_httpx.post.call_args_list[1]
    payload = call_kwargs.kwargs["json"]
    assert payload["conf"]["correlation_id"] == "test-correlation-123"


@pytest.mark.asyncio
async def test_trigger_dag_records_telemetry(mock_httpx):
    telemetry = FakeTelemetry()
    adapter = AirflowOrchestratorAdapter(telemetry=telemetry)
    await adapter.trigger_dag("pipe-1", "run-1", "run-id-1", "my_dag")
    assert any("latency_ms" in m[0] for m in telemetry.metrics)
    assert any("success" in e[0] for e in telemetry.events)


@pytest.mark.asyncio
async def test_circuit_breaker_open_raises_error(mock_httpx):
    cb = AsyncCircuitBreaker("airflow-test", failure_threshold=1, recovery_timeout_seconds=999)
    with pytest.raises(RuntimeError):
        await cb.call(_async_fail())
    adapter = AirflowOrchestratorAdapter(circuit_breaker=cb)
    with pytest.raises(CircuitBreakerOpenError):
        await adapter.trigger_dag("pipe-1", "run-1", "run-id-1", "my_dag")
