import pytest
import httpx
from unittest.mock import patch, AsyncMock
from app.infrastructure.adapters.orchestration.airflow_orchestrator_adapter import (
    AirflowOrchestratorAdapter,
)


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
    """DAG may not be parsed yet — adapter should retry up to 3 times."""
    mock_404 = httpx.Response(404, request=httpx.Request("POST", ""))
    mock_refresh = httpx.Response(200, json={}, request=httpx.Request("POST", ""))
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
        mock_post.side_effect = [mock_404, mock_refresh, mock_200]

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
        assert mock_post.call_count == 3
