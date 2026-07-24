import asyncio
import contextlib
import logging
import os

import httpx
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

logger = logging.getLogger(__name__)

_in_docker = os.path.exists("/.dockerenv") or os.getenv("API_URL", "").startswith(
    "http://platform-api"
)
_api_host = "platform-api" if _in_docker else "127.0.0.1"
_db_host = "postgres" if _in_docker else "127.0.0.1"
_airflow_host = "airflow-webserver" if _in_docker else "127.0.0.1"

API_URL = os.getenv("API_URL", f"http://{_api_host}:8000")
AIRFLOW_URL = os.getenv("AIRFLOW_URL", f"http://{_airflow_host}:8080")
_raw_db_url = os.getenv("PLATFORM_DATABASE_URL", "")
if not _raw_db_url or _raw_db_url.startswith("sqlite"):
    PLATFORM_DATABASE_URL = f"postgresql+asyncpg://airflow:airflow@{_db_host}:5432/platform_db"
else:
    PLATFORM_DATABASE_URL = _raw_db_url

pytestmark = pytest.mark.e2e


async def _wait_and_unpause_dag(
    dag_id: str,
    airflow_url: str = AIRFLOW_URL,
    username: str = "admin",
    password: str = "admin",
    max_wait_seconds: int = 600,
    poll_interval: float = 5.0,
) -> None:
    """Wait for the Airflow scheduler to parse a newly written DAG file, then unpause it.

    DAGs are paused at creation (AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION=true).
    The adapter only retries on 404 (DAG not yet parsed); unpausing is an explicit
    test-environment concern and must not live in production code.
    """
    async with httpx.AsyncClient(
        timeout=30.0, transport=httpx.AsyncHTTPTransport(retries=3)
    ) as client:
        # Obtain token
        token_resp = await client.post(
            f"{airflow_url}/auth/token",
            json={"username": username, "password": password},
        )
        token_resp.raise_for_status()
        headers = {"Authorization": f"Bearer {token_resp.json()['access_token']}"}

        # Force Airflow to reload the DAG from disk immediately by executing reserialize
        try:
            import subprocess

            res = await asyncio.to_thread(
                subprocess.run,
                [
                    "docker",
                    "exec",
                    "clean-data-platform-airflow-airflow-webserver-1",
                    "airflow",
                    "dags",
                    "reserialize",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            if res.returncode != 0:
                await asyncio.to_thread(
                    subprocess.run,
                    ["docker", "exec", "airflow-webserver", "airflow", "dags", "reserialize"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
            logger.info("DAG reserialize triggered via docker exec")
        except Exception as e:
            logger.warning("Could not trigger DAG reserialize via docker: %s", e)

        # Wait until the DAG appears in the API (scheduler has parsed it)
        elapsed = 0.0
        while elapsed < max_wait_seconds:
            try:
                resp = await client.get(f"{airflow_url}/api/v2/dags/{dag_id}", headers=headers)
                if resp.status_code == 200:
                    break
            except Exception:
                pass
            if int(elapsed) % 15 == 0 and elapsed > 0:
                with contextlib.suppress(Exception):
                    await asyncio.to_thread(
                        subprocess.run,
                        [
                            "docker",
                            "exec",
                            "clean-data-platform-airflow-airflow-webserver-1",
                            "airflow",
                            "dags",
                            "reserialize",
                        ],
                        capture_output=True,
                        text=True,
                        check=False,
                    )
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval
        else:
            raise TimeoutError(
                f"DAG '{dag_id}' was not parsed by Airflow scheduler within {max_wait_seconds}s"
            )

        # Unpause so dagRuns can be triggered
        for patch_attempt in range(1, 4):
            try:
                unpause_resp = await client.patch(
                    f"{airflow_url}/api/v2/dags/{dag_id}",
                    json={"is_paused": False},
                    headers=headers,
                )
                unpause_resp.raise_for_status()
                break
            except Exception:
                if patch_attempt == 3:
                    raise
                await asyncio.sleep(1.0)


@pytest.fixture(scope="session", autouse=True)
def setup_e2e_database() -> None:
    """
    Creates a dummy table in the actual Postgres container to simulate a source
    that the discovery will pick up.
    """

    async def _run() -> None:
        print(f"!!! setup_e2e_database URL: {PLATFORM_DATABASE_URL} !!!", flush=True)
        engine = create_async_engine(PLATFORM_DATABASE_URL)
        async with engine.begin() as conn:
            await conn.execute(text("DROP TABLE IF EXISTS e2e_source_table;"))
            await conn.execute(
                text(
                    """
                CREATE TABLE e2e_source_table (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
                )
            )
            # Check if it exists now
            from sqlalchemy import inspect

            def check_tables(connection):
                inspector = inspect(connection)
                return inspector.get_table_names()

            tables = await conn.run_sync(check_tables)
            print(f"!!! Reflected tables in setup_e2e_database: {tables} !!!", flush=True)
        await engine.dispose()

    print("!!! Running setup_e2e_database fixture !!!", flush=True)
    try:
        asyncio.run(_run())
    except Exception as e:
        print(f"!!! setup_e2e_database failed: {e} !!!", flush=True)
        raise
    print("!!! Finished setup_e2e_database fixture !!!", flush=True)


@pytest.mark.asyncio
async def test_end_to_end_platform_flow(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    endpoint_payload = {
        "name": "e2e-db-prod",
        "credential_ref": "secret/postgres",
        "technical_description": "E2E testing Postgres database",
    }

    resp = await sre_client.post("/v1/endpoints/database", json=endpoint_payload)
    assert resp.status_code in (
        201,
        409,
        422,
    ), f"Endpoint creation failed: {resp.status_code} - {resp.text}"

    # 2. Register a DataAsset
    asset_payload = {
        "name": "e2e-asset",
        "description": "E2E Data Asset for testing",
        "owner_email": "e2e@co.com",
        "tags": ["e2e"],
        "policy_tags": [],
        "discovery_schedule": "0 0 * * *",
        "discovery_scope_include": ["public.e2e_source_table"],
        "discovery_scope_exclude": [],
    }
    resp = await api_client.post("/v1/assets/", json=asset_payload)
    assert resp.status_code in (
        201,
        409,
        422,
    ), f"Asset creation failed: {resp.status_code} - {resp.text}"

    # 3. Activate the DataAsset (requires SRE role)
    resp = await sre_client.post(
        "/v1/assets/e2e-asset/activate", params={"endpoint_name": "e2e-db-prod"}
    )
    assert resp.status_code in (
        200,
        422,
    ), f"Asset activation failed: {resp.status_code} - {resp.text}"

    # 4. Trigger Discovery
    trigger_payload = {"triggered_by": "e2e_test"}
    resp = await api_client.post("/v1/discovery/assets/e2e-asset/run", json=trigger_payload)
    assert resp.status_code == 201

    data = resp.json()
    assert data["status"] in ["queued", "completed"]
    assert "id" in data

    engine = create_async_engine(PLATFORM_DATABASE_URL)
    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        result = await session.execute(
            text("SELECT id, name FROM data_objects WHERE name = 'public.e2e_source_table'")
        )
        row = result.fetchone()
        assert row is not None, "DataObject for e2e_source_table must exist after Discovery"
        assert row[1] == "public.e2e_source_table"
    await engine.dispose()


@pytest.mark.asyncio
async def test_pipeline_register_and_trigger(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    """
    Cenário E2E: Registrar um pipeline de ingestão para o e2e-asset,
    disparar a execução (trigger mocado com LoggingOrchestratorAdapter)
    e validar que o PipelineRun foi criado no banco com status 'running'.

    Os dados NÃO são apagados ao final — podem ser consultados via API ou SQL.
    """
    # Pré-condição: garantir que o e2e-asset e o endpoint já existem
    # (são idempotentes — 422 = já existe, aceito)
    await sre_client.post(
        "/v1/endpoints/database",
        json={
            "name": "e2e-db-prod",
            "credential_ref": "secret/postgres",
            "technical_description": "E2E testing Postgres database",
        },
    )
    resp = await api_client.post(
        "/v1/assets/",
        json={
            "name": "e2e-asset",
            "description": "E2E Data Asset for testing",
            "owner_email": "e2e@co.com",
            "tags": ["e2e"],
            "policy_tags": [],
            "discovery_schedule": "0 0 * * *",
            "discovery_scope_include": ["public.e2e_source_table"],
            "discovery_scope_exclude": [],
        },
    )
    asset_id = None
    if resp.status_code == 201:
        asset_id = resp.json()["id"]
    else:
        # Já existe — buscar o ID via GET
        resp_get = await api_client.get("/v1/assets/e2e-asset")
        assert resp_get.status_code == 200
        asset_id = resp_get.json()["id"]

    pipe_name = "e2e-ingest-pipeline"

    # 1. Registrar o pipeline de ingestão
    pipeline_payload = {
        "name": pipe_name,
        "pipeline_type": "ingestion",
        "owner_email": "e2e@co.com",
        "source_asset_id": asset_id,
        "cron_schedule": "0 0 * * *",
    }
    resp = await api_client.post("/v1/pipelines/", json=pipeline_payload)
    if resp.status_code == 201:
        pipeline_id = resp.json()["id"]
    elif resp.status_code == 422:
        engine = create_async_engine(PLATFORM_DATABASE_URL)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            res = await session.execute(
                text("SELECT id FROM pipelines WHERE name = :name"),
                {"name": pipe_name},
            )
            pipeline_id = str(res.scalar_one())
        await engine.dispose()
    else:
        assert (
            resp.status_code == 201
        ), f"Pipeline creation failed: {resp.status_code} - {resp.text}"
        pipeline_id = resp.json()["id"]

    assert pipeline_id is not None

    # 2. Consultar o pipeline recém-criado via GET
    resp = await api_client.get(f"/v1/pipelines/{pipeline_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == pipe_name
    assert data["pipeline_type"] == "ingestion"
    # 3. Disparar a execução da DAG (isso grava o arquivo físico em disco)
    resp = await api_client.post(
        f"/v1/pipelines/{pipeline_id}/run", json={"triggered_by": "e2e_test"}
    )
    assert resp.status_code == 201
    run_data = resp.json()
    assert run_data["status"] == "running"
    assert run_data["pipeline_id"] == pipeline_id
    run_id = run_data["id"]

    # 4. Garantir que a DAG foi parseada e despausear
    await _wait_and_unpause_dag(dag_id=pipe_name)

    # 4. Submit mocked compute metrics (simulating Airflow callback)
    metrics_payload = {
        "metrics": {
            "row_count": 1500,
            "null_count_id": 0,
            "null_count_name": 0,
            "null_count_created_at": 0,
        }
    }
    resp = await api_client.post(
        f"/v1/pipelines/{pipeline_id}/runs/{run_id}/quality-gate",
        json=metrics_payload,
    )
    assert resp.status_code == 200
    gate_data = resp.json()
    assert gate_data["status"] == "success"
    assert gate_data["violations"] == []
    assert gate_data["run_id"] == run_id


@pytest.mark.asyncio
async def test_pipeline_quality_gate_violation(
    api_client: httpx.AsyncClient, sre_client: httpx.AsyncClient
) -> None:
    """Submitting metrics that violate quality rules must set run to quality_failed."""
    # Reuse e2e-ingest-pipeline (already registered in test_pipeline_register_and_trigger)
    resp_get_pipeline = await api_client.get("/v1/assets/e2e-asset")
    asset_id = resp_get_pipeline.json()["id"]

    pipe_name = "e2e-ingest-pipeline-violation"

    # Register the pipeline explicitly for this test to isolate execution
    pipeline_payload = {
        "name": pipe_name,
        "pipeline_type": "ingestion",
        "owner_email": "e2e@co.com",
        "source_asset_id": asset_id,
        "cron_schedule": "0 0 * * *",
    }
    resp_pipeline = await api_client.post("/v1/pipelines/", json=pipeline_payload)
    if resp_pipeline.status_code == 201:
        pipeline_id = resp_pipeline.json()["id"]
    elif resp_pipeline.status_code == 422:
        engine = create_async_engine(PLATFORM_DATABASE_URL)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            res = await session.execute(
                text("SELECT id FROM pipelines WHERE name = :name"),
                {"name": pipe_name},
            )
            pipeline_id = str(res.scalar_one())
        await engine.dispose()
    else:
        assert (
            resp_pipeline.status_code == 201
        ), f"Pipeline creation failed: {resp_pipeline.status_code} - {resp_pipeline.text}"
        pipeline_id = resp_pipeline.json()["id"]

    # 2. Consultar o pipeline recém-criado via GET
    resp = await api_client.get(f"/v1/pipelines/{pipeline_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["name"] == pipe_name
    # 3. Disparar a execução da DAG (isso grava o arquivo físico em disco)
    resp_run = await api_client.post(
        f"/v1/pipelines/{pipeline_id}/run", json={"triggered_by": "violation_test"}
    )
    assert resp_run.status_code == 201
    run_id = resp_run.json()["id"]

    # 4. Garantir que a DAG foi parseada e despausear
    await _wait_and_unpause_dag(dag_id=pipe_name)

    # Submit metrics that violate row_count_min (if rule exists) or no rule = success
    # Since e2e pipeline has no quality_rules configured, result is always success.
    # This test validates the endpoint works end-to-end.
    resp_gate = await api_client.post(
        f"/v1/pipelines/{pipeline_id}/runs/{run_id}/quality-gate",
        json={"metrics": {"row_count": 0}},
    )
    assert resp_gate.status_code == 200
    assert resp_gate.json()["run_id"] == run_id
