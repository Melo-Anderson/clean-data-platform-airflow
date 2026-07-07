from __future__ import annotations

import logging

logger = logging.getLogger(__name__)


class LoggingOrchestratorAdapter:
    """
    Mock adapter: simula o disparo para o Airflow apenas com logging.
    Substituir por AirflowOrchestratorAdapter em produção.
    """

    async def trigger_dag(
        self,
        pipeline_id: str,
        run_id: str,
        dag_run_id: str,
    ) -> None:
        logger.info(
            "[MOCK] DAG triggered | pipeline_id=%s run_id=%s dag_run_id=%s",
            pipeline_id,
            run_id,
            dag_run_id,
        )
