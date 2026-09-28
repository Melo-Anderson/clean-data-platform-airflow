from __future__ import annotations

from typing import Any

from app.domain.pipelines.airflow_config import AirflowConfig
from app.domain.pipelines.compute_config import ComputeConfig
from app.domain.pipelines.compute_engine import ComputeEngine
from app.domain.pipelines.destination_object_config import DestinationObjectConfig
from app.domain.pipelines.extraction_config import ExtractionConfig
from app.domain.pipelines.load_strategy import LoadStrategy
from app.domain.pipelines.quality_rule import QualityRule
from app.domain.pipelines.quality_rule_type import QualityRuleType
from app.domain.shared.platform_defaults import PlatformDefaults


class PipelineNormalizer:
    """Normaliza payloads brutos de pipeline aplicando defaults canonicos.

    Responsabilidade unica: dict parcial -> estruturas de dominio preenchidas.
    Sem logica de persistencia ou orquestracao.
    """

    def __init__(self, defaults: PlatformDefaults | None = None) -> None:
        self._d = defaults or PlatformDefaults()

    def normalize_airflow(self, raw: dict[str, Any]) -> AirflowConfig:
        d = self._d.airflow
        return AirflowConfig(
            retries=int(raw.get("retries", d.retries)),
            retry_delay_minutes=int(raw.get("retry_delay_minutes", d.retry_delay_minutes)),
            execution_timeout_minutes=int(
                raw.get("execution_timeout_minutes", d.execution_timeout_minutes)
            ),
            sla_minutes=int(raw.get("sla_minutes", d.sla_minutes)),
            tags=tuple(raw.get("tags", [])),
            pool=str(raw.get("pool", d.pool)),
        )

    def normalize_extraction(self, items: list[dict[str, Any]]) -> list[ExtractionConfig]:
        d = self._d.extraction
        return [
            ExtractionConfig(
                object_name=item["object_name"],
                load_strategy=LoadStrategy(item.get("load_strategy", d.load_strategy)),
                watermark_column=item.get("watermark_column"),
                page_size=int(item.get("page_size", d.page_size)),
                partition_column=item.get("partition_column"),
                compression=item.get("compression", d.compression),
                encoding=item.get("encoding", d.encoding),
                extraction_query=item.get("extraction_query"),
                credential_ref=item.get("credential_ref"),
            )
            for item in items
        ]

    def normalize_compute(self, raw: dict[str, Any]) -> ComputeConfig:
        d = self._d.compute
        raw_config = raw.get("config")
        cfg: dict[str, Any] = raw_config if isinstance(raw_config, dict) else raw
        return ComputeConfig(
            engine=ComputeEngine(raw.get("engine", d.default_engine)),
            staging_bucket=str(raw.get("staging_bucket") or cfg.get("staging_bucket", "")),
            select=str(raw.get("select") or cfg.get("select", "")),
            num_workers=int(cfg.get("num_workers", d.num_workers)),
            machine_type=str(cfg.get("machine_type", d.machine_type)),
            source_type=str(cfg.get("source_type", "")),
            credential_ref=str(cfg.get("credential_ref", "")),
            driver=str(cfg.get("driver", "")),
        )

    def normalize_destination_objects(
        self, items: list[dict[str, Any]]
    ) -> list[DestinationObjectConfig]:
        return [
            DestinationObjectConfig(
                object_name=item["object_name"],
                create_if_not_exists=item.get("create_if_not_exists", True),
            )
            for item in items
        ]

    def normalize_quality_rules(self, items: list[dict[str, Any]]) -> list[QualityRule]:
        return [
            QualityRule(
                type=QualityRuleType(item["type"]),
                column=item.get("column"),
                value=item.get("value"),
            )
            for item in items
        ]
