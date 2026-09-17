"""Airflow adapters."""

from __future__ import annotations

from app.infrastructure.adapters.airflow.backfill_adapter import AirflowBackfillAdapter

__all__ = ["AirflowBackfillAdapter"]
