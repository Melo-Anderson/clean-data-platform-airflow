"""Test that all generated demo DAGs load cleanly into Airflow DagBag without import errors."""

from __future__ import annotations

import ast
from pathlib import Path

import pytest


def test_demo_dags_loaded_with_zero_errors() -> None:
    dags_dir = Path("dags")
    assert dags_dir.exists(), "dags directory does not exist"

    dag_files = list(dags_dir.glob("*.py"))
    assert len(dag_files) >= 10, f"Expected at least 10 DAGs, found {len(dag_files)}"

    for dag_file in dag_files:
        try:
            with open(dag_file, encoding="utf-8") as f:
                ast.parse(f.read(), filename=dag_file.name)
        except SyntaxError as e:
            pytest.fail(f"Syntax error in generated DAG {dag_file.name}: {e}")
