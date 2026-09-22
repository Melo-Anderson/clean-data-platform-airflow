"""Test that all generated demo DAGs are syntactically valid Python."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.infrastructure.dag_generator.dag_validator import DagSyntaxValidator


def test_demo_dags_loaded_with_zero_errors() -> None:
    dags_dir = Path("dags")
    if not dags_dir.exists() or len(list(dags_dir.glob("*.py"))) < 10:
        from scripts.seed_demo_environment import generate_dags_only

        generate_dags_only()

    dag_files = list(dags_dir.glob("*.py"))
    assert len(dag_files) >= 10, f"Expected at least 10 DAGs, found {len(dag_files)}"

    validator = DagSyntaxValidator()
    for dag_file in dag_files:
        code = dag_file.read_text(encoding="utf-8")
        try:
            validator.validate(code, filename=dag_file.name)
        except Exception as exc:
            pytest.fail(f"Validation failed for {dag_file.name}: {exc}")
