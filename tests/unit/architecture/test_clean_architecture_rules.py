from __future__ import annotations

import ast
from pathlib import Path


def _get_imports(file_path: Path) -> list[str]:
    """Return all top-level module names imported by the given Python file."""
    with open(file_path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=str(file_path))

    imports: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                imports.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    return imports


def test_domain_layer_has_zero_external_dependencies() -> None:
    """Domain must be pure Python — zero infrastructure, framework, or application imports."""
    domain_dir = Path("app/domain")
    assert domain_dir.is_dir(), "app/domain directory not found"

    forbidden_prefixes = (
        "fastapi",
        "starlette",
        "airflow",
        "sqlalchemy",
        "app.infrastructure",
        "app.application",
    )

    violations: list[str] = []
    for py_file in domain_dir.rglob("*.py"):
        for module in _get_imports(py_file):
            if any(
                module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden_prefixes
            ):
                violations.append(f"{py_file}: imports forbidden module '{module}'")

    assert not violations, "Clean Architecture Domain Layer violations detected:\n" + "\n".join(
        violations
    )


def test_application_layer_has_zero_infrastructure_or_framework_dependencies() -> None:
    """Application (use cases) must not import FastAPI, SQLAlchemy, Airflow, or concrete infra."""
    app_dir = Path("app/application")
    assert app_dir.is_dir(), "app/application directory not found"

    forbidden_prefixes = (
        "fastapi",
        "starlette",
        "airflow",
        "sqlalchemy",
        "app.infrastructure",
    )

    violations: list[str] = []
    for py_file in app_dir.rglob("*.py"):
        for module in _get_imports(py_file):
            if any(
                module == prefix or module.startswith(f"{prefix}.") for prefix in forbidden_prefixes
            ):
                violations.append(f"{py_file}: imports forbidden module '{module}'")

    assert not violations, (
        "Clean Architecture Application Layer violations detected:\n" + "\n".join(violations)
    )
