from __future__ import annotations

import ast

from app.domain.shared.exceptions import PlatformValidationError


class DagSyntaxValidator:
    """Validates the Python syntax of generated Airflow DAG code using AST.

    Responsibility: verify that the *code output* of DagGenerator is
    syntactically valid Python before it is written to disk.
    Distinct from CiValidator, which validates YAML pipeline definitions.
    """

    def validate(self, code: str, filename: str = "<generated_dag>") -> None:
        """Parse and validate DAG source code.

        Raises:
            PlatformValidationError: if code is empty, has a Python syntax
                error, or produces an empty module body.
        """
        if not code or not code.strip():
            raise PlatformValidationError(
                f"Generated DAG '{filename}' is empty — nothing to validate"
            )

        try:
            tree = ast.parse(code, filename=filename)
        except SyntaxError as exc:
            raise PlatformValidationError(
                f"Syntax error in generated DAG '{filename}': {exc.msg} at line {exc.lineno}"
            ) from exc

        if not tree.body:
            raise PlatformValidationError(
                f"Generated DAG '{filename}' contains no executable statements"
            )
