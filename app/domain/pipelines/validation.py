from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class ValidationError:
    json_pointer: str
    error_code: str
    message: str
    suggestion: str


@dataclass
class ValidationResult:
    is_valid: bool
    errors: list[ValidationError] = field(default_factory=list)
