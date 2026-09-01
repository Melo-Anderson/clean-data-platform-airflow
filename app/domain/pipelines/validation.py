from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ValidationError:
    json_pointer: str
    error_code: str
    message: str
    suggestion: str


@dataclass(frozen=True)
class ValidationResult:
    is_valid: bool
    errors: tuple[ValidationError, ...] = field(default_factory=tuple)
