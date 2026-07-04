from __future__ import annotations

from dataclasses import dataclass, field

_WIDENING_MAP: dict[str, set[str]] = {
    "integer": {"bigint", "float", "decimal"},
    "bigint": {"float", "decimal"},
    "float": {"decimal"},
    "string": {"string"},
}

@dataclass(frozen=True)
class SchemaField:
    """
    Immutable description of a single field/column within a DataObject schema.
    """

    name: str
    source_type: str                      # raw type from endpoint
    normalized_type: str                  # platform canonical (ElementType value)
    nullable: bool = True
    is_primary_key: bool = False
    description: str | None = None        # from source comments if available
    extra: dict = field(default_factory=dict)   # provider-specific metadata

    def is_compatible_with(self, other: "SchemaField") -> bool:
        """
        """
        base_other = other.normalized_type.lower()
        base_self = self.normalized_type.lower()
        
        # Complex types like JSON/STRUCT/ARRAY are not in the widening map.
        # This acts as a fallback: if it's complex, it MUST be identical to be compatible.
        return base_self == base_other or base_self in _WIDENING_MAP.get(base_other, set())
