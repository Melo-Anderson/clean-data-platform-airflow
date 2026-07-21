from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class CompositeIndex:
    """
    Represents a (potentially composite) index on a DataObject.

    A single-column unique index on 'email' is:
        CompositeIndex(name="idx_email", columns=["email"], unique=True)

    A multi-column covering index on ('tenant_id', 'created_at') is:
        CompositeIndex(name="idx_tenant_ts", columns=["tenant_id", "created_at"])
    """

    name: str
    columns: list[str]
    unique: bool = False


@dataclass(frozen=True)
class CompositeForeignKey:
    """
    Represents a (potentially composite) foreign key constraint on a DataObject.

    A composite FK from (tenant_id, org_id) -> organizations(tenant_id, id) is:
        CompositeForeignKey(
            name="fk_org",
            constrained_columns=["tenant_id", "org_id"],
            referred_table="organizations",
            referred_columns=["tenant_id", "id"],
        )
    """

    name: str
    constrained_columns: list[str]
    referred_table: str
    referred_columns: list[str]


@dataclass(frozen=True)
class DataObjectMetadata:
    """
    Structural metadata belonging to a DataObject as a whole (table/collection level).

    Stored as JSON in data_objects.object_metadata_json.
    Separate from DataElement (column-level) to correctly represent
    multi-column constructs like composite indexes and composite FKs.
    """

    indexes: list[CompositeIndex] = field(default_factory=list)
    foreign_keys: list[CompositeForeignKey] = field(default_factory=list)
    partition_key: str | None = None
