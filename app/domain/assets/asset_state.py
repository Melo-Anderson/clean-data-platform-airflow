from __future__ import annotations

from enum import StrEnum


class AssetState(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    DEPRECATED = "deprecated"
    ARCHIVED = "archived"


VALID_TRANSITIONS: dict[AssetState, frozenset[AssetState]] = {
    AssetState.DRAFT: frozenset({AssetState.ACTIVE}),
    AssetState.ACTIVE: frozenset({AssetState.DEPRECATED}),
    AssetState.DEPRECATED: frozenset({AssetState.ARCHIVED}),
    AssetState.ARCHIVED: frozenset(),
}
