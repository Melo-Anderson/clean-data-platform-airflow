# tests/integration/repositories/test_sql_endpoint_repository.py
from __future__ import annotations

import uuid

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.endpoints.endpoint import CloudBucketEndpoint, DatabaseEndpoint
from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.value_objects import CredentialReference
from app.infrastructure.persistence.repositories.sql_endpoint_repository import (
    SqlEndpointRepository,
)


def _cred() -> CredentialReference:
    return CredentialReference("vault/secret/test")


@pytest.mark.asyncio
async def test_save_and_find_database_endpoint(db_session: AsyncSession) -> None:
    repo = SqlEndpointRepository(db_session)
    ep = DatabaseEndpoint(
        id=str(uuid.uuid4()),
        asset_id="asset-1",
        credential_ref=_cred(),
    )
    saved = await repo.save(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE


@pytest.mark.asyncio
async def test_save_and_find_cloud_bucket_endpoint(db_session: AsyncSession) -> None:
    repo = SqlEndpointRepository(db_session)
    ep = CloudBucketEndpoint(
        id=str(uuid.uuid4()),
        asset_id="asset-2",
        credential_ref=_cred(),
        provider="gcs",
        bucket="raw-data-prod",
        region="us-central1",
    )
    saved = await repo.save(ep)
    assert isinstance(saved, CloudBucketEndpoint)
    assert saved.provider == "gcs"
    assert saved.bucket == "raw-data-prod"
