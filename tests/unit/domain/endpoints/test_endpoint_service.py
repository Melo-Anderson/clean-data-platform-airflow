# tests/unit/domain/endpoints/test_endpoint_service.py
from __future__ import annotations

import uuid

import pytest

from app.domain.endpoints.endpoint import (
    CloudBucketEndpoint,
    DatabaseEndpoint,
    EtlFlowEndpoint,
    RestApiEndpoint,
    SftpEndpoint,
)
from app.domain.endpoints.endpoint_service import EndpointService
from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.value_objects import CredentialReference


class FakeEndpointRepository:
    """Named fake implementing EndpointRepository Protocol for unit tests."""

    def __init__(self) -> None:
        self._store: dict[
            str,
            DatabaseEndpoint
            | RestApiEndpoint
            | SftpEndpoint
            | CloudBucketEndpoint
            | EtlFlowEndpoint,
        ] = {}

    async def save(
        self,
        endpoint: DatabaseEndpoint
        | RestApiEndpoint
        | SftpEndpoint
        | CloudBucketEndpoint
        | EtlFlowEndpoint,
    ) -> DatabaseEndpoint | RestApiEndpoint | SftpEndpoint | CloudBucketEndpoint | EtlFlowEndpoint:
        self._store[endpoint.id] = endpoint
        return endpoint

    async def find_by_id(
        self, endpoint_id: str
    ) -> (
        DatabaseEndpoint
        | RestApiEndpoint
        | SftpEndpoint
        | CloudBucketEndpoint
        | EtlFlowEndpoint
        | None
    ):
        return self._store.get(endpoint_id)

    async def find_by_asset_id(
        self, asset_id: str
    ) -> (
        DatabaseEndpoint
        | RestApiEndpoint
        | SftpEndpoint
        | CloudBucketEndpoint
        | EtlFlowEndpoint
        | None
    ):
        return next((e for e in self._store.values() if e.asset_id == asset_id), None)


def _cred(path: str = "vault/secret/prod") -> CredentialReference:
    return CredentialReference(path)


def _id() -> str:
    return str(uuid.uuid4())


@pytest.mark.asyncio
async def test_provision_database_endpoint_has_typed_fields() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = DatabaseEndpoint(
        id=_id(),
        asset_id="a1",
        credential_ref=_cred(),
        host="oracle.internal",
        port=1521,
        database="PROD",
        driver="oracle",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, DatabaseEndpoint)
    assert saved.type == EndpointType.DATABASE
    assert saved.host == "oracle.internal"
    assert saved.port == 1521


@pytest.mark.asyncio
async def test_provision_rest_api_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = RestApiEndpoint(
        id=_id(),
        asset_id="a2",
        credential_ref=_cred(),
        base_url="https://api.example.com",
        auth_type="bearer",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, RestApiEndpoint)
    assert saved.type == EndpointType.REST_API
    assert saved.base_url == "https://api.example.com"


@pytest.mark.asyncio
async def test_provision_sftp_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = SftpEndpoint(
        id=_id(),
        asset_id="a3",
        credential_ref=_cred(),
        host="sftp.example.com",
        port=22,
        root_path="/exports",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, SftpEndpoint)
    assert saved.type == EndpointType.SFTP


@pytest.mark.asyncio
async def test_provision_cloud_bucket_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = CloudBucketEndpoint(
        id=_id(),
        asset_id="a4",
        credential_ref=_cred(),
        provider="gcs",
        bucket="raw-data-prod",
        region="us-central1",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, CloudBucketEndpoint)
    assert saved.type == EndpointType.CLOUD_BUCKET
    assert saved.provider == "gcs"


@pytest.mark.asyncio
async def test_provision_etl_flow_endpoint() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    ep = EtlFlowEndpoint(
        id=_id(),
        asset_id="a5",
        credential_ref=_cred(),
        tool="fivetran",
        flow_id="connector-abc123",
    )
    saved = await service.provision(ep)
    assert isinstance(saved, EtlFlowEndpoint)
    assert saved.type == EndpointType.ETL_FLOW
    assert saved.flow_id == "connector-abc123"


@pytest.mark.asyncio
async def test_credential_ref_validates_on_construction() -> None:
    with pytest.raises(ValueError, match="cannot be empty"):
        CredentialReference("")


@pytest.mark.asyncio
async def test_find_for_asset_returns_none_when_not_provisioned() -> None:
    service = EndpointService(repo=FakeEndpointRepository())
    result = await service.find_for_asset("nonexistent-asset")
    assert result is None
