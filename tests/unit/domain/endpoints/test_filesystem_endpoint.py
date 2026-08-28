from __future__ import annotations

from app.domain.endpoints.endpoint import FileSystemEndpoint
from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.value_objects import CredentialReference


def test_create_file_system_endpoint() -> None:
    endpoint = FileSystemEndpoint(
        id="ep-fs-001",
        name="local-data-lake",
        credential_ref=CredentialReference("vault/storage/local"),
        root_path="/var/data/landing",
        technical_description="Local mounted data volume",
    )
    assert endpoint.id == "ep-fs-001"
    assert endpoint.name == "local-data-lake"
    assert endpoint.type == EndpointType.FILE_SYSTEM
    assert endpoint.root_path == "/var/data/landing"
    assert endpoint.credential_ref.path == "vault/storage/local"


def test_file_system_endpoint_default_root_path() -> None:
    endpoint = FileSystemEndpoint(
        id="ep-fs-002",
        name="default-fs",
        credential_ref=CredentialReference("vault/none"),
    )
    assert endpoint.root_path == ""
