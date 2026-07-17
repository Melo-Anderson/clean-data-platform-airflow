from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

from app.domain.endpoints.endpoint_type import EndpointType
from app.domain.shared.auditable import Auditable
from app.domain.shared.value_objects import CredentialReference


@dataclass(kw_only=True)
class Endpoint(ABC, Auditable):
    """
    Abstract base for all data source/destination connections.

    Managed exclusively by SRE. Business users see only id and type.
    Subclasses define type-specific connection fields.

    credential_ref is a Value Object — never the actual secret value.
    """

    id: str
    name: str
    credential_ref: CredentialReference
    technical_description: str = ""

    @property
    @abstractmethod
    def type(self) -> EndpointType:
        """Return the EndpointType discriminator for this subclass."""


@dataclass(kw_only=True)
class DatabaseEndpoint(Endpoint):
    """
    Endpoint for relational databases (Oracle, PostgreSQL, MySQL, etc.).

    All connection details (host, port, database, driver) reside in the Vault
    and are resolved dynamically via credential_ref.
    """

    @property
    def type(self) -> EndpointType:
        return EndpointType.DATABASE


@dataclass(kw_only=True)
class RestApiEndpoint(Endpoint):
    """
    Endpoint for REST APIs.

    auth_type: "bearer" | "api_key" | "oauth2" | "basic"
    headers_ref: optional reference to custom headers stored in Vault.
    """

    base_url: str = ""
    auth_type: str = ""
    headers_ref: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.REST_API


@dataclass(kw_only=True)
class SftpEndpoint(Endpoint):
    """
    Endpoint for SFTP servers.

    private_key_ref: reference to the SSH private key in Vault.
    """

    host: str = ""
    port: int = 22
    root_path: str = "/"
    private_key_ref: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.SFTP


@dataclass(kw_only=True)
class CloudBucketEndpoint(Endpoint):
    """
    Endpoint for cloud object storage (S3, GCS, Azure Blob).

    provider: "s3" | "gcs" | "azure"
    """

    provider: str = ""  # "s3" | "gcs" | "azure"
    bucket: str = ""
    prefix: str = ""
    region: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.CLOUD_BUCKET


@dataclass(kw_only=True)
class EtlFlowEndpoint(Endpoint):
    """
    Endpoint for managed ETL tools (Fivetran, Airbyte, etc.).

    tool: "fivetran" | "airbyte"
    flow_id: the connector / sync id within the ETL tool.
    """

    tool: str = ""  # "fivetran" | "airbyte"
    flow_id: str = ""

    @property
    def type(self) -> EndpointType:
        return EndpointType.ETL_FLOW


@dataclass(kw_only=True)
class NoSqlEndpoint(Endpoint):
    """
    Endpoint for NoSQL document databases (MongoDB, DocumentDB, etc.).

    Connection URI (e.g. mongodb://host:27017/dbname) is stored in Vault
    and resolved at discovery time via credential_ref.
    """

    @property
    def type(self) -> EndpointType:
        return EndpointType.NOSQL


# Convenience union type for type hints
AnyEndpoint = (
    DatabaseEndpoint
    | RestApiEndpoint
    | SftpEndpoint
    | CloudBucketEndpoint
    | EtlFlowEndpoint
    | NoSqlEndpoint
)
