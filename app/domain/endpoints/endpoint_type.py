from __future__ import annotations

from enum import StrEnum


class EndpointType(StrEnum):
    """Supported endpoint (connection) types. Each maps to a typed Endpoint subclass."""

    DATABASE = "database"
    REST_API = "rest_api"
    SFTP = "sftp"
    CLOUD_BUCKET = "cloud_bucket"
    ETL_FLOW = "etl_flow"
    NOSQL = "nosql"
