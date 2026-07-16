class UnsupportedEndpointError(Exception):
    """Raised when an endpoint type has no registered DiscoveryRunner."""

    pass
