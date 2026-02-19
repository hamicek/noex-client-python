from __future__ import annotations


class NoexClientError(Exception):
    """Base error for all noex client errors."""

    def __init__(self, code: str, message: str, details: object = None) -> None:
        super().__init__(message)
        self.code = code
        self.details = details


class RequestTimeoutError(NoexClientError):
    """Request timed out waiting for a server response."""

    def __init__(self, message: str) -> None:
        super().__init__("TIMEOUT", message)


class DisconnectedError(NoexClientError):
    """Client is not connected or connection was lost."""

    def __init__(self, message: str = "Not connected") -> None:
        super().__init__("DISCONNECTED", message)
