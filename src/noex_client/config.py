from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal, TypeAlias

ConnectionState: TypeAlias = Literal[
    "connecting", "connected", "reconnecting", "disconnected"
]
TransportState: TypeAlias = Literal[
    "idle", "connecting", "connected", "disconnected"
]

DEFAULT_REQUEST_TIMEOUT_MS = 10_000
DEFAULT_CONNECT_TIMEOUT_MS = 5_000


@dataclass(frozen=True)
class ReconnectOptions:
    max_retries: float = float("inf")
    initial_delay_ms: int = 1_000
    max_delay_ms: int = 30_000
    backoff_multiplier: float = 2.0
    jitter_ms: int = 500


@dataclass(frozen=True)
class CredentialOptions:
    username: str
    password: str


@dataclass(frozen=True)
class AuthOptions:
    token: str | None = None
    credentials: CredentialOptions | None = None


@dataclass(frozen=True)
class ClientOptions:
    auth: AuthOptions | None = None
    reconnect: bool | ReconnectOptions = True
    request_timeout_ms: int = DEFAULT_REQUEST_TIMEOUT_MS
    connect_timeout_ms: int = DEFAULT_CONNECT_TIMEOUT_MS
    heartbeat: bool = True


@dataclass(frozen=True)
class WelcomeInfo:
    version: str
    server_time: int
    requires_auth: bool
