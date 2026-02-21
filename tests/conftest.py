from __future__ import annotations

import asyncio
import json
import signal
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator

import pytest
import pytest_asyncio

from noex_client import ClientOptions, NoexClient

FIXTURES_DIR = Path(__file__).parent / "fixtures"
SERVER_SCRIPT = FIXTURES_DIR / "start-server.mjs"



@dataclass
class ServerInfo:
    url: str
    process: asyncio.subprocess.Process


async def start_test_server(
    *,
    buckets: list[dict[str, Any]] | None = None,
    queries: list[dict[str, Any]] | None = None,
    auth: dict[str, Any] | None = None,
    audit: dict[str, Any] | None = None,
    rules: bool | None = None,
    logic: bool | None = None,
    port: int | None = None,
) -> ServerInfo:
    """Start a noex-server subprocess, wait for the URL output, and return it."""
    config: dict[str, Any] = {}
    if buckets:
        config["buckets"] = buckets
    if queries:
        config["queries"] = queries
    if auth is not None:
        config["auth"] = auth
    if audit is not None:
        config["audit"] = audit
    if rules is not None:
        config["rules"] = rules
    if logic is not None:
        config["logic"] = logic
    if port is not None:
        config["port"] = port

    config_json = json.dumps(config)

    proc = await asyncio.create_subprocess_exec(
        "node",
        str(SERVER_SCRIPT),
        config_json,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=str(FIXTURES_DIR),
    )

    assert proc.stdout is not None

    try:
        line = await asyncio.wait_for(proc.stdout.readline(), timeout=10)
    except asyncio.TimeoutError:
        proc.kill()
        stderr = b""
        if proc.stderr:
            stderr = await proc.stderr.read()
        raise RuntimeError(
            f"Test server did not output URL within 10s. stderr: {stderr.decode()}"
        )

    url = line.decode().strip()
    if not url.startswith("ws://"):
        proc.kill()
        stderr = b""
        if proc.stderr:
            stderr = await proc.stderr.read()
        raise RuntimeError(
            f"Unexpected server output: {url!r}. stderr: {stderr.decode()}"
        )

    return ServerInfo(url=url, process=proc)


async def stop_test_server(info: ServerInfo) -> None:
    """Gracefully stop the test server subprocess."""
    if info.process.returncode is not None:
        return

    info.process.send_signal(signal.SIGTERM)
    try:
        await asyncio.wait_for(info.process.wait(), timeout=5)
    except asyncio.TimeoutError:
        info.process.kill()
        await info.process.wait()


@pytest_asyncio.fixture
async def test_server() -> AsyncIterator[ServerInfo]:
    """Fixture that starts a test server and stops it after the test."""
    info = await start_test_server()
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def test_server_with_buckets() -> AsyncIterator[ServerInfo]:
    """Fixture with pre-defined buckets for CRUD tests."""
    info = await start_test_server(
        buckets=[
            {"name": "users", "schema": {"name": {"type": "string", "required": True}}},
            {"name": "items", "schema": {"value": {"type": "number", "required": True}}},
        ],
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def client(test_server: ServerInfo) -> AsyncIterator[NoexClient]:
    """Fixture that creates a connected NoexClient."""
    c = NoexClient(
        test_server.url,
        ClientOptions(reconnect=False),
    )
    await c.connect()
    yield c
    if c.is_connected:
        await c.disconnect()


@pytest_asyncio.fixture
async def client_with_buckets(
    test_server_with_buckets: ServerInfo,
) -> AsyncIterator[NoexClient]:
    """Fixture that creates a connected NoexClient with pre-defined buckets."""
    c = NoexClient(
        test_server_with_buckets.url,
        ClientOptions(reconnect=False),
    )
    await c.connect()
    yield c
    if c.is_connected:
        await c.disconnect()


@pytest_asyncio.fixture
async def test_server_with_queries() -> AsyncIterator[ServerInfo]:
    """Fixture with pre-defined buckets and reactive queries for subscription tests."""
    info = await start_test_server(
        buckets=[
            {
                "name": "users",
                "schema": {
                    "name": {"type": "string", "required": True},
                    "role": {"type": "string", "default": "user"},
                },
            },
        ],
        queries=[
            {"name": "all-users", "type": "all", "bucket": "users"},
            {"name": "users-by-role", "type": "where", "bucket": "users", "field": "role"},
            {"name": "user-count", "type": "count", "bucket": "users"},
        ],
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def client_with_queries(
    test_server_with_queries: ServerInfo,
) -> AsyncIterator[NoexClient]:
    """Fixture that creates a connected NoexClient with buckets and queries."""
    c = NoexClient(
        test_server_with_queries.url,
        ClientOptions(reconnect=False),
    )
    await c.connect()
    yield c
    if c.is_connected:
        await c.disconnect()


# ── Auth fixtures ────────────────────────────────────────────────────

STANDARD_SESSIONS = {
    "admin": {"userId": "admin-1", "roles": ["admin"]},
    "writer": {"userId": "writer-1", "roles": ["writer"]},
    "reader": {"userId": "reader-1", "roles": ["reader"]},
}

IDENTITY_SECRET = "test-identity-secret"


@pytest_asyncio.fixture
async def test_server_with_auth() -> AsyncIterator[ServerInfo]:
    """Fixture with session-based auth configured."""
    info = await start_test_server(
        auth={"sessions": STANDARD_SESSIONS},
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def test_server_with_auth_and_buckets() -> AsyncIterator[ServerInfo]:
    """Fixture with session-based auth and pre-defined buckets."""
    info = await start_test_server(
        auth={"sessions": STANDARD_SESSIONS},
        buckets=[
            {"name": "items", "schema": {"value": {"type": "number", "required": True}}},
            {"name": "users", "schema": {"name": {"type": "string", "required": True}}},
        ],
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def test_server_with_identity() -> AsyncIterator[ServerInfo]:
    """Fixture with built-in identity auth configured."""
    info = await start_test_server(
        auth={"builtIn": True, "adminSecret": IDENTITY_SECRET},
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def test_server_with_audit() -> AsyncIterator[ServerInfo]:
    """Fixture with auth + audit configured."""
    info = await start_test_server(
        auth={"sessions": STANDARD_SESSIONS},
        audit={},
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def test_server_with_procedures() -> AsyncIterator[ServerInfo]:
    """Fixture with auth + buckets for procedure tests."""
    info = await start_test_server(
        auth={"sessions": STANDARD_SESSIONS},
        buckets=[
            {
                "name": "orders",
                "schema": {
                    "status": {"type": "string"},
                    "total": {"type": "number"},
                },
            },
            {
                "name": "items",
                "schema": {
                    "orderId": {"type": "string", "required": True},
                    "price": {"type": "number", "required": True},
                },
            },
        ],
    )
    yield info
    await stop_test_server(info)


# ── Logic fixtures ──────────────────────────────────────────────────


@pytest_asyncio.fixture
async def test_server_with_logic() -> AsyncIterator[ServerInfo]:
    """Fixture with logic engine and buckets for logic tests."""
    info = await start_test_server(
        logic=True,
        buckets=[
            {
                "name": "items",
                "schema": {
                    "name": {"type": "string"},
                    "qty": {"type": "number"},
                    "price": {"type": "number"},
                },
            },
            {
                "name": "accounts",
                "schema": {
                    "name": {"type": "string"},
                    "balance": {"type": "number"},
                },
            },
            {
                "name": "customers",
                "schema": {
                    "name": {"type": "string"},
                },
            },
            {
                "name": "invoices",
                "schema": {
                    "customerId": {"type": "string"},
                    "total": {"type": "number"},
                    "issueDate": {"type": "string"},
                    "dueDate": {"type": "string"},
                },
            },
        ],
    )
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def client_with_logic(
    test_server_with_logic: ServerInfo,
) -> AsyncIterator[NoexClient]:
    """Fixture that creates a connected NoexClient with logic engine."""
    c = NoexClient(
        test_server_with_logic.url,
        ClientOptions(reconnect=False),
    )
    await c.connect()
    yield c
    if c.is_connected:
        await c.disconnect()


@pytest_asyncio.fixture
async def test_server_no_logic() -> AsyncIterator[ServerInfo]:
    """Fixture with logic explicitly disabled."""
    info = await start_test_server(logic=False)
    yield info
    await stop_test_server(info)


@pytest_asyncio.fixture
async def client_no_logic(
    test_server_no_logic: ServerInfo,
) -> AsyncIterator[NoexClient]:
    """Fixture that creates a connected NoexClient without logic engine."""
    c = NoexClient(
        test_server_no_logic.url,
        ClientOptions(reconnect=False),
    )
    await c.connect()
    yield c
    if c.is_connected:
        await c.disconnect()
