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
) -> ServerInfo:
    """Start a noex-server subprocess, wait for the URL output, and return it."""
    config: dict[str, Any] = {}
    if buckets:
        config["buckets"] = buckets
    if queries:
        config["queries"] = queries

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
