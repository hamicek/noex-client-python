from __future__ import annotations

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import ServerInfo, start_test_server, stop_test_server

SESSIONS = {
    "admin": {"userId": "admin-1", "roles": ["admin"]},
    "writer": {"userId": "writer-1", "roles": ["writer"]},
    "reader": {"userId": "reader-1", "roles": ["reader"]},
}


@pytest.fixture
async def audit_server():
    info = await start_test_server(
        auth={"sessions": SESSIONS},
        audit={},
    )
    yield info
    await stop_test_server(info)


@pytest.fixture
async def audit_server_with_buckets():
    info = await start_test_server(
        auth={"sessions": SESSIONS},
        audit={"tiers": ["admin", "write"]},
        buckets=[{"name": "items", "schema": {"value": {"type": "number", "required": True}}}],
    )
    yield info
    await stop_test_server(info)


def _make_client(url: str, token: str | None = None) -> NoexClient:
    from noex_client import AuthOptions

    opts = ClientOptions(reconnect=False)
    if token:
        opts = ClientOptions(reconnect=False, auth=AuthOptions(token=token))
    return NoexClient(url, opts)


# ── Basic query ──────────────────────────────────────────────────────


async def test_audit_query_returns_entries(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "admin")
    await client.connect()

    # First query generates an audit entry
    await client.audit.query()
    # Second query can see the first one
    entries = await client.audit.query()

    assert len(entries) >= 1
    audit_entry = next((e for e in entries if e["operation"] == "audit.query"), None)
    assert audit_entry is not None
    assert audit_entry["result"] == "success"
    assert audit_entry["userId"] == "admin-1"

    await client.disconnect()


async def test_audit_query_empty_filter(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "admin")
    await client.connect()

    entries = await client.audit.query({"operation": "store.insert"})
    assert entries == []

    await client.disconnect()


# ── Filtering ────────────────────────────────────────────────────────


async def test_filter_by_user_id(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "admin")
    await client.connect()

    await client.audit.query()

    entries = await client.audit.query({"userId": "admin-1"})
    assert len(entries) >= 1
    assert all(e["userId"] == "admin-1" for e in entries)

    await client.disconnect()


async def test_filter_by_operation(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "admin")
    await client.connect()

    await client.audit.query()
    await client.audit.query()

    entries = await client.audit.query({"operation": "audit.query"})
    assert len(entries) >= 2
    assert all(e["operation"] == "audit.query" for e in entries)

    await client.disconnect()


async def test_filter_with_limit(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "admin")
    await client.connect()

    await client.audit.query()
    await client.audit.query()
    await client.audit.query()

    entries = await client.audit.query({"limit": 2})
    assert len(entries) == 2

    await client.disconnect()


# ── Access control ───────────────────────────────────────────────────


async def test_reject_from_writer(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "writer")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.audit.query()
    assert exc_info.value.code == "FORBIDDEN"

    await client.disconnect()


async def test_reject_from_reader(audit_server: ServerInfo):
    client = _make_client(audit_server.url, "reader")
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.audit.query()

    await client.disconnect()


# ── Write operations audited ─────────────────────────────────────────


async def test_audit_write_operations(audit_server_with_buckets: ServerInfo):
    client = _make_client(audit_server_with_buckets.url, "admin")
    await client.connect()

    await client.store.bucket("items").insert({"value": 42})

    entries = await client.audit.query({"operation": "store.insert"})
    assert len(entries) >= 1
    assert entries[0]["resource"] == "items"

    await client.disconnect()


# ── No audit config ──────────────────────────────────────────────────


async def test_reject_when_no_audit(test_server_with_auth: ServerInfo):
    from noex_client import AuthOptions

    client = NoexClient(
        test_server_with_auth.url,
        ClientOptions(reconnect=False, auth=AuthOptions(token="admin")),
    )
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.audit.query()

    await client.disconnect()
