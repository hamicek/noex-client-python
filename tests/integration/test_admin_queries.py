from __future__ import annotations

import asyncio

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import ServerInfo, start_test_server, stop_test_server

SESSIONS = {
    "admin": {"userId": "admin-1", "roles": ["admin"]},
    "writer": {"userId": "writer-1", "roles": ["writer"]},
    "reader": {"userId": "reader-1", "roles": ["reader"]},
}


@pytest.fixture
async def query_server():
    info = await start_test_server(
        auth={"sessions": SESSIONS},
        buckets=[
            {"name": "users", "schema": {
                "name": {"type": "string", "required": True},
                "role": {"type": "string"},
                "active": {"type": "boolean"},
            }},
            {"name": "items", "schema": {"title": {"type": "string", "required": True}}},
        ],
    )
    yield info
    await stop_test_server(info)


def _make_client(url: str, token: str | None = None) -> NoexClient:
    from noex_client import AuthOptions

    if token:
        return NoexClient(url, ClientOptions(reconnect=False, auth=AuthOptions(token=token)))
    return NoexClient(url, ClientOptions(reconnect=False))


# ── defineQuery ──────────────────────────────────────────────────────


async def test_define_query(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    result = await client.store.define_query("all-users", {"bucket": "users"})

    assert result["name"] == "all-users"
    assert result["defined"] is True

    await client.disconnect()


async def test_define_and_subscribe(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.bucket("users").insert({"name": "Alice", "role": "admin", "active": True})
    await client.store.bucket("users").insert({"name": "Bob", "role": "user", "active": False})

    await client.store.define_query("active-users", {
        "bucket": "users",
        "filter": {"active": True},
    })

    received = []
    unsub = await client.store.subscribe("active-users", lambda data: received.append(data))

    assert len(received) == 1
    assert isinstance(received[0], list)
    assert len(received[0]) == 1
    assert received[0][0]["name"] == "Alice"

    unsub()
    await client.disconnect()


async def test_reactive_push_after_insert(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.define_query("all-items", {"bucket": "items"})

    snapshots: list = []
    unsub = await client.store.subscribe("all-items", lambda data: snapshots.append(data))

    assert len(snapshots) == 1
    assert snapshots[0] == []

    await client.store.bucket("items").insert({"title": "Widget"})
    await asyncio.sleep(0.2)

    assert len(snapshots) >= 2
    latest = snapshots[-1]
    assert len(latest) == 1
    assert latest[0]["title"] == "Widget"

    unsub()
    await client.disconnect()


async def test_define_query_duplicate(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.define_query("dup", {"bucket": "users"})

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.define_query("dup", {"bucket": "users"})
    assert exc_info.value.code == "ALREADY_EXISTS"

    await client.disconnect()


async def test_define_query_nonexistent_bucket(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.define_query("bad", {"bucket": "nonexistent"})
    assert exc_info.value.code == "BUCKET_NOT_DEFINED"

    await client.disconnect()


# ── undefineQuery ────────────────────────────────────────────────────


async def test_undefine_query(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.define_query("temp", {"bucket": "users"})

    result = await client.store.undefine_query("temp")
    assert result["name"] == "temp"
    assert result["undefined"] is True

    await client.disconnect()


async def test_undefine_then_subscribe_fails(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.define_query("doomed", {"bucket": "users"})
    await client.store.undefine_query("doomed")

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.subscribe("doomed", lambda data: None)
    assert exc_info.value.code == "QUERY_NOT_DEFINED"

    await client.disconnect()


async def test_undefine_nonexistent(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.undefine_query("no-such-query")
    assert exc_info.value.code == "QUERY_NOT_DEFINED"

    await client.disconnect()


# ── listQueries ──────────────────────────────────────────────────────


async def test_list_queries_empty(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    result = await client.store.list_queries()
    assert result["queries"] == []

    await client.disconnect()


async def test_list_queries_with_entries(query_server: ServerInfo):
    client = _make_client(query_server.url, "admin")
    await client.connect()

    await client.store.define_query("q1", {"bucket": "users", "filter": {"active": True}})
    await client.store.define_query("q2", {"bucket": "users", "sort": {"name": "asc"}, "limit": 5})

    result = await client.store.list_queries()
    assert len(result["queries"]) == 2

    names = [q["name"] for q in result["queries"]]
    assert "q1" in names
    assert "q2" in names

    await client.disconnect()


# ── Tier enforcement ─────────────────────────────────────────────────


async def test_reject_define_from_writer(query_server: ServerInfo):
    client = _make_client(query_server.url, "writer")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.define_query("forbidden", {"bucket": "users"})
    assert exc_info.value.code == "FORBIDDEN"

    await client.disconnect()


async def test_reject_list_from_reader(query_server: ServerInfo):
    client = _make_client(query_server.url, "reader")
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.store.list_queries()

    await client.disconnect()


# ── No auth mode ─────────────────────────────────────────────────────


async def test_define_query_no_auth():
    info = await start_test_server(
        buckets=[{"name": "items", "schema": {"value": {"type": "string"}}}],
    )
    try:
        client = _make_client(info.url)
        await client.connect()

        result = await client.store.define_query("all-items", {"bucket": "items"})
        assert result["name"] == "all-items"
        assert result["defined"] is True

        await client.disconnect()
    finally:
        await stop_test_server(info)
