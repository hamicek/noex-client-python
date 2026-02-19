from __future__ import annotations

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import ServerInfo

SESSIONS = {
    "admin": {"userId": "admin-1", "roles": ["admin"]},
    "writer": {"userId": "writer-1", "roles": ["writer"]},
    "reader": {"userId": "reader-1", "roles": ["reader"]},
}


def _make_client(url: str, token: str | None = None) -> NoexClient:
    from noex_client import AuthOptions

    if token:
        return NoexClient(url, ClientOptions(reconnect=False, auth=AuthOptions(token=token)))
    return NoexClient(url, ClientOptions(reconnect=False))


# ── defineBucket ─────────────────────────────────────────────────────


async def test_define_bucket_and_crud(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    result = await client.store.define_bucket("users", {
        "key": "id",
        "schema": {
            "id": {"type": "string", "generated": "uuid"},
            "name": {"type": "string", "required": True},
            "email": {"type": "string"},
        },
        "indexes": ["email"],
    })

    assert result["name"] == "users"
    assert result["created"] is True

    record = await client.store.bucket("users").insert({"name": "Alice", "email": "alice@test.com"})
    assert record["name"] == "Alice"
    assert isinstance(record["id"], str)

    fetched = await client.store.bucket("users").get(record["id"])
    assert fetched is not None
    assert fetched["email"] == "alice@test.com"

    await client.disconnect()


async def test_define_bucket_duplicate(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    config = {
        "key": "id",
        "schema": {
            "id": {"type": "string", "generated": "uuid"},
            "title": {"type": "string", "required": True},
        },
    }
    await client.store.define_bucket("posts", config)

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.define_bucket("posts", config)
    assert exc_info.value.code == "ALREADY_EXISTS"

    await client.disconnect()


async def test_define_bucket_no_auth(test_server: ServerInfo):
    client = _make_client(test_server.url)
    await client.connect()

    result = await client.store.define_bucket("open", {
        "key": "id",
        "schema": {
            "id": {"type": "string", "generated": "uuid"},
            "value": {"type": "string"},
        },
    })

    assert result["created"] is True

    record = await client.store.bucket("open").insert({"value": "test"})
    assert record["value"] == "test"

    await client.disconnect()


# ── dropBucket ───────────────────────────────────────────────────────


async def test_drop_bucket(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    await client.store.define_bucket("temp", {
        "key": "id",
        "schema": {"id": {"type": "string", "generated": "uuid"}, "data": {"type": "string"}},
    })

    result = await client.store.drop_bucket("temp")
    assert result["name"] == "temp"
    assert result["dropped"] is True

    await client.disconnect()


async def test_drop_nonexistent_bucket(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.drop_bucket("no-such-bucket")
    assert exc_info.value.code == "BUCKET_NOT_DEFINED"

    await client.disconnect()


# ── getBucketSchema ──────────────────────────────────────────────────


async def test_get_bucket_schema(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    await client.store.define_bucket("docs", {
        "key": "id",
        "schema": {
            "id": {"type": "string", "generated": "uuid"},
            "content": {"type": "string", "required": True},
        },
        "indexes": ["content"],
    })

    result = await client.store.get_bucket_schema("docs")
    assert result["name"] == "docs"
    assert result["config"]["key"] == "id"
    assert "id" in result["config"]["schema"]
    assert "content" in result["config"]["schema"]
    assert "content" in result["config"]["indexes"]

    await client.disconnect()


# ── updateBucket ─────────────────────────────────────────────────────


async def test_update_bucket_add_fields(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "admin")
    await client.connect()

    await client.store.define_bucket("profiles", {
        "key": "id",
        "schema": {
            "id": {"type": "string", "generated": "uuid"},
            "name": {"type": "string", "required": True},
        },
    })

    result = await client.store.update_bucket("profiles", {
        "addFields": {
            "phone": {"type": "string"},
            "age": {"type": "number", "min": 0},
        },
    })

    assert result["name"] == "profiles"
    assert result["updated"] is True

    record = await client.store.bucket("profiles").insert({
        "name": "Bob",
        "phone": "+420123456789",
        "age": 30,
    })
    assert record["phone"] == "+420123456789"
    assert record["age"] == 30

    await client.disconnect()


# ── Tier enforcement ─────────────────────────────────────────────────


async def test_reject_define_from_writer(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "writer")
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.store.define_bucket("forbidden", {
            "key": "id",
            "schema": {"id": {"type": "string", "generated": "uuid"}},
        })
    assert exc_info.value.code == "FORBIDDEN"

    await client.disconnect()


async def test_reject_drop_from_reader(test_server_with_auth: ServerInfo):
    client = _make_client(test_server_with_auth.url, "reader")
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.store.drop_bucket("anything")

    await client.disconnect()
