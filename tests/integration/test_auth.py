from __future__ import annotations

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import ServerInfo, start_test_server, stop_test_server

SESSIONS = {
    "valid-user": {"userId": "user-1", "roles": ["user"]},
    "valid-admin": {"userId": "admin-1", "roles": ["admin"]},
}


@pytest.fixture
async def auth_server():
    info = await start_test_server(auth={"sessions": SESSIONS})
    yield info
    await stop_test_server(info)


@pytest.fixture
async def auth_server_with_items():
    info = await start_test_server(
        auth={"sessions": SESSIONS},
        buckets=[{"name": "items", "schema": {"value": {"type": "number", "required": True}}}],
    )
    yield info
    await stop_test_server(info)


# ── welcome.requiresAuth ─────────────────────────────────────────────


async def test_requires_auth_true(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    welcome = await client.connect()
    assert welcome.requires_auth is True
    await client.disconnect()


async def test_requires_auth_false(test_server: ServerInfo):
    client = NoexClient(test_server.url, ClientOptions(reconnect=False))
    welcome = await client.connect()
    assert welcome.requires_auth is False
    await client.disconnect()


# ── login ─────────────────────────────────────────────────────────────


async def test_login_valid_token(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    session = await client.auth.login("valid-user")
    assert session["userId"] == "user-1"
    assert session["roles"] == ["user"]

    await client.disconnect()


async def test_login_invalid_token(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    with pytest.raises(NoexClientError) as exc_info:
        await client.auth.login("bad-token")
    assert exc_info.value.code == "UNAUTHORIZED"

    await client.disconnect()


# ── logout ────────────────────────────────────────────────────────────


async def test_logout(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    await client.auth.login("valid-user")
    await client.auth.logout()

    who = await client.auth.whoami()
    assert who is None

    await client.disconnect()


# ── whoami ────────────────────────────────────────────────────────────


async def test_whoami_authenticated(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    await client.auth.login("valid-admin")
    session = await client.auth.whoami()

    assert session is not None
    assert session["userId"] == "admin-1"
    assert session["roles"] == ["admin"]

    await client.disconnect()


async def test_whoami_unauthenticated(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    session = await client.auth.whoami()
    assert session is None

    await client.disconnect()


# ── auto-login ────────────────────────────────────────────────────────


async def test_auto_login_with_token(auth_server: ServerInfo):
    client = NoexClient(
        auth_server.url,
        ClientOptions(
            reconnect=False,
            auth=__import__("noex_client").AuthOptions(token="valid-user"),
        ),
    )

    welcome = await client.connect()
    assert welcome.requires_auth is True

    session = await client.auth.whoami()
    assert session is not None
    assert session["userId"] == "user-1"

    await client.disconnect()


async def test_auto_login_skipped_when_not_required(test_server: ServerInfo):
    from noex_client import AuthOptions

    client = NoexClient(
        test_server.url,
        ClientOptions(
            reconnect=False,
            auth=AuthOptions(token="valid-user"),
        ),
    )

    welcome = await client.connect()
    assert welcome.requires_auth is False
    # No error — auto-login was skipped

    await client.disconnect()


# ── operations gated by auth ─────────────────────────────────────────


async def test_reject_operations_without_login(auth_server_with_items: ServerInfo):
    client = NoexClient(auth_server_with_items.url, ClientOptions(reconnect=False))
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.store.bucket("items").insert({"value": 42})

    await client.disconnect()


async def test_allow_operations_after_login(auth_server_with_items: ServerInfo):
    client = NoexClient(auth_server_with_items.url, ClientOptions(reconnect=False))
    await client.connect()

    await client.auth.login("valid-user")
    record = await client.store.bucket("items").insert({"value": 42})
    assert record["value"] == 42

    await client.disconnect()


async def test_reject_operations_after_logout(auth_server_with_items: ServerInfo):
    client = NoexClient(auth_server_with_items.url, ClientOptions(reconnect=False))
    await client.connect()

    await client.auth.login("valid-user")
    await client.auth.logout()

    with pytest.raises(NoexClientError):
        await client.store.bucket("items").insert({"value": 1})

    await client.disconnect()


# ── re-authentication ─────────────────────────────────────────────────


async def test_relogin_different_token(auth_server: ServerInfo):
    client = NoexClient(auth_server.url, ClientOptions(reconnect=False))
    await client.connect()

    await client.auth.login("valid-user")
    session = await client.auth.whoami()
    assert session["userId"] == "user-1"

    await client.auth.login("valid-admin")
    session = await client.auth.whoami()
    assert session["userId"] == "admin-1"

    await client.disconnect()
