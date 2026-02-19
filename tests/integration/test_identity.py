from __future__ import annotations

import pytest

from noex_client import ClientOptions, NoexClient, NoexClientError
from tests.conftest import IDENTITY_SECRET, ServerInfo, start_test_server, stop_test_server


@pytest.fixture
async def identity_server():
    info = await start_test_server(
        auth={"builtIn": True, "adminSecret": IDENTITY_SECRET},
    )
    yield info
    await stop_test_server(info)


async def admin_client(url: str) -> NoexClient:
    """Connect and login as superadmin."""
    c = NoexClient(url, ClientOptions(reconnect=False))
    await c.connect()
    await c.identity.login_with_secret(IDENTITY_SECRET)
    return c


# ── Auth operations ──────────────────────────────────────────────────


async def test_login_with_secret(identity_server: ServerInfo):
    client = NoexClient(identity_server.url, ClientOptions(reconnect=False))
    await client.connect()

    result = await client.identity.login_with_secret(IDENTITY_SECRET)

    assert result["token"]
    assert isinstance(result["expiresAt"], int)
    assert result["user"]["id"] == "__superadmin__"
    assert result["user"]["username"] == "__superadmin__"
    assert result["user"]["roles"] == ["superadmin"]

    await client.disconnect()


async def test_invalid_secret(identity_server: ServerInfo):
    client = NoexClient(identity_server.url, ClientOptions(reconnect=False))
    await client.connect()

    with pytest.raises(NoexClientError):
        await client.identity.login_with_secret("wrong")

    await client.disconnect()


async def test_login_with_credentials(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)

    await c.identity.create_user({
        "username": "alice",
        "password": "Str0ngP@ss!",
        "displayName": "Alice",
    })

    await c.identity.logout()
    result = await c.identity.login("alice", "Str0ngP@ss!")

    assert result["user"]["username"] == "alice"
    assert result["token"]

    await c.disconnect()


async def test_wrong_password(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    await c.identity.create_user({"username": "bob", "password": "Correct1!"})

    await c.identity.logout()
    with pytest.raises(NoexClientError):
        await c.identity.login("bob", "WrongPass1!")

    await c.disconnect()


async def test_whoami(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)

    who = await c.identity.whoami()
    assert who["authenticated"] is True
    assert who["userId"] == "__superadmin__"
    assert who["roles"] == ["superadmin"]

    await c.disconnect()


async def test_logout_clears_session(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    await c.identity.logout()

    with pytest.raises(NoexClientError):
        await c.identity.list_roles()

    await c.disconnect()


async def test_refresh_session(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    old_login = await c.identity.login_with_secret(IDENTITY_SECRET)
    refreshed = await c.identity.refresh_session()

    assert refreshed["token"] != old_login["token"]
    assert refreshed["user"]["id"] == "__superadmin__"

    await c.disconnect()


# ── User CRUD ────────────────────────────────────────────────────────


async def test_create_user(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)

    user = await c.identity.create_user({
        "username": "alice",
        "password": "P@ssw0rd!",
        "displayName": "Alice Doe",
        "email": "alice@example.com",
        "metadata": {"department": "eng"},
    })

    assert user["id"]
    assert user["username"] == "alice"
    assert user["displayName"] == "Alice Doe"
    assert user["email"] == "alice@example.com"
    assert user["enabled"] is True
    assert user["metadata"] == {"department": "eng"}

    await c.disconnect()


async def test_duplicate_username(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    await c.identity.create_user({"username": "alice", "password": "P@ssw0rd!"})

    with pytest.raises(NoexClientError):
        await c.identity.create_user({"username": "alice", "password": "Other1234!"})

    await c.disconnect()


async def test_get_user(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    created = await c.identity.create_user({"username": "bob", "password": "Secret123!"})

    fetched = await c.identity.get_user(created["id"])
    assert fetched["username"] == "bob"
    assert fetched["id"] == created["id"]

    await c.disconnect()


async def test_update_user(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    user = await c.identity.create_user({"username": "carol", "password": "Pass1234!"})

    updated = await c.identity.update_user(user["id"], {
        "displayName": "Carol Updated",
        "email": "carol@new.com",
    })

    assert updated["displayName"] == "Carol Updated"
    assert updated["email"] == "carol@new.com"

    await c.disconnect()


async def test_delete_user(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    user = await c.identity.create_user({"username": "dave", "password": "Pass1234!"})

    await c.identity.delete_user(user["id"])

    with pytest.raises(NoexClientError):
        await c.identity.get_user(user["id"])

    await c.disconnect()


async def test_list_users(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    await c.identity.create_user({"username": "user1", "password": "Pass1234!"})
    await c.identity.create_user({"username": "user2", "password": "Pass1234!"})

    result = await c.identity.list_users(page=1, page_size=10)

    assert len(result["users"]) == 2
    assert result["total"] == 2

    await c.disconnect()


async def test_enable_disable_user(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    user = await c.identity.create_user({"username": "eve", "password": "Pass1234!"})

    disabled = await c.identity.disable_user(user["id"])
    assert disabled["enabled"] is False

    enabled = await c.identity.enable_user(user["id"])
    assert enabled["enabled"] is True

    await c.disconnect()


# ── Password operations ──────────────────────────────────────────────


async def test_admin_reset_password(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    user = await c.identity.create_user({"username": "grace", "password": "OldPass1!"})

    await c.identity.reset_password(user["id"], "ResetPass1!")

    await c.identity.logout()
    result = await c.identity.login("grace", "ResetPass1!")
    assert result["user"]["username"] == "grace"

    await c.disconnect()


# ── Role management ──────────────────────────────────────────────────


async def test_list_system_roles(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    roles = await c.identity.list_roles()

    names = sorted(r["name"] for r in roles)
    assert names == ["admin", "reader", "superadmin", "writer"]
    assert all(r["system"] for r in roles)

    await c.disconnect()


async def test_create_custom_role(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)

    role = await c.identity.create_role({
        "name": "editor",
        "description": "Can edit content",
        "permissions": [{"allow": ["store.insert", "store.update"]}],
    })

    assert role["name"] == "editor"
    assert role["system"] is False
    assert len(role["permissions"]) == 1

    await c.disconnect()


async def test_assign_and_remove_role(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    user = await c.identity.create_user({"username": "hank", "password": "Pass1234!"})

    await c.identity.assign_role(user["id"], "writer")
    roles = await c.identity.get_user_roles(user["id"])
    assert "writer" in [r["name"] for r in roles]

    await c.identity.remove_role(user["id"], "writer")
    roles = await c.identity.get_user_roles(user["id"])
    assert "writer" not in [r["name"] for r in roles]

    await c.disconnect()


async def test_delete_custom_role(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    role = await c.identity.create_role({"name": "temporary"})

    await c.identity.delete_role(role["id"])

    roles = await c.identity.list_roles()
    assert not any(r["name"] == "temporary" for r in roles)

    await c.disconnect()


async def test_cannot_delete_system_role(identity_server: ServerInfo):
    c = await admin_client(identity_server.url)
    roles = await c.identity.list_roles()
    admin_role = next(r for r in roles if r["name"] == "admin")

    with pytest.raises(NoexClientError):
        await c.identity.delete_role(admin_role["id"])

    await c.disconnect()


# ── Auto-login with credentials ──────────────────────────────────────


async def test_auto_login_with_credentials(identity_server: ServerInfo):
    from noex_client import AuthOptions, CredentialOptions

    # Create user via admin
    admin = NoexClient(identity_server.url, ClientOptions(reconnect=False))
    await admin.connect()
    await admin.identity.login_with_secret(IDENTITY_SECRET)
    auto_user = await admin.identity.create_user({"username": "auto", "password": "AutoP@ss1"})
    await admin.identity.assign_role(auto_user["id"], "writer")
    await admin.disconnect()

    # Connect with credentials — should auto-login
    client = NoexClient(
        identity_server.url,
        ClientOptions(
            reconnect=False,
            auth=AuthOptions(
                credentials=CredentialOptions(username="auto", password="AutoP@ss1"),
            ),
        ),
    )

    welcome = await client.connect()
    assert welcome.requires_auth is True

    who = await client.identity.whoami()
    assert who["authenticated"] is True

    await client.disconnect()
