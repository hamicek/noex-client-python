from __future__ import annotations

import asyncio
from typing import Any

import pytest

from noex_client import NoexClient


async def wait_for(
    condition: callable,  # type: ignore[valid-type]
    *,
    timeout: float = 3.0,
    interval: float = 0.01,
) -> None:
    """Poll *condition* every *interval* seconds until it returns ``True``."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("wait_for timed out")
        await asyncio.sleep(interval)


# ── Initial data ──────────────────────────────────────────────────


class TestSubscribeInitialData:
    @pytest.mark.asyncio
    async def test_delivers_empty_array_for_empty_bucket(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        await client.store.subscribe("all-users", lambda data: received.append(data))

        assert len(received) == 1
        assert received[0] == []

    @pytest.mark.asyncio
    async def test_delivers_existing_records_as_initial_data(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        await client.store.bucket("users").insert({"name": "Alice"})

        received: list[Any] = []
        await client.store.subscribe("all-users", lambda data: received.append(data))

        assert len(received) == 1
        users = received[0]
        assert len(users) == 1
        assert users[0]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_delivers_filtered_initial_data_with_params(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        await client.store.bucket("users").insert({"name": "Admin", "role": "admin"})
        await client.store.bucket("users").insert({"name": "User", "role": "user"})

        received: list[Any] = []
        await client.store.subscribe(
            "users-by-role",
            lambda data: received.append(data),
            params={"role": "admin"},
        )

        assert len(received) == 1
        users = received[0]
        assert len(users) == 1
        assert users[0]["name"] == "Admin"

    @pytest.mark.asyncio
    async def test_delivers_scalar_initial_data(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        await client.store.bucket("users").insert({"name": "A"})
        await client.store.bucket("users").insert({"name": "B"})

        received: list[Any] = []
        await client.store.subscribe("user-count", lambda data: received.append(data))

        assert len(received) == 1
        assert received[0] == 2


# ── Push notifications ────────────────────────────────────────────


class TestPushNotifications:
    @pytest.mark.asyncio
    async def test_calls_callback_on_insert(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        await client.store.subscribe("all-users", lambda data: received.append(data))
        assert len(received) == 1  # initial

        await client.store.bucket("users").insert({"name": "Bob"})
        await wait_for(lambda: len(received) >= 2)

        users = received[1]
        assert len(users) == 1
        assert users[0]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_calls_callback_on_update(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        inserted = await client.store.bucket("users").insert({"name": "Carol"})

        received: list[Any] = []
        await client.store.subscribe("all-users", lambda data: received.append(data))

        await client.store.bucket("users").update(inserted["id"], {"name": "Caroline"})
        await wait_for(lambda: len(received) >= 2)

        users = received[1]
        assert len(users) == 1
        assert users[0]["name"] == "Caroline"

    @pytest.mark.asyncio
    async def test_calls_callback_on_delete(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        inserted = await client.store.bucket("users").insert({"name": "Dave"})

        received: list[Any] = []
        await client.store.subscribe("all-users", lambda data: received.append(data))

        assert len(received) == 1
        assert len(received[0]) == 1

        await client.store.bucket("users").delete(inserted["id"])
        await wait_for(lambda: len(received) >= 2)

        assert received[1] == []

    @pytest.mark.asyncio
    async def test_pushes_updated_scalar_value(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        await client.store.subscribe("user-count", lambda data: received.append(data))
        assert received[0] == 0

        await client.store.bucket("users").insert({"name": "Eve"})
        await wait_for(lambda: len(received) >= 2)

        assert received[1] == 1

    @pytest.mark.asyncio
    async def test_delivers_pushes_for_multiple_mutations(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        await client.store.subscribe("user-count", lambda data: received.append(data))

        await client.store.bucket("users").insert({"name": "First"})
        await wait_for(lambda: len(received) >= 2)
        assert received[1] == 1

        await client.store.bucket("users").insert({"name": "Second"})
        await wait_for(lambda: len(received) >= 3)
        assert received[2] == 2

    @pytest.mark.asyncio
    async def test_only_pushes_when_result_changes(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        await client.store.subscribe(
            "users-by-role",
            lambda data: received.append(data),
            params={"role": "admin"},
        )
        assert len(received) == 1
        assert received[0] == []

        # Insert a regular user — filtered query result unchanged
        await client.store.bucket("users").insert({"name": "Regular", "role": "user"})
        await asyncio.sleep(0.2)
        assert len(received) == 1

        # Insert an admin — query result changes
        await client.store.bucket("users").insert({"name": "AdminUser", "role": "admin"})
        await wait_for(lambda: len(received) >= 2)

        admins = received[1]
        assert len(admins) == 1
        assert admins[0]["name"] == "AdminUser"


# ── Unsubscribe ───────────────────────────────────────────────────


class TestUnsubscribe:
    @pytest.mark.asyncio
    async def test_returned_function_is_synchronous(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        unsub = await client.store.subscribe("all-users", lambda data: None)
        assert callable(unsub)

        # Call is synchronous (no await needed)
        result = unsub()
        assert result is None

    @pytest.mark.asyncio
    async def test_stops_push_notifications(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []

        unsub = await client.store.subscribe(
            "all-users", lambda data: received.append(data)
        )
        assert len(received) == 1

        unsub()

        await client.store.bucket("users").insert({"name": "Ghost"})
        await asyncio.sleep(0.3)
        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_explicit_unsubscribe(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries

        result = await client.request(
            "store.subscribe", {"query": "all-users"}
        )
        subscription_id = result["subscriptionId"]

        await client.store.unsubscribe(subscription_id)

        await client.store.bucket("users").insert({"name": "Nobody"})
        await asyncio.sleep(0.3)


# ── Multiple subscriptions ────────────────────────────────────────


class TestMultipleSubscriptions:
    @pytest.mark.asyncio
    async def test_supports_multiple_active_subscriptions(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        users_received: list[Any] = []
        count_received: list[Any] = []

        await client.store.subscribe(
            "all-users", lambda data: users_received.append(data)
        )
        await client.store.subscribe(
            "user-count", lambda data: count_received.append(data)
        )

        assert len(users_received) == 1
        assert len(count_received) == 1

        await client.store.bucket("users").insert({"name": "MultiSub"})
        await wait_for(
            lambda: len(users_received) >= 2 and len(count_received) >= 2
        )

        users = users_received[1]
        assert len(users) == 1
        assert users[0]["name"] == "MultiSub"
        assert count_received[1] == 1

    @pytest.mark.asyncio
    async def test_unsubscribing_one_does_not_affect_others(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        users_received: list[Any] = []
        count_received: list[Any] = []

        unsub_users = await client.store.subscribe(
            "all-users", lambda data: users_received.append(data)
        )
        await client.store.subscribe(
            "user-count", lambda data: count_received.append(data)
        )

        unsub_users()

        await client.store.bucket("users").insert({"name": "StillWorking"})
        await wait_for(lambda: len(count_received) >= 2)

        # Count subscription still works
        assert count_received[1] == 1

        # Users subscription stopped
        await asyncio.sleep(0.15)
        assert len(users_received) == 1


# ── Error handling ────────────────────────────────────────────────


class TestSubscriptionErrors:
    @pytest.mark.asyncio
    async def test_rejects_on_unknown_query(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        with pytest.raises(Exception):
            await client.store.subscribe("nonexistent-query", lambda data: None)

    @pytest.mark.asyncio
    async def test_callback_errors_do_not_crash_client(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        received: list[Any] = []
        call_count = 0

        def callback(data: Any) -> None:
            nonlocal call_count
            call_count += 1
            if call_count == 2:
                raise ValueError("callback boom")
            received.append(data)

        await client.store.subscribe("all-users", callback)

        await client.store.bucket("users").insert({"name": "Survivor"})
        await wait_for(lambda: call_count >= 2)

        # Client is still functional after callback error
        assert client.is_connected

        # Can still make requests
        all_users = await client.store.bucket("users").all()
        assert len(all_users) == 1
