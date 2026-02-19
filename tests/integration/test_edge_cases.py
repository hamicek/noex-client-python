from __future__ import annotations

import asyncio
from typing import Any

import pytest

from noex_client import (
    ClientOptions,
    DisconnectedError,
    NoexClient,
    NoexClientError,
    RequestTimeoutError,
)
from tests.conftest import ServerInfo, start_test_server, stop_test_server


# ── Initial state ────────────────────────────────────────────────


class TestClientInitialState:
    def test_fresh_client_state_is_disconnected(self) -> None:
        client = NoexClient("ws://127.0.0.1:1")
        assert client.state == "disconnected"

    def test_fresh_client_is_not_connected(self) -> None:
        client = NoexClient("ws://127.0.0.1:1")
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_request_on_never_connected_client_raises(self) -> None:
        client = NoexClient("ws://127.0.0.1:1", ClientOptions(reconnect=False))

        with pytest.raises(DisconnectedError) as exc_info:
            await client.request("store.all", {"bucket": "x"})

        assert "disconnected" in str(exc_info.value).lower()

    @pytest.mark.asyncio
    async def test_store_operation_on_never_connected_client_raises(self) -> None:
        client = NoexClient("ws://127.0.0.1:1", ClientOptions(reconnect=False))

        with pytest.raises(DisconnectedError):
            await client.store.bucket("users").all()


# ── Idempotent disconnect ────────────────────────────────────────


class TestIdempotentDisconnect:
    @pytest.mark.asyncio
    async def test_disconnect_when_already_disconnected_is_safe(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        await client.connect()
        await client.disconnect()
        assert client.state == "disconnected"

        # Second disconnect should be a no-op
        await client.disconnect()
        assert client.state == "disconnected"

    @pytest.mark.asyncio
    async def test_disconnect_on_never_connected_client_is_safe(self) -> None:
        client = NoexClient("ws://127.0.0.1:1", ClientOptions(reconnect=False))

        await client.disconnect()
        assert client.state == "disconnected"


# ── Context manager edge cases ───────────────────────────────────


class TestContextManagerEdgeCases:
    @pytest.mark.asyncio
    async def test_propagates_connect_failure(self) -> None:
        with pytest.raises(Exception):
            async with NoexClient(
                "ws://127.0.0.1:1",
                ClientOptions(reconnect=False, connect_timeout_ms=1_000),
            ):
                pytest.fail("Should not reach body")

    @pytest.mark.asyncio
    async def test_disconnects_on_body_exception(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        with pytest.raises(RuntimeError, match="intentional"):
            async with client:
                assert client.is_connected
                raise RuntimeError("intentional")

        assert client.state == "disconnected"
        assert client.is_connected is False


# ── Event system edge cases ──────────────────────────────────────


class TestEventSystemEdgeCases:
    @pytest.mark.asyncio
    async def test_multiple_handlers_for_same_event(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        calls: list[str] = []

        client.on("connected", lambda: calls.append("handler_a"))
        client.on("connected", lambda: calls.append("handler_b"))
        client.on("connected", lambda: calls.append("handler_c"))

        await client.connect()

        assert calls == ["handler_a", "handler_b", "handler_c"]

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_error_in_handler_does_not_block_others(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        calls: list[str] = []

        client.on("connected", lambda: calls.append("before_error"))

        def bad_handler() -> None:
            raise ValueError("handler boom")

        client.on("connected", bad_handler)
        client.on("connected", lambda: calls.append("after_error"))

        await client.connect()

        assert "before_error" in calls
        assert "after_error" in calls
        assert client.is_connected

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_unsubscribe_prevents_future_calls(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        calls: list[str] = []
        unsub = client.on("connected", lambda: calls.append("hit"))

        unsub()

        await client.connect()

        assert calls == []

        await client.disconnect()


# ── Error hierarchy ──────────────────────────────────────────────


class TestErrorHierarchy:
    def test_disconnected_error_is_noex_client_error(self) -> None:
        err = DisconnectedError("gone")
        assert isinstance(err, NoexClientError)
        assert isinstance(err, Exception)

    def test_disconnected_error_has_correct_code(self) -> None:
        err = DisconnectedError("test message")
        assert err.code == "DISCONNECTED"
        assert str(err) == "test message"
        assert err.details is None

    def test_request_timeout_error_is_noex_client_error(self) -> None:
        err = RequestTimeoutError("timed out")
        assert isinstance(err, NoexClientError)
        assert err.code == "TIMEOUT"
        assert str(err) == "timed out"

    def test_noex_client_error_with_details(self) -> None:
        err = NoexClientError(
            code="VALIDATION_ERROR",
            message="Invalid input",
            details={"field": "name", "reason": "required"},
        )
        assert err.code == "VALIDATION_ERROR"
        assert str(err) == "Invalid input"
        assert err.details == {"field": "name", "reason": "required"}

    @pytest.mark.asyncio
    async def test_server_error_has_correct_code(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        await client.connect()

        with pytest.raises(NoexClientError) as exc_info:
            await client.request("store.get", {"bucket": "no_such_bucket", "key": "1"})

        err = exc_info.value
        assert isinstance(err.code, str)
        assert len(err.code) > 0
        assert str(err) != ""

        await client.disconnect()


# ── Multiple concurrent clients ──────────────────────────────────


class TestMultipleClients:
    @pytest.mark.asyncio
    async def test_two_clients_work_independently(
        self, test_server_with_buckets: ServerInfo
    ) -> None:
        client_a = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        client_b = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        await client_a.connect()
        await client_b.connect()

        # Client A inserts
        alice = await client_a.store.bucket("users").insert({"name": "Alice"})

        # Client B sees it
        found = await client_b.store.bucket("users").get(alice["id"])
        assert found is not None
        assert found["name"] == "Alice"

        await client_a.disconnect()
        await client_b.disconnect()

    @pytest.mark.asyncio
    async def test_one_client_disconnect_does_not_affect_other(
        self, test_server_with_buckets: ServerInfo
    ) -> None:
        client_a = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        client_b = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        await client_a.connect()
        await client_b.connect()

        await client_a.disconnect()
        assert client_a.state == "disconnected"

        # Client B is still connected and functional
        assert client_b.is_connected
        inserted = await client_b.store.bucket("users").insert({"name": "Bob"})
        assert inserted["name"] == "Bob"

        await client_b.disconnect()


# ── Subscription edge cases ──────────────────────────────────────


async def wait_for(
    condition: callable,  # type: ignore[valid-type]
    *,
    timeout: float = 3.0,
    interval: float = 0.01,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("wait_for timed out")
        await asyncio.sleep(interval)


class TestSubscriptionEdgeCases:
    @pytest.mark.asyncio
    async def test_subscriptions_cleared_on_disconnect(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries
        await client.store.subscribe("all-users", lambda data: None)
        await client.store.subscribe("user-count", lambda data: None)

        await client.disconnect()

        # Internal subscription manager should be empty
        assert client._subscription_manager.count == 0  # noqa: SLF001

    @pytest.mark.asyncio
    async def test_rapid_subscribe_unsubscribe_cycle(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries

        for _ in range(5):
            unsub = await client.store.subscribe("all-users", lambda data: None)
            unsub()

        # Client should still be functional
        assert client.is_connected
        unsub = await client.store.subscribe("all-users", lambda data: None)
        unsub()

    @pytest.mark.asyncio
    async def test_unsubscribe_is_idempotent(
        self, client_with_queries: NoexClient
    ) -> None:
        client = client_with_queries

        unsub = await client.store.subscribe("all-users", lambda data: None)
        unsub()
        unsub()
        unsub()

        # No errors, client still works
        assert client.is_connected
        await client.store.bucket("users").insert({"name": "Test"})

    @pytest.mark.asyncio
    async def test_two_clients_independent_subscriptions(self) -> None:
        info = await start_test_server(
            buckets=[
                {"name": "users", "schema": {"name": {"type": "string", "required": True}}},
            ],
            queries=[
                {"name": "all-users", "type": "all", "bucket": "users"},
            ],
        )
        try:
            client_a = NoexClient(info.url, ClientOptions(reconnect=False))
            client_b = NoexClient(info.url, ClientOptions(reconnect=False))
            await client_a.connect()
            await client_b.connect()

            received_a: list[Any] = []
            received_b: list[Any] = []

            await client_a.store.subscribe(
                "all-users", lambda data: received_a.append(data)
            )
            await client_b.store.subscribe(
                "all-users", lambda data: received_b.append(data)
            )

            assert len(received_a) == 1
            assert len(received_b) == 1

            await client_a.store.bucket("users").insert({"name": "Shared"})
            await wait_for(
                lambda: len(received_a) >= 2 and len(received_b) >= 2
            )

            # Both clients received the push
            assert len(received_a[-1]) == 1
            assert received_a[-1][0]["name"] == "Shared"
            assert len(received_b[-1]) == 1
            assert received_b[-1][0]["name"] == "Shared"

            await client_a.disconnect()
            await client_b.disconnect()
        finally:
            await stop_test_server(info)


# ── State transitions ────────────────────────────────────────────


class TestStateTransitions:
    @pytest.mark.asyncio
    async def test_state_progresses_through_lifecycle(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        assert client.state == "disconnected"

        await client.connect()
        assert client.state == "connected"

        await client.disconnect()
        assert client.state == "disconnected"

    @pytest.mark.asyncio
    async def test_state_is_disconnected_after_failed_connect(self) -> None:
        client = NoexClient(
            "ws://127.0.0.1:1",
            ClientOptions(reconnect=False, connect_timeout_ms=1_000),
        )

        with pytest.raises(Exception):
            await client.connect()

        assert client.state == "disconnected"
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_welcome_info_returned_on_connect(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        welcome = await client.connect()
        assert isinstance(welcome.version, str)
        assert isinstance(welcome.server_time, int)
        assert isinstance(welcome.requires_auth, bool)

        await client.disconnect()


# ── Concurrent operations ────────────────────────────────────────


class TestConcurrentOperations:
    @pytest.mark.asyncio
    async def test_concurrent_inserts_across_buckets(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        items = client_with_buckets.store.bucket("items")

        results = await asyncio.gather(
            users.insert({"name": "Alice"}),
            items.insert({"value": 1}),
            users.insert({"name": "Bob"}),
            items.insert({"value": 2}),
        )

        assert len(results) == 4
        assert results[0]["name"] == "Alice"
        assert results[1]["value"] == 1
        assert results[2]["name"] == "Bob"
        assert results[3]["value"] == 2

    @pytest.mark.asyncio
    async def test_concurrent_reads_and_writes(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})

        # Concurrent read + write + read
        results = await asyncio.gather(
            users.get(alice["id"]),
            users.insert({"name": "Bob"}),
            users.all(),
        )

        found = results[0]
        assert found is not None
        assert found["name"] == "Alice"

        bob = results[1]
        assert bob["name"] == "Bob"


# ── Connection loss during operation ─────────────────────────────


class TestConnectionLoss:
    @pytest.mark.asyncio
    async def test_pending_subscription_callback_not_called_after_disconnect(
        self,
    ) -> None:
        info = await start_test_server(
            buckets=[
                {"name": "users", "schema": {"name": {"type": "string", "required": True}}},
            ],
            queries=[
                {"name": "all-users", "type": "all", "bucket": "users"},
            ],
        )
        try:
            client = NoexClient(info.url, ClientOptions(reconnect=False))
            await client.connect()

            received: list[Any] = []
            unsub = await client.store.subscribe(
                "all-users", lambda data: received.append(data)
            )
            assert len(received) == 1

            unsub()
            await client.disconnect()

            # No more callbacks after disconnect
            initial_count = len(received)
            await asyncio.sleep(0.2)
            assert len(received) == initial_count
        finally:
            await stop_test_server(info)

    @pytest.mark.asyncio
    async def test_error_event_emitted_on_server_crash(self) -> None:
        info = await start_test_server()
        client = NoexClient(info.url, ClientOptions(reconnect=False))
        await client.connect()

        disconnected: asyncio.Future[str] = (
            asyncio.get_running_loop().create_future()
        )

        def on_disconnected(reason: str) -> None:
            if not disconnected.done():
                disconnected.set_result(reason)

        client.on("disconnected", on_disconnected)

        info.process.kill()
        await info.process.wait()

        reason = await asyncio.wait_for(disconnected, timeout=5)
        assert client.state == "disconnected"
        assert isinstance(reason, str)
