from __future__ import annotations

import asyncio

import pytest

from noex_client import ClientOptions, DisconnectedError, NoexClient
from tests.conftest import ServerInfo, start_test_server, stop_test_server


class TestConnection:
    @pytest.mark.asyncio
    async def test_connect_and_receive_welcome(self, test_server: ServerInfo) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        welcome = await client.connect()

        assert welcome.version == "1.0.0"
        assert isinstance(welcome.server_time, int)
        assert welcome.requires_auth is False
        assert client.state == "connected"
        assert client.is_connected is True

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_disconnect_gracefully(self, test_server: ServerInfo) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        await client.connect()
        assert client.is_connected is True

        await client.disconnect()
        assert client.state == "disconnected"
        assert client.is_connected is False

    @pytest.mark.asyncio
    async def test_emits_connected_and_welcome_events(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        connected_called = False
        welcome_info = None

        def on_connected() -> None:
            nonlocal connected_called
            connected_called = True

        def on_welcome(info: object) -> None:
            nonlocal welcome_info
            welcome_info = info

        client.on("connected", on_connected)
        client.on("welcome", on_welcome)

        await client.connect()

        assert connected_called is True
        assert welcome_info is not None
        assert welcome_info.version == "1.0.0"  # type: ignore[union-attr]

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_emits_disconnected_event_on_server_stop(self) -> None:
        info = await start_test_server()
        client = NoexClient(info.url, ClientOptions(reconnect=False))
        await client.connect()

        disconnected_event: asyncio.Future[str] = asyncio.get_running_loop().create_future()

        def on_disconnected(reason: str) -> None:
            if not disconnected_event.done():
                disconnected_event.set_result(reason)

        client.on("disconnected", on_disconnected)

        await stop_test_server(info)

        reason = await asyncio.wait_for(disconnected_event, timeout=5)
        assert client.state == "disconnected"
        assert isinstance(reason, str)

    @pytest.mark.asyncio
    async def test_event_unsubscribe(self, test_server: ServerInfo) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))

        call_count = 0

        def on_connected() -> None:
            nonlocal call_count
            call_count += 1

        unsub = client.on("connected", on_connected)
        await client.connect()
        assert call_count == 1

        unsub()
        # Calling unsub again should be safe
        unsub()

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_connect_failure_on_invalid_address(self) -> None:
        client = NoexClient(
            "ws://127.0.0.1:1",
            ClientOptions(reconnect=False, connect_timeout_ms=2_000),
        )

        with pytest.raises(Exception):
            await client.connect()

        assert client.state == "disconnected"

    @pytest.mark.asyncio
    async def test_request_response(
        self, test_server_with_buckets: ServerInfo
    ) -> None:
        client = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        await client.connect()

        inserted = await client.request(
            "store.insert", {"bucket": "users", "data": {"name": "Alice"}}
        )

        assert isinstance(inserted, dict)
        assert inserted["name"] == "Alice"
        assert isinstance(inserted["id"], str)

        found = await client.request(
            "store.get", {"bucket": "users", "key": inserted["id"]}
        )

        assert isinstance(found, dict)
        assert found["name"] == "Alice"
        assert found["id"] == inserted["id"]

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_server_error_handling(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        await client.connect()

        with pytest.raises(Exception):
            await client.request(
                "store.get", {"bucket": "nonexistent", "key": "1"}
            )

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_request_after_disconnect_raises(
        self, test_server: ServerInfo
    ) -> None:
        client = NoexClient(test_server.url, ClientOptions(reconnect=False))
        await client.connect()
        await client.disconnect()

        with pytest.raises(DisconnectedError):
            await client.request("store.all", {"bucket": "users"})

    @pytest.mark.asyncio
    async def test_multiple_concurrent_requests(
        self, test_server_with_buckets: ServerInfo
    ) -> None:
        client = NoexClient(
            test_server_with_buckets.url, ClientOptions(reconnect=False)
        )
        await client.connect()

        results = await asyncio.gather(
            client.request("store.insert", {"bucket": "items", "data": {"value": 1}}),
            client.request("store.insert", {"bucket": "items", "data": {"value": 2}}),
            client.request("store.insert", {"bucket": "items", "data": {"value": 3}}),
        )

        assert len(results) == 3
        values = sorted(r["value"] for r in results)
        assert values == [1, 2, 3]

        all_items = await client.request("store.all", {"bucket": "items"})
        assert len(all_items) == 3

        await client.disconnect()

    @pytest.mark.asyncio
    async def test_pending_requests_rejected_on_connection_loss(self) -> None:
        info = await start_test_server()
        client = NoexClient(info.url, ClientOptions(reconnect=False))
        await client.connect()

        # Start a request, then kill the server
        request_task = asyncio.create_task(
            client.request("store.all", {"bucket": "x"})
        )

        await stop_test_server(info)

        with pytest.raises(Exception):
            await asyncio.wait_for(request_task, timeout=5)

    @pytest.mark.asyncio
    async def test_context_manager(self, test_server: ServerInfo) -> None:
        async with NoexClient(
            test_server.url, ClientOptions(reconnect=False)
        ) as client:
            assert client.is_connected is True

        assert client.state == "disconnected"
