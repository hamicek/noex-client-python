from __future__ import annotations

import asyncio
from typing import Any

import pytest

from noex_client import ClientOptions, NoexClient, ReconnectOptions
from tests.conftest import start_test_server, stop_test_server

USERS_BUCKET = {
    "name": "users",
    "schema": {"name": {"type": "string", "required": True}},
}
ALL_USERS_QUERY = {"name": "all-users", "type": "all", "bucket": "users"}
USER_COUNT_QUERY = {"name": "user-count", "type": "count", "bucket": "users"}

FAST_RECONNECT = ReconnectOptions(
    initial_delay_ms=100,
    max_delay_ms=500,
    jitter_ms=0,
    max_retries=20,
)


async def wait_for(
    condition: callable,  # type: ignore[valid-type]
    *,
    timeout: float = 5.0,
    interval: float = 0.05,
) -> None:
    """Poll *condition* every *interval* seconds until it returns ``True``."""
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError("wait_for timed out")
        await asyncio.sleep(interval)


def extract_port(url: str) -> int:
    """Extract port number from a ws:// URL."""
    return int(url.rsplit(":", 1)[1])


# ── Basic reconnect ──────────────────────────────────────────────


class TestReconnectBasic:
    @pytest.mark.asyncio
    async def test_reconnects_after_server_restart(self) -> None:
        info = await start_test_server(buckets=[USERS_BUCKET])
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()
            assert client.is_connected

            reconnected = asyncio.Event()
            client.on("reconnected", lambda: reconnected.set())

            info.process.kill()
            await info.process.wait()

            await wait_for(lambda: client.state == "reconnecting")

            info2 = await start_test_server(buckets=[USERS_BUCKET], port=port)

            await asyncio.wait_for(reconnected.wait(), timeout=10)
            assert client.state == "connected"
            assert client.is_connected
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)

    @pytest.mark.asyncio
    async def test_emits_reconnecting_and_reconnected_events(self) -> None:
        info = await start_test_server(buckets=[USERS_BUCKET])
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()

            events: list[tuple[str, ...]] = []
            reconnected = asyncio.Event()

            client.on(
                "reconnecting",
                lambda attempt: events.append(("reconnecting", attempt)),
            )
            client.on(
                "reconnected",
                lambda: (events.append(("reconnected",)), reconnected.set()),
            )
            client.on("connected", lambda: events.append(("connected",)))

            info.process.kill()
            await info.process.wait()

            info2 = await start_test_server(buckets=[USERS_BUCKET], port=port)

            await asyncio.wait_for(reconnected.wait(), timeout=10)

            assert any(e[0] == "reconnecting" for e in events)
            assert any(e[0] == "reconnected" for e in events)
            assert any(e[0] == "connected" for e in events)

            reconnecting_events = [e for e in events if e[0] == "reconnecting"]
            assert reconnecting_events[0][1] == 1
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)

    @pytest.mark.asyncio
    async def test_can_make_requests_after_reconnect(self) -> None:
        info = await start_test_server(buckets=[USERS_BUCKET])
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()

            reconnected = asyncio.Event()
            client.on("reconnected", lambda: reconnected.set())

            info.process.kill()
            await info.process.wait()

            info2 = await start_test_server(buckets=[USERS_BUCKET], port=port)

            await asyncio.wait_for(reconnected.wait(), timeout=10)

            alice = await client.store.bucket("users").insert({"name": "Alice"})
            assert alice["name"] == "Alice"

            found = await client.store.bucket("users").get(alice["id"])
            assert found is not None
            assert found["name"] == "Alice"
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)

    @pytest.mark.asyncio
    async def test_disconnect_during_reconnect_aborts(self) -> None:
        info = await start_test_server()

        client = NoexClient(
            info.url,
            ClientOptions(
                reconnect=ReconnectOptions(
                    initial_delay_ms=2_000,
                    max_delay_ms=5_000,
                    jitter_ms=0,
                )
            ),
        )
        try:
            await client.connect()

            info.process.kill()
            await info.process.wait()

            await wait_for(lambda: client.state == "reconnecting")

            await client.disconnect()
            assert client.state == "disconnected"
        finally:
            if client.is_connected:
                await client.disconnect()


# ── Subscription recovery ────────────────────────────────────────


class TestSubscriptionRecovery:
    @pytest.mark.asyncio
    async def test_store_subscription_restored_after_reconnect(self) -> None:
        info = await start_test_server(
            buckets=[USERS_BUCKET],
            queries=[ALL_USERS_QUERY],
        )
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()

            received: list[Any] = []
            unsub = await client.store.subscribe(
                "all-users", lambda data: received.append(data)
            )
            assert len(received) == 1
            assert received[0] == []

            await client.store.bucket("users").insert({"name": "Alice"})
            await wait_for(lambda: len(received) >= 2)
            assert len(received[1]) == 1
            assert received[1][0]["name"] == "Alice"

            reconnected = asyncio.Event()
            client.on("reconnected", lambda: reconnected.set())

            info.process.kill()
            await info.process.wait()

            info2 = await start_test_server(
                buckets=[USERS_BUCKET],
                queries=[ALL_USERS_QUERY],
                port=port,
            )

            await asyncio.wait_for(reconnected.wait(), timeout=10)

            # Resubscribe delivers fresh initial data (empty — new server)
            await wait_for(lambda: len(received) >= 3, timeout=5)
            assert received[-1] == []

            # Pushes work on the recovered subscription
            await client.store.bucket("users").insert({"name": "Bob"})
            await wait_for(lambda: len(received) >= 4, timeout=5)
            assert received[-1][0]["name"] == "Bob"

            unsub()
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)

    @pytest.mark.asyncio
    async def test_multiple_subscriptions_restored(self) -> None:
        info = await start_test_server(
            buckets=[USERS_BUCKET],
            queries=[ALL_USERS_QUERY, USER_COUNT_QUERY],
        )
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()

            users_received: list[Any] = []
            count_received: list[Any] = []

            unsub1 = await client.store.subscribe(
                "all-users", lambda data: users_received.append(data)
            )
            unsub2 = await client.store.subscribe(
                "user-count", lambda data: count_received.append(data)
            )
            assert len(users_received) == 1
            assert len(count_received) == 1

            reconnected = asyncio.Event()
            client.on("reconnected", lambda: reconnected.set())

            info.process.kill()
            await info.process.wait()

            info2 = await start_test_server(
                buckets=[USERS_BUCKET],
                queries=[ALL_USERS_QUERY, USER_COUNT_QUERY],
                port=port,
            )

            await asyncio.wait_for(reconnected.wait(), timeout=10)

            await wait_for(
                lambda: len(users_received) >= 2 and len(count_received) >= 2,
                timeout=5,
            )

            # Insert after reconnect — both subscriptions should receive pushes
            await client.store.bucket("users").insert({"name": "Charlie"})
            await wait_for(
                lambda: len(users_received) >= 3 and len(count_received) >= 3,
                timeout=5,
            )

            assert users_received[-1][0]["name"] == "Charlie"
            assert count_received[-1] == 1

            unsub1()
            unsub2()
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)

    @pytest.mark.asyncio
    async def test_rules_subscription_restored_after_reconnect(self) -> None:
        info = await start_test_server(rules=True)
        port = extract_port(info.url)

        client = NoexClient(info.url, ClientOptions(reconnect=FAST_RECONNECT))
        info2 = None
        try:
            await client.connect()

            events: list[tuple[dict[str, Any], str]] = []
            unsub = await client.rules.subscribe(
                "user.*",
                lambda event, topic: events.append((event, topic)),
            )

            await client.rules.emit("user.created", {"userId": "1"})
            await wait_for(lambda: len(events) >= 1)
            assert events[0][1] == "user.created"

            reconnected = asyncio.Event()
            client.on("reconnected", lambda: reconnected.set())

            info.process.kill()
            await info.process.wait()

            info2 = await start_test_server(rules=True, port=port)

            await asyncio.wait_for(reconnected.wait(), timeout=10)

            await client.rules.emit("user.updated", {"userId": "2"})
            await wait_for(lambda: len(events) >= 2, timeout=5)
            assert events[1][1] == "user.updated"

            unsub()
        finally:
            if client.is_connected:
                await client.disconnect()
            if info2 is not None:
                await stop_test_server(info2)


# ── Max retries ──────────────────────────────────────────────────


class TestMaxRetries:
    @pytest.mark.asyncio
    async def test_gives_up_after_max_retries(self) -> None:
        info = await start_test_server()

        client = NoexClient(
            info.url,
            ClientOptions(
                reconnect=ReconnectOptions(
                    max_retries=3,
                    initial_delay_ms=50,
                    max_delay_ms=100,
                    jitter_ms=0,
                ),
                connect_timeout_ms=500,
            ),
        )
        try:
            await client.connect()

            errors: list[Exception] = []
            disconnected: list[str] = []
            client.on("error", lambda e: errors.append(e))
            client.on("disconnected", lambda r: disconnected.append(r))

            info.process.kill()
            await info.process.wait()

            await wait_for(lambda: len(disconnected) > 0, timeout=15)

            assert client.state == "disconnected"
            assert len(errors) > 0
            assert "Max reconnect attempts" in str(errors[-1])
            assert "Max reconnect attempts" in disconnected[0]
        finally:
            if client.is_connected:
                await client.disconnect()


# ── No reconnect ─────────────────────────────────────────────────


class TestNoReconnect:
    @pytest.mark.asyncio
    async def test_no_reconnect_when_disabled(self) -> None:
        info = await start_test_server()

        client = NoexClient(info.url, ClientOptions(reconnect=False))
        try:
            await client.connect()

            disconnected: list[str] = []
            client.on("disconnected", lambda r: disconnected.append(r))

            info.process.kill()
            await info.process.wait()

            await wait_for(lambda: len(disconnected) > 0, timeout=5)
            assert client.state == "disconnected"
        finally:
            if client.is_connected:
                await client.disconnect()
