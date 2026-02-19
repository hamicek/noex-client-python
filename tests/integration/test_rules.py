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


# ── Emit ─────────────────────────────────────────────────────────


class TestEmit:
    @pytest.mark.asyncio
    async def test_emit_returns_event(self, client: NoexClient) -> None:
        event = await client.rules.emit("order.created", {"orderId": "123"})

        assert event["topic"] == "order.created"
        assert event["data"]["orderId"] == "123"
        assert isinstance(event["id"], str)
        assert isinstance(event["timestamp"], int)

    @pytest.mark.asyncio
    async def test_emit_without_data(self, client: NoexClient) -> None:
        event = await client.rules.emit("ping")

        assert event["topic"] == "ping"
        assert isinstance(event["id"], str)

    @pytest.mark.asyncio
    async def test_emit_with_correlation_id(self, client: NoexClient) -> None:
        event = await client.rules.emit(
            "order.shipped",
            {"orderId": "456"},
            correlation_id="corr-1",
        )

        assert event["topic"] == "order.shipped"
        assert event["correlationId"] == "corr-1"

    @pytest.mark.asyncio
    async def test_emit_with_correlation_and_causation(
        self, client: NoexClient
    ) -> None:
        event = await client.rules.emit(
            "order.delivered",
            {"orderId": "789"},
            correlation_id="corr-2",
            causation_id="cause-1",
        )

        assert event["correlationId"] == "corr-2"
        assert event["causationId"] == "cause-1"


# ── Facts ────────────────────────────────────────────────────────


class TestFacts:
    @pytest.mark.asyncio
    async def test_set_and_get_fact(self, client: NoexClient) -> None:
        await client.rules.set_fact("user:1:name", "Alice")
        value = await client.rules.get_fact("user:1:name")

        assert value == "Alice"

    @pytest.mark.asyncio
    async def test_get_nonexistent_fact_returns_none(
        self, client: NoexClient
    ) -> None:
        value = await client.rules.get_fact("nonexistent")
        assert value is None

    @pytest.mark.asyncio
    async def test_set_fact_returns_fact_object(
        self, client: NoexClient
    ) -> None:
        fact = await client.rules.set_fact("counter", 42)

        assert fact["key"] == "counter"
        assert fact["value"] == 42
        assert isinstance(fact["timestamp"], int)
        assert isinstance(fact["version"], int)

    @pytest.mark.asyncio
    async def test_update_fact_increments_version(
        self, client: NoexClient
    ) -> None:
        fact1 = await client.rules.set_fact("counter", 1)
        fact2 = await client.rules.set_fact("counter", 2)

        assert fact2["version"] > fact1["version"]
        assert fact2["value"] == 2

    @pytest.mark.asyncio
    async def test_delete_existing_fact(self, client: NoexClient) -> None:
        await client.rules.set_fact("temp", "value")
        deleted = await client.rules.delete_fact("temp")

        assert deleted is True

        value = await client.rules.get_fact("temp")
        assert value is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_fact(self, client: NoexClient) -> None:
        deleted = await client.rules.delete_fact("nonexistent")
        assert deleted is False

    @pytest.mark.asyncio
    async def test_set_complex_value(self, client: NoexClient) -> None:
        data = {"name": "Alice", "scores": [10, 20, 30]}
        await client.rules.set_fact("user:1:profile", data)

        value = await client.rules.get_fact("user:1:profile")
        assert value["name"] == "Alice"
        assert value["scores"] == [10, 20, 30]


# ── Query facts ──────────────────────────────────────────────────


class TestQueryFacts:
    @pytest.mark.asyncio
    async def test_query_facts_with_wildcard(
        self, client: NoexClient
    ) -> None:
        await client.rules.set_fact("user:1:name", "Alice")
        await client.rules.set_fact("user:2:name", "Bob")
        await client.rules.set_fact("product:1:title", "Widget")

        facts = await client.rules.query_facts("user:*:name")

        assert len(facts) == 2
        keys = sorted(f["key"] for f in facts)
        assert keys == ["user:1:name", "user:2:name"]

    @pytest.mark.asyncio
    async def test_query_facts_no_match(self, client: NoexClient) -> None:
        await client.rules.set_fact("user:1:name", "Alice")

        facts = await client.rules.query_facts("nonexistent:*")
        assert facts == []

    @pytest.mark.asyncio
    async def test_get_all_facts(self, client: NoexClient) -> None:
        await client.rules.set_fact("a", 1)
        await client.rules.set_fact("b", 2)

        facts = await client.rules.get_all_facts()
        assert len(facts) >= 2

        keys = {f["key"] for f in facts}
        assert "a" in keys
        assert "b" in keys


# ── Subscribe ────────────────────────────────────────────────────


class TestRulesSubscribe:
    @pytest.mark.asyncio
    async def test_subscribe_receives_matching_events(
        self, client: NoexClient
    ) -> None:
        received: list[tuple[dict[str, Any], str]] = []

        unsub = await client.rules.subscribe(
            "order.*",
            lambda event, topic: received.append((event, topic)),
        )

        await client.rules.emit("order.created", {"orderId": "1"})
        await wait_for(lambda: len(received) >= 1)

        event, topic = received[0]
        assert topic == "order.created"
        assert event["data"]["orderId"] == "1"

        unsub()

    @pytest.mark.asyncio
    async def test_subscribe_ignores_non_matching_events(
        self, client: NoexClient
    ) -> None:
        received: list[tuple[dict[str, Any], str]] = []

        unsub = await client.rules.subscribe(
            "order.*",
            lambda event, topic: received.append((event, topic)),
        )

        await client.rules.emit("user.created", {"userId": "1"})
        await asyncio.sleep(0.3)

        assert len(received) == 0

        unsub()

    @pytest.mark.asyncio
    async def test_unsubscribe_stops_delivery(
        self, client: NoexClient
    ) -> None:
        received: list[tuple[dict[str, Any], str]] = []

        unsub = await client.rules.subscribe(
            "order.*",
            lambda event, topic: received.append((event, topic)),
        )

        await client.rules.emit("order.created", {"orderId": "1"})
        await wait_for(lambda: len(received) >= 1)

        unsub()

        await client.rules.emit("order.shipped", {"orderId": "2"})
        await asyncio.sleep(0.3)

        assert len(received) == 1

    @pytest.mark.asyncio
    async def test_unsubscribe_is_synchronous(
        self, client: NoexClient
    ) -> None:
        unsub = await client.rules.subscribe(
            "test.*", lambda event, topic: None
        )
        assert callable(unsub)
        result = unsub()
        assert result is None

    @pytest.mark.asyncio
    async def test_wildcard_all(self, client: NoexClient) -> None:
        received: list[tuple[dict[str, Any], str]] = []

        unsub = await client.rules.subscribe(
            "*",
            lambda event, topic: received.append((event, topic)),
        )

        await client.rules.emit("foo", {"a": 1})
        await client.rules.emit("bar", {"b": 2})
        await wait_for(lambda: len(received) >= 2)

        topics = {r[1] for r in received}
        assert "foo" in topics
        assert "bar" in topics

        unsub()

    @pytest.mark.asyncio
    async def test_multiple_subscriptions(
        self, client: NoexClient
    ) -> None:
        orders: list[str] = []
        users: list[str] = []

        unsub1 = await client.rules.subscribe(
            "order.*", lambda event, topic: orders.append(topic)
        )
        unsub2 = await client.rules.subscribe(
            "user.*", lambda event, topic: users.append(topic)
        )

        await client.rules.emit("order.created", {"orderId": "1"})
        await client.rules.emit("user.created", {"userId": "1"})

        await wait_for(lambda: len(orders) >= 1 and len(users) >= 1)

        assert "order.created" in orders
        assert "user.created" in users

        unsub1()
        unsub2()

    @pytest.mark.asyncio
    async def test_callback_error_does_not_crash_client(
        self, client: NoexClient
    ) -> None:
        call_count = 0

        def callback(event: dict[str, Any], topic: str) -> None:
            nonlocal call_count
            call_count += 1
            raise ValueError("boom")

        unsub = await client.rules.subscribe("test.*", callback)

        await client.rules.emit("test.event", {})
        await wait_for(lambda: call_count >= 1)

        assert client.is_connected
        # Client is still functional
        stats = await client.rules.stats()
        assert isinstance(stats, dict)

        unsub()


# ── Stats ────────────────────────────────────────────────────────


class TestRulesStats:
    @pytest.mark.asyncio
    async def test_returns_stats(self, client: NoexClient) -> None:
        stats = await client.rules.stats()

        assert isinstance(stats, dict)
        assert "rulesCount" in stats
        assert "factsCount" in stats
        assert "eventsProcessed" in stats

    @pytest.mark.asyncio
    async def test_stats_reflect_facts(self, client: NoexClient) -> None:
        await client.rules.set_fact("s1", 1)
        await client.rules.set_fact("s2", 2)

        stats = await client.rules.stats()
        assert stats["factsCount"] >= 2

    @pytest.mark.asyncio
    async def test_stats_reflect_events(self, client: NoexClient) -> None:
        initial = await client.rules.stats()
        initial_count = initial["eventsProcessed"]

        await client.rules.emit("stat.test", {})

        stats = await client.rules.stats()
        assert stats["eventsProcessed"] > initial_count
