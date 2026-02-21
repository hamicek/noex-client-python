"""Integration tests for LogicAPI — computed fields, views, constraints, expressions."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from noex_client import NoexClient, NoexClientError


async def wait_for(
    condition: Any, *, timeout: float = 3.0, interval: float = 0.01
) -> None:
    """Poll *condition* (callable) until it returns truthy or timeout."""
    deadline = asyncio.get_event_loop().time() + timeout
    while True:
        if condition():
            return
        if asyncio.get_event_loop().time() >= deadline:
            raise TimeoutError("wait_for timed out")
        await asyncio.sleep(interval)


# ── Computed Fields ──────────────────────────────────────────────────


class TestComputedFields:
    async def test_define_computed_fields(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_computed("items", {
            "total": {
                "depends": ["qty", "price"],
                "expr": {"$multiply": ["$qty", "$price"]},
            },
        })

        result = await c.logic.list_computed()
        assert len(result) == 1
        assert result[0]["bucket"] == "items"

    async def test_auto_computes_on_insert(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_computed("items", {
            "total": {
                "depends": ["qty", "price"],
                "expr": {"$multiply": ["$qty", "$price"]},
            },
        })

        await c.store.bucket("items").insert({"id": "i1", "qty": 3, "price": 10})

        # Wait for async computed field evaluation
        async def check() -> bool:
            record = await c.store.bucket("items").get("i1")
            return record.get("total") == 30

        deadline = asyncio.get_event_loop().time() + 3.0
        while True:
            if await check():
                break
            if asyncio.get_event_loop().time() >= deadline:
                pytest.fail("Computed field was not set within timeout")
            await asyncio.sleep(0.05)

    async def test_drop_computed_fields(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_computed("items", {
            "total": {
                "depends": ["qty", "price"],
                "expr": {"$multiply": ["$qty", "$price"]},
            },
        })

        dropped = await c.logic.drop_computed("items")
        assert dropped is True

        result = await c.logic.list_computed()
        assert len(result) == 0

    async def test_drop_nonexistent_returns_false(
        self, client_with_logic: NoexClient
    ) -> None:
        dropped = await client_with_logic.logic.drop_computed("nonexistent")
        assert dropped is False


# ── Derived Views ────────────────────────────────────────────────────


class TestDerivedViews:
    async def test_define_and_query_simple_view(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.store.bucket("items").insert({"id": "i1", "qty": 5, "price": 20})

        await c.logic.define_view({
            "name": "item_summary",
            "from": {"i": "items"},
            "select": {
                "itemId": "i.id",
                "qty": "i.qty",
                "price": "i.price",
            },
        })

        data = await c.logic.query_view("item_summary")
        assert len(data) == 1
        assert data[0]["itemId"] == "i1"
        assert data[0]["qty"] == 5

    async def test_define_and_query_join_view(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.store.bucket("customers").insert({"id": "c1", "name": "Alice"})
        await c.store.bucket("invoices").insert({
            "id": "inv1",
            "customerId": "c1",
            "total": 200,
            "issueDate": "2025-01-01",
            "dueDate": "2025-02-01",
        })

        await c.logic.define_view({
            "name": "invoice_details",
            "from": {"i": "invoices", "c": "customers"},
            "join": {"i.customerId": "c.id"},
            "select": {
                "invoiceId": "i.id",
                "customerName": "c.name",
                "total": "i.total",
            },
        })

        data = await c.logic.query_view("invoice_details")
        assert len(data) == 1
        assert data[0]["customerName"] == "Alice"
        assert data[0]["total"] == 200

    async def test_explain_view(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_view({
            "name": "explained_view",
            "from": {"i": "items"},
            "select": {"id": "i.id"},
        })

        explanation = await c.logic.explain_view("explained_view")
        assert explanation["name"] == "explained_view"
        assert explanation["sources"] == {"i": "items"}

    async def test_list_views(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_view({
            "name": "listed_view",
            "from": {"i": "items"},
            "select": {"id": "i.id"},
        })

        views = await c.logic.list_views()
        assert len(views) >= 1
        assert any(v["name"] == "listed_view" for v in views)

    async def test_drop_view(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_view({
            "name": "drop_me",
            "from": {"i": "items"},
            "select": {"id": "i.id"},
        })

        dropped = await c.logic.drop_view("drop_me")
        assert dropped is True

        views = await c.logic.list_views()
        assert not any(v["name"] == "drop_me" for v in views)

    async def test_drop_nonexistent_view_returns_false(
        self, client_with_logic: NoexClient
    ) -> None:
        dropped = await client_with_logic.logic.drop_view("nonexistent")
        assert dropped is False


# ── View Subscriptions ───────────────────────────────────────────────


class TestViewSubscriptions:
    async def test_delivers_initial_data(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.store.bucket("items").insert({"id": "i1", "qty": 3, "price": 10})

        await c.logic.define_view({
            "name": "sub_view",
            "from": {"i": "items"},
            "select": {"id": "i.id", "qty": "i.qty"},
            "reactive": True,
        })

        received: list[Any] = []
        unsub = await c.logic.subscribe_view("sub_view", lambda data: received.append(data))

        assert len(received) == 1
        assert len(received[0]) == 1
        assert received[0][0]["id"] == "i1"

        unsub()

    async def test_pushes_updates_on_source_change(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_view({
            "name": "push_view",
            "from": {"i": "items"},
            "select": {"id": "i.id", "qty": "i.qty"},
            "reactive": True,
        })

        received: list[Any] = []
        unsub = await c.logic.subscribe_view("push_view", lambda data: received.append(data))

        # Initial data (empty)
        assert len(received) == 1
        assert len(received[0]) == 0

        await c.store.bucket("items").insert({"id": "i1", "qty": 7, "price": 5})

        await wait_for(lambda: len(received) >= 2)
        assert len(received[1]) == 1
        assert received[1][0]["qty"] == 7

        unsub()

    async def test_stops_receiving_after_unsubscribe(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_view({
            "name": "stop_view",
            "from": {"i": "items"},
            "select": {"id": "i.id"},
            "reactive": True,
        })

        received: list[Any] = []
        unsub = await c.logic.subscribe_view("stop_view", lambda data: received.append(data))

        unsub()

        await c.store.bucket("items").insert({"id": "i1", "qty": 1, "price": 1})
        await asyncio.sleep(0.3)

        # Only initial data
        assert len(received) == 1


# ── Constraints ──────────────────────────────────────────────────────


class TestConstraints:
    async def test_define_constraint(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_constraint({
            "name": "positive_balance",
            "on": "accounts",
            "expr": {"$gte": ["$balance", 0]},
            "message": "Balance must be non-negative",
        })

        result = await c.logic.list_constraints()
        assert len(result) >= 1
        assert any(con["name"] == "positive_balance" for con in result)

    async def test_rejects_violating_insert(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_constraint({
            "name": "positive_balance_2",
            "on": "accounts",
            "expr": {"$gte": ["$balance", 0]},
            "message": "Balance must be non-negative",
        })

        with pytest.raises(NoexClientError):
            await c.store.bucket("accounts").insert({"id": "a1", "balance": -100})

    async def test_allows_satisfying_insert(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_constraint({
            "name": "positive_balance_3",
            "on": "accounts",
            "expr": {"$gte": ["$balance", 0]},
            "message": "Balance must be non-negative",
        })

        record = await c.store.bucket("accounts").insert({"id": "a1", "balance": 100})
        assert record["balance"] == 100

    async def test_drop_constraint(
        self, client_with_logic: NoexClient
    ) -> None:
        c = client_with_logic
        await c.logic.define_constraint({
            "name": "to_drop",
            "on": "accounts",
            "expr": {"$gte": ["$balance", 0]},
            "message": "nope",
        })

        dropped = await c.logic.drop_constraint("to_drop")
        assert dropped is True

        # After dropping, negative balance should be allowed
        record = await c.store.bucket("accounts").insert({"id": "a2", "balance": -50})
        assert record["balance"] == -50

    async def test_drop_nonexistent_returns_false(
        self, client_with_logic: NoexClient
    ) -> None:
        dropped = await client_with_logic.logic.drop_constraint("nonexistent")
        assert dropped is False


# ── Expression Evaluation ────────────────────────────────────────────


class TestExpressionEvaluation:
    async def test_simple_arithmetic(
        self, client_with_logic: NoexClient
    ) -> None:
        result = await client_with_logic.logic.evaluate_expr({"$add": [2, 3]})
        assert result == 5

    async def test_with_field_references(
        self, client_with_logic: NoexClient
    ) -> None:
        result = await client_with_logic.logic.evaluate_expr(
            {"$multiply": ["$price", "$qty"]},
            record={"price": 15, "qty": 4},
        )
        assert result == 60

    async def test_nested_expressions(
        self, client_with_logic: NoexClient
    ) -> None:
        result = await client_with_logic.logic.evaluate_expr(
            {"$add": [{"$multiply": ["$a", "$b"]}, "$c"]},
            record={"a": 3, "b": 4, "c": 5},
        )
        assert result == 17


# ── Logic Not Available ──────────────────────────────────────────────


class TestLogicNotAvailable:
    async def test_rejects_when_no_logic_engine(
        self, client_no_logic: NoexClient
    ) -> None:
        with pytest.raises(NoexClientError):
            await client_no_logic.logic.list_computed()
