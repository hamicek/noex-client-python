from __future__ import annotations

import pytest

from noex_client import NoexClient, NoexClientError


class TestTransaction:
    @pytest.mark.asyncio
    async def test_single_insert(
        self, client_with_buckets: NoexClient
    ) -> None:
        result = await client_with_buckets.store.transaction(
            [{"op": "insert", "bucket": "users", "data": {"name": "Alice"}}]
        )
        assert "results" in result
        assert len(result["results"]) == 1
        assert result["results"][0]["index"] == 0
        assert result["results"][0]["data"]["name"] == "Alice"
        assert result["results"][0]["data"]["_version"] == 1

    @pytest.mark.asyncio
    async def test_multiple_inserts(
        self, client_with_buckets: NoexClient
    ) -> None:
        result = await client_with_buckets.store.transaction(
            [
                {"op": "insert", "bucket": "users", "data": {"name": "Alice"}},
                {"op": "insert", "bucket": "users", "data": {"name": "Bob"}},
            ]
        )
        assert len(result["results"]) == 2
        assert result["results"][0]["data"]["name"] == "Alice"
        assert result["results"][1]["data"]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_insert_and_get(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})

        result = await client_with_buckets.store.transaction(
            [{"op": "get", "bucket": "users", "key": alice["id"]}]
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["data"]["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_mixed_operations(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        bob = await users.insert({"name": "Bob"})

        result = await client_with_buckets.store.transaction(
            [
                {"op": "insert", "bucket": "users", "data": {"name": "Alice"}},
                {"op": "update", "bucket": "users", "key": bob["id"], "data": {"name": "Bobby"}},
                {"op": "get", "bucket": "users", "key": bob["id"]},
            ]
        )
        assert len(result["results"]) == 3
        assert result["results"][0]["data"]["name"] == "Alice"
        assert result["results"][1]["data"]["name"] == "Bobby"
        assert result["results"][2]["data"]["name"] == "Bobby"

    @pytest.mark.asyncio
    async def test_delete_operation(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})

        result = await client_with_buckets.store.transaction(
            [{"op": "delete", "bucket": "users", "key": alice["id"]}]
        )
        assert len(result["results"]) == 1

        found = await users.get(alice["id"])
        assert found is None

    @pytest.mark.asyncio
    async def test_where_operation(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})
        await users.insert({"name": "Alice"})

        result = await client_with_buckets.store.transaction(
            [{"op": "where", "bucket": "users", "filter": {"name": "Alice"}}]
        )
        assert len(result["results"]) == 1
        data = result["results"][0]["data"]
        assert len(data) == 2
        assert all(r["name"] == "Alice" for r in data)

    @pytest.mark.asyncio
    async def test_count_operation(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})

        result = await client_with_buckets.store.transaction(
            [{"op": "count", "bucket": "users"}]
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["data"] == 2

    @pytest.mark.asyncio
    async def test_cross_bucket_transaction(
        self, client_with_buckets: NoexClient
    ) -> None:
        result = await client_with_buckets.store.transaction(
            [
                {"op": "insert", "bucket": "users", "data": {"name": "Alice"}},
                {"op": "insert", "bucket": "items", "data": {"value": 42}},
            ]
        )
        assert len(result["results"]) == 2
        assert result["results"][0]["data"]["name"] == "Alice"
        assert result["results"][1]["data"]["value"] == 42

    @pytest.mark.asyncio
    async def test_find_one_operation(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})

        result = await client_with_buckets.store.transaction(
            [{"op": "findOne", "bucket": "users", "filter": {"name": "Bob"}}]
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["data"]["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_null(
        self, client_with_buckets: NoexClient
    ) -> None:
        result = await client_with_buckets.store.transaction(
            [{"op": "get", "bucket": "users", "key": "nonexistent"}]
        )
        assert len(result["results"]) == 1
        assert result["results"][0]["data"] is None
