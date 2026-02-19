from __future__ import annotations

import pytest

from noex_client import NoexClient


# ── Insert ────────────────────────────────────────────────────────


class TestInsert:
    @pytest.mark.asyncio
    async def test_insert_returns_record_with_meta(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})

        assert alice["name"] == "Alice"
        assert isinstance(alice["id"], str)
        assert alice["_version"] == 1
        assert isinstance(alice["_createdAt"], int)
        assert isinstance(alice["_updatedAt"], int)

    @pytest.mark.asyncio
    async def test_insert_multiple_records(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})
        bob = await users.insert({"name": "Bob"})

        assert alice["id"] != bob["id"]
        assert alice["name"] == "Alice"
        assert bob["name"] == "Bob"


# ── Get ───────────────────────────────────────────────────────────


class TestGet:
    @pytest.mark.asyncio
    async def test_get_existing_record(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})
        found = await users.get(alice["id"])

        assert found is not None
        assert found["id"] == alice["id"]
        assert found["name"] == "Alice"

    @pytest.mark.asyncio
    async def test_get_nonexistent_record_returns_none(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        found = await users.get("nonexistent-id")

        assert found is None


# ── Update ────────────────────────────────────────────────────────


class TestUpdate:
    @pytest.mark.asyncio
    async def test_update_record(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})
        updated = await users.update(alice["id"], {"name": "Alice Updated"})

        assert updated["id"] == alice["id"]
        assert updated["name"] == "Alice Updated"
        assert updated["_version"] == 2

    @pytest.mark.asyncio
    async def test_update_preserves_other_fields(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        item = await items.insert({"value": 10})
        updated = await items.update(item["id"], {"value": 20})

        assert updated["value"] == 20
        assert updated["id"] == item["id"]


# ── Delete ────────────────────────────────────────────────────────


class TestDelete:
    @pytest.mark.asyncio
    async def test_delete_record(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        alice = await users.insert({"name": "Alice"})
        await users.delete(alice["id"])

        found = await users.get(alice["id"])
        assert found is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_is_noop(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        # Server treats delete of nonexistent key as idempotent no-op
        await users.delete("nonexistent-id")


# ── All ───────────────────────────────────────────────────────────


class TestAll:
    @pytest.mark.asyncio
    async def test_all_empty_bucket(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        result = await users.all()
        assert result == []

    @pytest.mark.asyncio
    async def test_all_returns_inserted_records(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})

        result = await users.all()
        assert len(result) == 2
        names = sorted(r["name"] for r in result)
        assert names == ["Alice", "Bob"]


# ── Where ─────────────────────────────────────────────────────────


class TestWhere:
    @pytest.mark.asyncio
    async def test_where_matches_filter(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})
        await users.insert({"name": "Alice"})

        result = await users.where({"name": "Alice"})
        assert len(result) == 2
        assert all(r["name"] == "Alice" for r in result)

    @pytest.mark.asyncio
    async def test_where_no_match(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})

        result = await users.where({"name": "Nobody"})
        assert result == []


# ── FindOne ───────────────────────────────────────────────────────


class TestFindOne:
    @pytest.mark.asyncio
    async def test_find_one_returns_match(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})

        found = await users.find_one({"name": "Bob"})
        assert found is not None
        assert found["name"] == "Bob"

    @pytest.mark.asyncio
    async def test_find_one_no_match_returns_none(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})

        found = await users.find_one({"name": "Nobody"})
        assert found is None


# ── Count ─────────────────────────────────────────────────────────


class TestCount:
    @pytest.mark.asyncio
    async def test_count_all(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})

        assert await users.count() == 2

    @pytest.mark.asyncio
    async def test_count_with_filter(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        await users.insert({"name": "Alice"})
        await users.insert({"name": "Bob"})
        await users.insert({"name": "Alice"})

        assert await users.count({"name": "Alice"}) == 2

    @pytest.mark.asyncio
    async def test_count_empty_bucket(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        assert await users.count() == 0


# ── First / Last ──────────────────────────────────────────────────


class TestFirstLast:
    @pytest.mark.asyncio
    async def test_first_n(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 1})
        await items.insert({"value": 2})
        await items.insert({"value": 3})

        result = await items.first(2)
        assert len(result) == 2
        assert result[0]["value"] == 1
        assert result[1]["value"] == 2

    @pytest.mark.asyncio
    async def test_last_n(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 1})
        await items.insert({"value": 2})
        await items.insert({"value": 3})

        result = await items.last(2)
        assert len(result) == 2
        assert result[0]["value"] == 2
        assert result[1]["value"] == 3

    @pytest.mark.asyncio
    async def test_first_more_than_exists(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 1})

        result = await items.first(10)
        assert len(result) == 1


# ── Paginate ──────────────────────────────────────────────────────


class TestPaginate:
    @pytest.mark.asyncio
    async def test_paginate_first_page(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        for i in range(5):
            await items.insert({"value": i + 1})

        page = await items.paginate(limit=3)
        assert len(page["records"]) == 3
        assert page["hasMore"] is True
        assert page["nextCursor"] is not None

    @pytest.mark.asyncio
    async def test_paginate_through_all(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        for i in range(5):
            await items.insert({"value": i + 1})

        all_records: list = []

        page = await items.paginate(limit=2)
        all_records.extend(page["records"])

        while page["hasMore"]:
            page = await items.paginate(limit=2, after=page["nextCursor"])
            all_records.extend(page["records"])

        assert len(all_records) == 5
        values = sorted(r["value"] for r in all_records)
        assert values == [1, 2, 3, 4, 5]

    @pytest.mark.asyncio
    async def test_paginate_empty_bucket(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        page = await items.paginate(limit=10)
        assert page["records"] == []
        assert page["hasMore"] is False


# ── Aggregation ───────────────────────────────────────────────────


class TestAggregation:
    @pytest.mark.asyncio
    async def test_sum(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 10})
        await items.insert({"value": 20})
        await items.insert({"value": 30})

        assert await items.sum("value") == 60

    @pytest.mark.asyncio
    async def test_sum_with_filter(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 10})
        await items.insert({"value": 20})
        await items.insert({"value": 30})

        assert await items.sum("value", {"value": 10}) == 10

    @pytest.mark.asyncio
    async def test_avg(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 10})
        await items.insert({"value": 20})
        await items.insert({"value": 30})

        assert await items.avg("value") == 20

    @pytest.mark.asyncio
    async def test_min_and_max(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 10})
        await items.insert({"value": 20})
        await items.insert({"value": 30})

        assert await items.min("value") == 10
        assert await items.max("value") == 30

    @pytest.mark.asyncio
    async def test_min_max_empty_returns_none(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        assert await items.min("value") is None
        assert await items.max("value") is None

    @pytest.mark.asyncio
    async def test_sum_empty_returns_zero(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        assert await items.sum("value") == 0


# ── Clear ─────────────────────────────────────────────────────────


class TestClear:
    @pytest.mark.asyncio
    async def test_clear_removes_all_records(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.insert({"value": 1})
        await items.insert({"value": 2})
        await items.insert({"value": 3})

        assert await items.count() == 3

        await items.clear()
        assert await items.count() == 0

    @pytest.mark.asyncio
    async def test_clear_empty_bucket_is_noop(
        self, client_with_buckets: NoexClient
    ) -> None:
        items = client_with_buckets.store.bucket("items")
        await items.clear()
        assert await items.count() == 0


# ── Bucket factory ────────────────────────────────────────────────


class TestBucketFactory:
    @pytest.mark.asyncio
    async def test_store_bucket_returns_bucket_api(
        self, client_with_buckets: NoexClient
    ) -> None:
        from noex_client import BucketAPI

        bucket = client_with_buckets.store.bucket("users")
        assert isinstance(bucket, BucketAPI)

    @pytest.mark.asyncio
    async def test_different_buckets_are_independent(
        self, client_with_buckets: NoexClient
    ) -> None:
        users = client_with_buckets.store.bucket("users")
        items = client_with_buckets.store.bucket("items")

        await users.insert({"name": "Alice"})
        await items.insert({"value": 42})

        assert len(await users.all()) == 1
        assert len(await items.all()) == 1

        assert (await users.all())[0]["name"] == "Alice"
        assert (await items.all())[0]["value"] == 42
