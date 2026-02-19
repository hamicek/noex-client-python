from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class BucketAPI:
    """Per-bucket operations for store CRUD, queries, aggregations."""

    def __init__(self, name: str, send: SendFn) -> None:
        self._name = name
        self._send = send

    # -- CRUD -------------------------------------------------------------

    async def insert(self, data: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "store.insert", {"bucket": self._name, "data": data}
        )
        return result

    async def get(self, key: Any) -> dict[str, Any] | None:
        result: dict[str, Any] | None = await self._send(
            "store.get", {"bucket": self._name, "key": key}
        )
        return result

    async def update(self, key: Any, data: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "store.update", {"bucket": self._name, "key": key, "data": data}
        )
        return result

    async def delete(self, key: Any) -> None:
        await self._send("store.delete", {"bucket": self._name, "key": key})

    # -- Queries ----------------------------------------------------------

    async def all(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "store.all", {"bucket": self._name}
        )
        return result

    async def where(self, filter: dict[str, Any]) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "store.where", {"bucket": self._name, "filter": filter}
        )
        return result

    async def find_one(self, filter: dict[str, Any]) -> dict[str, Any] | None:
        result: dict[str, Any] | None = await self._send(
            "store.findOne", {"bucket": self._name, "filter": filter}
        )
        return result

    async def count(self, filter: dict[str, Any] | None = None) -> int:
        payload: dict[str, Any] = {"bucket": self._name}
        if filter is not None:
            payload["filter"] = filter
        result: int = await self._send("store.count", payload)
        return result

    async def first(self, n: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "store.first", {"bucket": self._name, "n": n}
        )
        return result

    async def last(self, n: int) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "store.last", {"bucket": self._name, "n": n}
        )
        return result

    async def paginate(
        self, *, limit: int, after: Any = None
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"bucket": self._name, "limit": limit}
        if after is not None:
            payload["after"] = after
        result: dict[str, Any] = await self._send("store.paginate", payload)
        return result

    # -- Aggregation ------------------------------------------------------

    async def sum(
        self, field: str, filter: dict[str, Any] | None = None
    ) -> float:
        payload: dict[str, Any] = {"bucket": self._name, "field": field}
        if filter is not None:
            payload["filter"] = filter
        result: float = await self._send("store.sum", payload)
        return result

    async def avg(
        self, field: str, filter: dict[str, Any] | None = None
    ) -> float:
        payload: dict[str, Any] = {"bucket": self._name, "field": field}
        if filter is not None:
            payload["filter"] = filter
        result: float = await self._send("store.avg", payload)
        return result

    async def min(
        self, field: str, filter: dict[str, Any] | None = None
    ) -> float | None:
        payload: dict[str, Any] = {"bucket": self._name, "field": field}
        if filter is not None:
            payload["filter"] = filter
        result: float | None = await self._send("store.min", payload)
        return result

    async def max(
        self, field: str, filter: dict[str, Any] | None = None
    ) -> float | None:
        payload: dict[str, Any] = {"bucket": self._name, "field": field}
        if filter is not None:
            payload["filter"] = filter
        result: float | None = await self._send("store.max", payload)
        return result

    # -- Bulk -------------------------------------------------------------

    async def clear(self) -> None:
        await self._send("store.clear", {"bucket": self._name})
