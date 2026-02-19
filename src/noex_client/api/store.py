from __future__ import annotations

from typing import Any, Awaitable, Callable

from .bucket import BucketAPI

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class StoreAPI:
    """High-level store operations — bucket factory, metadata."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    def bucket(self, name: str) -> BucketAPI:
        return BucketAPI(name, self._send)
