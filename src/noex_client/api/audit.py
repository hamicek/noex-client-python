from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class AuditAPI:
    """Audit log queries."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    async def query(self, filter: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {}

        if filter is not None:
            for key in ("userId", "operation", "result", "from", "to", "limit"):
                if key in filter:
                    payload[key] = filter[key]

        result = await self._send("audit.query", payload)
        return result["entries"]
