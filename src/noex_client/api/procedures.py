from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class ProceduresAPI:
    """Stored procedures — register, execute, manage."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    # -- Admin ------------------------------------------------------------

    async def register(self, procedure: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "procedures.register", {"procedure": procedure}
        )
        return result

    async def unregister(self, name: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "procedures.unregister", {"name": name}
        )
        return result

    async def update(
        self, name: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "procedures.update", {"name": name, "updates": updates}
        )
        return result

    async def get(self, name: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "procedures.get", {"name": name}
        )
        return result

    async def list(self) -> dict[str, Any]:
        result: dict[str, Any] = await self._send("procedures.list", {})
        return result

    # -- Execution --------------------------------------------------------

    async def call(
        self, name: str, input: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "procedures.call", {"name": name, "input": input or {}}
        )
        return result
