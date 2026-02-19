from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class ProceduresAPI:
    """Stored procedures — register, execute, manage."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    # ── Admin ──────────────────────────────────────────────────────

    async def register(self, procedure: dict[str, Any]) -> dict[str, Any]:
        return await self._send("procedures.register", {"procedure": procedure})

    async def unregister(self, name: str) -> dict[str, Any]:
        return await self._send("procedures.unregister", {"name": name})

    async def update(self, name: str, updates: dict[str, Any]) -> dict[str, Any]:
        return await self._send("procedures.update", {"name": name, "updates": updates})

    async def get(self, name: str) -> dict[str, Any]:
        return await self._send("procedures.get", {"name": name})

    async def list(self) -> dict[str, Any]:
        return await self._send("procedures.list", {})

    # ── Execution ──────────────────────────────────────────────────

    async def call(self, name: str, input: dict[str, Any] | None = None) -> dict[str, Any]:
        return await self._send("procedures.call", {"name": name, "input": input or {}})
