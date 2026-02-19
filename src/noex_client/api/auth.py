from __future__ import annotations

from typing import Any, Awaitable, Callable

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]


class AuthAPI:
    """Token-based authentication — login, logout, whoami."""

    def __init__(self, send: SendFn) -> None:
        self._send = send

    async def login(self, token: str) -> dict[str, Any]:
        return await self._send("auth.login", {"token": token})

    async def logout(self) -> None:
        await self._send("auth.logout", {})

    async def whoami(self) -> dict[str, Any] | None:
        result = await self._send("auth.whoami", {})

        if not result.get("authenticated"):
            return None

        session: dict[str, Any] = {
            "userId": result["userId"],
            "roles": result["roles"],
        }
        if result.get("expiresAt") is not None:
            session["expiresAt"] = result["expiresAt"]

        return session
