from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from ..subscription.subscription_manager import (
    ResubscribeInfo,
    SubscriptionEntry,
    SubscriptionManager,
)

logger = logging.getLogger("noex_client")

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]
Unsubscribe = Callable[[], None]


class LogicAPI:
    """High-level logic operations — computed fields, views, constraints."""

    def __init__(self, send: SendFn, subscriptions: SubscriptionManager) -> None:
        self._send = send
        self._subscriptions = subscriptions

    # ── Computed Fields ──────────────────────────────────────────────

    async def define_computed(
        self, bucket: str, fields: dict[str, Any]
    ) -> None:
        await self._send("logic.defineComputed", {"bucket": bucket, "fields": fields})

    async def drop_computed(self, bucket: str) -> bool:
        result = await self._send("logic.dropComputed", {"bucket": bucket})
        dropped: bool = result["dropped"]
        return dropped

    async def list_computed(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send("logic.listComputed", {})
        return result

    # ── Derived Views ────────────────────────────────────────────────

    async def define_view(self, definition: dict[str, Any]) -> None:
        await self._send("logic.defineView", {"definition": definition})

    async def drop_view(self, name: str) -> bool:
        result = await self._send("logic.dropView", {"name": name})
        dropped: bool = result["dropped"]
        return dropped

    async def query_view(self, name: str) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "logic.queryView", {"name": name}
        )
        return result

    async def explain_view(self, name: str) -> dict[str, Any]:
        result: dict[str, Any] = await self._send(
            "logic.explainView", {"name": name}
        )
        return result

    async def list_views(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send("logic.listViews", {})
        return result

    async def subscribe_view(
        self,
        name: str,
        callback: Callable[[list[dict[str, Any]]], None],
    ) -> Unsubscribe:
        """Subscribe to a reactive derived view.

        Calls *callback* immediately with the initial result, then on every
        subsequent push from the server.  Returns a **synchronous** function
        that unregisters the subscription locally and fire-and-forgets the
        server-side unsubscribe.
        """
        result = await self._send("logic.subscribeView", {"name": name})
        subscription_id: str = result["subscriptionId"]

        self._subscriptions.register(
            SubscriptionEntry(
                id=subscription_id,
                channel="logic",
                callback=callback,
                resubscribe=ResubscribeInfo(
                    type="logic.subscribeView",
                    payload={"name": name},
                ),
            )
        )

        # Deliver initial data — on error, clean up immediately.
        try:
            callback(result.get("data"))
        except Exception:
            self._subscriptions.unregister(subscription_id)
            _fire_and_forget(
                self._send(
                    "logic.unsubscribeView",
                    {"subscriptionId": subscription_id},
                )
            )
            raise

        def unsub() -> None:
            self._subscriptions.unregister(subscription_id)
            _fire_and_forget(
                self._send(
                    "logic.unsubscribeView",
                    {"subscriptionId": subscription_id},
                )
            )

        return unsub

    async def unsubscribe_view(self, subscription_id: str) -> None:
        """Explicitly unsubscribe by subscription ID."""
        self._subscriptions.unregister(subscription_id)
        await self._send(
            "logic.unsubscribeView", {"subscriptionId": subscription_id}
        )

    # ── Constraints ──────────────────────────────────────────────────

    async def define_constraint(self, constraint: dict[str, Any]) -> None:
        await self._send("logic.defineConstraint", {"constraint": constraint})

    async def drop_constraint(self, name: str) -> bool:
        result = await self._send("logic.dropConstraint", {"name": name})
        dropped: bool = result["dropped"]
        return dropped

    async def list_constraints(self) -> list[dict[str, Any]]:
        result: list[dict[str, Any]] = await self._send(
            "logic.listConstraints", {}
        )
        return result

    # ── Expression (utility) ─────────────────────────────────────────

    async def evaluate_expr(
        self,
        expr: Any,
        record: dict[str, Any] | None = None,
    ) -> Any:
        payload: dict[str, Any] = {"expr": expr}
        if record is not None:
            payload["record"] = record
        return await self._send("logic.evaluateExpr", payload)


def _fire_and_forget(coro: Any) -> None:
    """Schedule a coroutine without awaiting it, suppressing errors."""

    async def _wrapper() -> None:
        try:
            await coro
        except Exception:
            pass

    try:
        asyncio.get_running_loop().create_task(_wrapper())
    except RuntimeError:
        pass
