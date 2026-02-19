from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable

from ..subscription.subscription_manager import (
    ResubscribeInfo,
    SubscriptionEntry,
    SubscriptionManager,
)
from .bucket import BucketAPI

logger = logging.getLogger("noex_client")

SendFn = Callable[[str, dict[str, Any] | None], Awaitable[Any]]
Unsubscribe = Callable[[], None]


class StoreAPI:
    """High-level store operations — bucket factory, subscriptions, metadata."""

    def __init__(self, send: SendFn, subscriptions: SubscriptionManager) -> None:
        self._send = send
        self._subscriptions = subscriptions

    def bucket(self, name: str) -> BucketAPI:
        return BucketAPI(name, self._send)

    # ── Subscriptions ──────────────────────────────────────────────

    async def subscribe(
        self,
        query: str,
        callback: Callable[[Any], None],
        params: dict[str, Any] | None = None,
    ) -> Unsubscribe:
        """Subscribe to a reactive query.

        Calls *callback* immediately with the initial result, then on every
        subsequent push from the server.  Returns a **synchronous** function
        that unregisters the subscription locally and fire-and-forgets the
        server-side unsubscribe.
        """
        payload: dict[str, Any] = {"query": query}
        if params is not None:
            payload["params"] = params

        result = await self._send("store.subscribe", payload)
        subscription_id: str = result["subscriptionId"]

        self._subscriptions.register(
            SubscriptionEntry(
                id=subscription_id,
                channel="subscription",
                callback=callback,
                resubscribe=ResubscribeInfo(
                    type="store.subscribe",
                    payload=payload,
                ),
            )
        )

        # Deliver initial data — on error, clean up immediately.
        try:
            callback(result.get("data"))
        except Exception:
            self._subscriptions.unregister(subscription_id)
            _fire_and_forget(
                self._send("store.unsubscribe", {"subscriptionId": subscription_id})
            )
            raise

        def unsub() -> None:
            self._subscriptions.unregister(subscription_id)
            _fire_and_forget(
                self._send("store.unsubscribe", {"subscriptionId": subscription_id})
            )

        return unsub

    async def unsubscribe(self, subscription_id: str) -> None:
        """Explicitly unsubscribe by subscription ID."""
        self._subscriptions.unregister(subscription_id)
        await self._send("store.unsubscribe", {"subscriptionId": subscription_id})


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
