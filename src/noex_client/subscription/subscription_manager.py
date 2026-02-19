from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Awaitable, Callable

logger = logging.getLogger("noex_client")

SendFn = Callable[[str, dict[str, Any]], Awaitable[Any]]


@dataclass
class ResubscribeInfo:
    type: str
    payload: dict[str, Any]


@dataclass
class SubscriptionEntry:
    id: str
    channel: str  # 'subscription' | 'event'
    callback: Callable[[Any], None]
    resubscribe: ResubscribeInfo


class SubscriptionManager:
    """Tracks active subscriptions and handles push message routing."""

    def __init__(self) -> None:
        self._subscriptions: dict[str, SubscriptionEntry] = {}

    @property
    def count(self) -> int:
        return len(self._subscriptions)

    def register(self, entry: SubscriptionEntry) -> None:
        self._subscriptions[entry.id] = entry

    def unregister(self, subscription_id: str) -> None:
        self._subscriptions.pop(subscription_id, None)

    def handle_push(self, subscription_id: str, data: Any) -> None:
        entry = self._subscriptions.get(subscription_id)
        if entry is None:
            return

        try:
            entry.callback(data)
        except Exception:
            logger.exception("Subscription %s callback error", subscription_id)

    async def resubscribe_all(self, send: SendFn) -> None:
        """Re-subscribe all active subscriptions after reconnect."""
        entries = list(self._subscriptions.values())

        for entry in entries:
            try:
                result = await send(entry.resubscribe.type, entry.resubscribe.payload)
                new_id = result["subscriptionId"]

                del self._subscriptions[entry.id]
                entry.id = new_id
                self._subscriptions[new_id] = entry

                if isinstance(result, dict) and "data" in result:
                    try:
                        entry.callback(result["data"])
                    except Exception:
                        logger.exception(
                            "Subscription %s callback error during resubscribe",
                            entry.id,
                        )
            except Exception:
                logger.exception("Failed to resubscribe %s", entry.id)
                self._subscriptions.pop(entry.id, None)

    def clear(self) -> None:
        self._subscriptions.clear()
