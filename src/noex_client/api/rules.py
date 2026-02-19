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


class RulesAPI:
    """High-level rules operations — events, facts, subscriptions, admin."""

    def __init__(self, send: SendFn, subscriptions: SubscriptionManager) -> None:
        self._send = send
        self._subscriptions = subscriptions

    # ── Events ─────────────────────────────────────────────────────

    async def emit(
        self,
        topic: str,
        data: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        causation_id: str | None = None,
    ) -> dict[str, Any]:
        """Emit a rules event."""
        payload: dict[str, Any] = {"topic": topic}
        if data is not None:
            payload["data"] = data
        if correlation_id is not None:
            payload["correlationId"] = correlation_id
            if causation_id is not None:
                payload["causationId"] = causation_id
        return await self._send("rules.emit", payload)

    # ── Facts ──────────────────────────────────────────────────────

    async def set_fact(self, key: str, value: Any) -> dict[str, Any]:
        """Set a fact value."""
        return await self._send("rules.setFact", {"key": key, "value": value})

    async def get_fact(self, key: str) -> Any:
        """Get a fact value. Returns ``None`` if not found."""
        return await self._send("rules.getFact", {"key": key})

    async def delete_fact(self, key: str) -> bool:
        """Delete a fact. Returns ``True`` if the fact existed."""
        result = await self._send("rules.deleteFact", {"key": key})
        return result["deleted"]

    async def query_facts(self, pattern: str) -> list[dict[str, Any]]:
        """Query facts matching a glob pattern."""
        return await self._send("rules.queryFacts", {"pattern": pattern})

    async def get_all_facts(self) -> list[dict[str, Any]]:
        """Return all facts."""
        return await self._send("rules.getAllFacts", {})

    # ── Subscriptions ──────────────────────────────────────────────

    async def subscribe(
        self,
        pattern: str,
        callback: Callable[[dict[str, Any], str], None],
    ) -> Unsubscribe:
        """Subscribe to rules events matching *pattern*.

        The *callback* receives ``(event, topic)`` on each matching event.
        Returns a **synchronous** unsubscribe function.
        """
        result = await self._send("rules.subscribe", {"pattern": pattern})
        subscription_id: str = result["subscriptionId"]

        def push_callback(data: Any) -> None:
            topic = data["topic"]
            event = data["event"]
            callback(event, topic)

        self._subscriptions.register(
            SubscriptionEntry(
                id=subscription_id,
                channel="event",
                callback=push_callback,
                resubscribe=ResubscribeInfo(
                    type="rules.subscribe",
                    payload={"pattern": pattern},
                ),
            )
        )

        def unsub() -> None:
            self._subscriptions.unregister(subscription_id)
            _fire_and_forget(
                self._send(
                    "rules.unsubscribe", {"subscriptionId": subscription_id}
                )
            )

        return unsub

    async def unsubscribe(self, subscription_id: str) -> None:
        """Explicitly unsubscribe by subscription ID."""
        self._subscriptions.unregister(subscription_id)
        await self._send("rules.unsubscribe", {"subscriptionId": subscription_id})

    # ── Admin ──────────────────────────────────────────────────────

    async def register_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        """Register a new rule."""
        return await self._send("rules.registerRule", {"rule": rule})

    async def unregister_rule(self, rule_id: str) -> dict[str, Any]:
        """Unregister a rule by ID."""
        return await self._send("rules.unregisterRule", {"ruleId": rule_id})

    async def update_rule(
        self, rule_id: str, updates: dict[str, Any]
    ) -> dict[str, Any]:
        """Update a rule."""
        return await self._send(
            "rules.updateRule", {"ruleId": rule_id, "updates": updates}
        )

    async def enable_rule(self, rule_id: str) -> dict[str, Any]:
        """Enable a rule."""
        return await self._send("rules.enableRule", {"ruleId": rule_id})

    async def disable_rule(self, rule_id: str) -> dict[str, Any]:
        """Disable a rule."""
        return await self._send("rules.disableRule", {"ruleId": rule_id})

    async def get_rule(self, rule_id: str) -> dict[str, Any]:
        """Get a rule by ID."""
        return await self._send("rules.getRule", {"ruleId": rule_id})

    async def get_rules(self) -> dict[str, Any]:
        """List all registered rules."""
        return await self._send("rules.getRules", {})

    async def validate_rule(self, rule: dict[str, Any]) -> dict[str, Any]:
        """Validate a rule definition without registering it."""
        return await self._send("rules.validateRule", {"rule": rule})

    # ── Stats ──────────────────────────────────────────────────────

    async def stats(self) -> dict[str, Any]:
        """Return rules engine statistics."""
        return await self._send("rules.stats", {})


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
