from __future__ import annotations

from typing import Any, Callable

PushHandler = Callable[[str, str, Any], None]


class PushRouter:
    """Routes incoming push messages to the appropriate handler."""

    def __init__(self, on_push: PushHandler) -> None:
        self._on_push = on_push

    def handle_message(self, msg: dict[str, Any]) -> bool:
        """Process an incoming message. Returns True if it was a push
        notification, False otherwise."""
        if msg.get("type") != "push":
            return False

        subscription_id = msg.get("subscriptionId")
        channel = msg.get("channel")

        if not isinstance(subscription_id, str) or not isinstance(channel, str):
            return False

        self._on_push(subscription_id, channel, msg.get("data"))
        return True
