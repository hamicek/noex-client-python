from __future__ import annotations

import asyncio
import json
from typing import Any

from ..errors import NoexClientError, RequestTimeoutError
from ..transport.transport import WebSocketTransport


class RequestManager:
    """Correlates outgoing requests with incoming responses via message IDs."""

    def __init__(self, *, timeout_ms: int = 10_000) -> None:
        self._timeout_ms = timeout_ms
        self._pending: dict[int, _PendingRequest] = {}
        self._next_id = 1

    @property
    def pending_count(self) -> int:
        return len(self._pending)

    async def send(
        self,
        transport: WebSocketTransport,
        msg_type: str,
        payload: dict[str, Any] | None = None,
    ) -> Any:
        """Send a typed request and wait for the correlated response."""
        msg_id = self._next_id
        self._next_id += 1

        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()

        timeout_handle = loop.call_later(
            self._timeout_ms / 1000,
            self._handle_timeout,
            msg_id,
        )

        self._pending[msg_id] = _PendingRequest(
            future=future,
            timeout_handle=timeout_handle,
            msg_type=msg_type,
        )

        msg: dict[str, Any] = {"id": msg_id, "type": msg_type}
        if payload:
            msg.update(payload)

        transport.send(json.dumps(msg))
        return await future

    def handle_message(self, msg: dict[str, Any]) -> bool:
        """Process an incoming message. Returns True if it was a response
        to a pending request, False otherwise."""
        msg_type = msg.get("type")

        # Push, ping, welcome, system messages are not request responses
        if msg_type in ("push", "ping", "welcome", "system"):
            return False

        msg_id = msg.get("id")
        if not isinstance(msg_id, int):
            return False

        pending = self._pending.pop(msg_id, None)
        if pending is None:
            return False

        pending.timeout_handle.cancel()

        if msg_type == "result":
            pending.future.set_result(msg.get("data"))
        elif msg_type == "error":
            pending.future.set_exception(
                NoexClientError(
                    code=msg.get("code", "UNKNOWN"),
                    message=msg.get("message", "Unknown server error"),
                    details=msg.get("details"),
                )
            )
        else:
            pending.future.set_exception(
                NoexClientError(
                    code="UNKNOWN",
                    message=f"Unexpected response type: {msg_type}",
                )
            )

        return True

    def reject_all(self, error: Exception) -> None:
        """Reject all pending requests (on disconnect)."""
        for pending in self._pending.values():
            pending.timeout_handle.cancel()
            if not pending.future.done():
                pending.future.set_exception(error)
        self._pending.clear()

    def _handle_timeout(self, msg_id: int) -> None:
        pending = self._pending.pop(msg_id, None)
        if pending is not None and not pending.future.done():
            pending.future.set_exception(
                RequestTimeoutError(
                    f"Request {pending.msg_type} (id={msg_id}) "
                    f"timed out after {self._timeout_ms}ms"
                )
            )


class _PendingRequest:
    __slots__ = ("future", "timeout_handle", "msg_type")

    def __init__(
        self,
        future: asyncio.Future[Any],
        timeout_handle: asyncio.TimerHandle,
        msg_type: str,
    ) -> None:
        self.future = future
        self.timeout_handle = timeout_handle
        self.msg_type = msg_type
