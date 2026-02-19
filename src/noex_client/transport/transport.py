from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

from websockets.asyncio.client import ClientConnection, connect as ws_connect
from websockets.exceptions import ConnectionClosed

from ..config import TransportState

logger = logging.getLogger("noex_client")

Unsubscribe = Callable[[], None]


class WebSocketTransport:
    """Low-level WebSocket wrapper with heartbeat support."""

    def __init__(
        self,
        url: str,
        *,
        connect_timeout_ms: int = 5_000,
        heartbeat: bool = True,
    ) -> None:
        self._url = url
        self._connect_timeout_ms = connect_timeout_ms
        self._heartbeat = heartbeat
        self._ws: ClientConnection | None = None
        self._state: TransportState = "idle"
        self._listeners: dict[str, list[Callable[..., Any]]] = {}
        self._recv_task: asyncio.Task[None] | None = None

    @property
    def state(self) -> TransportState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == "connected"

    # -- Lifecycle --------------------------------------------------------

    async def connect(self) -> None:
        if self._state in ("connected", "connecting"):
            return

        self._state = "connecting"
        try:
            self._ws = await asyncio.wait_for(
                ws_connect(self._url, ping_interval=None),
                timeout=self._connect_timeout_ms / 1000,
            )
        except asyncio.TimeoutError:
            self._state = "disconnected"
            raise ConnectionError(
                f"Connect timeout after {self._connect_timeout_ms}ms"
            ) from None
        except Exception:
            self._state = "disconnected"
            raise

        self._state = "connected"
        self._emit("open")
        self._recv_task = asyncio.create_task(self._receive_loop())

    async def disconnect(
        self, code: int = 1000, reason: str = "Client disconnect"
    ) -> None:
        if self._state in ("disconnected", "idle"):
            return

        self._state = "disconnected"

        if self._recv_task is not None:
            self._recv_task.cancel()
            try:
                await self._recv_task
            except asyncio.CancelledError:
                pass
            self._recv_task = None

        if self._ws is not None:
            try:
                await self._ws.close(code, reason)
            except Exception:
                pass
            self._ws = None

    # -- Communication ----------------------------------------------------

    def send(self, data: str) -> None:
        if self._ws is None or self._state != "connected":
            raise ConnectionError("Cannot send — transport is not connected")
        # websockets send is a coroutine, fire-and-forget via task
        asyncio.get_running_loop().create_task(self._ws.send(data))

    async def send_async(self, data: str) -> None:
        if self._ws is None or self._state != "connected":
            raise ConnectionError("Cannot send — transport is not connected")
        await self._ws.send(data)

    # -- Events -----------------------------------------------------------

    def on(self, event: str, handler: Callable[..., Any]) -> Unsubscribe:
        listeners = self._listeners.setdefault(event, [])
        listeners.append(handler)

        def unsub() -> None:
            try:
                listeners.remove(handler)
            except ValueError:
                pass

        return unsub

    # -- Private ----------------------------------------------------------

    def _emit(self, event: str, *args: Any) -> None:
        for handler in self._listeners.get(event, []):
            try:
                handler(*args)
            except Exception:
                logger.exception("Event handler error for %s", event)

    async def _receive_loop(self) -> None:
        assert self._ws is not None
        ws = self._ws
        try:
            async for raw in ws:
                data = raw if isinstance(raw, str) else raw.decode("utf-8")
                if self._heartbeat and self._handle_ping_pong(data):
                    continue
                self._emit("message", data)
        except ConnectionClosed:
            code = ws.close_code or 1006
            reason = ws.close_reason or ""
            self._emit("close", code, reason)
        except asyncio.CancelledError:
            return
        except Exception as e:
            self._emit("error", e)
        else:
            # Clean close — the async for loop exits normally for code 1000/1001.
            code = ws.close_code or 1000
            reason = ws.close_reason or ""
            self._emit("close", code, reason)
        finally:
            self._state = "disconnected"
            self._ws = None

    def _handle_ping_pong(self, raw: str) -> bool:
        try:
            msg = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return False

        if (
            isinstance(msg, dict)
            and msg.get("type") == "ping"
            and isinstance(msg.get("timestamp"), (int, float))
        ):
            self.send(json.dumps({"type": "pong", "timestamp": msg["timestamp"]}))
            return True
        return False
