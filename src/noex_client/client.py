from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

from .api.rules import RulesAPI
from .api.store import StoreAPI
from .config import (
    ClientOptions,
    ConnectionState,
    ReconnectOptions,
    WelcomeInfo,
)
from .errors import DisconnectedError
from .protocol.push_router import PushRouter
from .protocol.request_manager import RequestManager
from .subscription.subscription_manager import SubscriptionManager
from .transport.reconnect import ReconnectStrategy
from .transport.transport import WebSocketTransport

logger = logging.getLogger("noex_client")

Unsubscribe = Callable[[], None]


class NoexClient:
    """Async Python client for noex-server."""

    def __init__(
        self, url: str, options: ClientOptions | None = None
    ) -> None:
        self._url = url
        self._options = options or ClientOptions()

        self._transport = WebSocketTransport(
            url,
            connect_timeout_ms=self._options.connect_timeout_ms,
            heartbeat=self._options.heartbeat,
        )

        self._request_manager = RequestManager(
            timeout_ms=self._options.request_timeout_ms
        )

        self._subscription_manager = SubscriptionManager()

        self._push_router = PushRouter(
            lambda sub_id, _channel, data: self._subscription_manager.handle_push(
                sub_id, data
            )
        )

        self._reconnect_strategy = self._create_reconnect_strategy()

        self._state: ConnectionState = "disconnected"
        self._listeners: dict[str, list[Callable[..., Any]]] = {}
        self._intentional_disconnect = False
        self._reconnecting = False
        self._reconnect_abort: asyncio.Event | None = None
        self._session_token: str | None = None

        self._store = StoreAPI(self.request, self._subscription_manager)
        self._rules = RulesAPI(self.request, self._subscription_manager)

        self._setup_transport_listeners()

    # ── State ─────────────────────────────────────────────────────

    @property
    def url(self) -> str:
        return self._url

    @property
    def state(self) -> ConnectionState:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == "connected"

    @property
    def store(self) -> StoreAPI:
        return self._store

    @property
    def rules(self) -> RulesAPI:
        return self._rules

    # ── Lifecycle ─────────────────────────────────────────────────

    async def connect(self) -> WelcomeInfo:
        self._intentional_disconnect = False
        self._state = "connecting"

        try:
            welcome = await self._perform_connect()
        except Exception:
            self._state = "disconnected"
            raise

        self._state = "connected"

        if welcome.requires_auth:
            await self._auto_login()

        self._emit_event("connected")
        self._emit_event("welcome", welcome)

        return welcome

    async def disconnect(self) -> None:
        self._intentional_disconnect = True
        if self._reconnect_abort is not None:
            self._reconnect_abort.set()
        self._request_manager.reject_all(
            DisconnectedError("Client disconnecting")
        )
        self._subscription_manager.clear()
        await self._transport.disconnect()
        self._state = "disconnected"

    # ── Request sending ───────────────────────────────────────────

    async def request(
        self, msg_type: str, payload: dict[str, Any] | None = None
    ) -> Any:
        """Send a typed request and wait for the response."""
        if self._state != "connected":
            raise DisconnectedError(
                f"Cannot send request — client is {self._state}"
            )
        return await self._request_manager.send(
            self._transport, msg_type, payload
        )

    # ── Events ────────────────────────────────────────────────────

    def on(self, event: str, handler: Callable[..., Any]) -> Unsubscribe:
        """Register an event handler. Returns a function to unsubscribe."""
        listeners = self._listeners.setdefault(event, [])
        listeners.append(handler)

        def unsub() -> None:
            try:
                listeners.remove(handler)
            except ValueError:
                pass

        return unsub

    # ── Context manager ───────────────────────────────────────────

    async def __aenter__(self) -> NoexClient:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.disconnect()

    # ── Private ───────────────────────────────────────────────────

    def _emit_event(self, event: str, *args: Any) -> None:
        for handler in self._listeners.get(event, []):
            try:
                handler(*args)
            except Exception:
                logger.exception("Event handler error for %s", event)

    def _setup_transport_listeners(self) -> None:
        self._transport.on("message", self._on_transport_message)
        self._transport.on("close", self._on_transport_close)
        self._transport.on("error", self._on_transport_error)

    def _on_transport_message(self, data: str) -> None:
        try:
            msg = json.loads(data)
        except (json.JSONDecodeError, TypeError):
            return

        if not isinstance(msg, dict):
            return

        self._handle_message(msg)

    def _on_transport_close(self, code: int, reason: str) -> None:
        if not self._intentional_disconnect:
            self._request_manager.reject_all(
                DisconnectedError("Connection lost")
            )

        if (
            not self._intentional_disconnect
            and self._reconnect_strategy is not None
            and not self._reconnecting
        ):
            self._reconnecting = True
            asyncio.get_running_loop().create_task(self._handle_reconnect())
        elif not self._reconnecting:
            self._state = "disconnected"
            self._emit_event("disconnected", reason)

    def _on_transport_error(self, error: Exception) -> None:
        self._emit_event("error", error)

    def _handle_message(self, msg: dict[str, Any]) -> None:
        if self._request_manager.handle_message(msg):
            return

        if self._push_router.handle_message(msg):
            return

        if msg.get("type") == "system":
            self._handle_system_message(msg)

    def _handle_system_message(self, msg: dict[str, Any]) -> None:
        if msg.get("event") == "session_revoked":
            reason = msg.get("reason", "Session revoked by administrator")
            if not isinstance(reason, str):
                reason = "Session revoked by administrator"
            self._intentional_disconnect = True
            self._emit_event("session_revoked", reason)

    async def _perform_connect(self) -> WelcomeInfo:
        welcome_future: asyncio.Future[WelcomeInfo] = (
            asyncio.get_running_loop().create_future()
        )
        timeout_ms = self._options.connect_timeout_ms

        timeout_handle = asyncio.get_running_loop().call_later(
            timeout_ms / 1000,
            lambda: (
                welcome_future.set_exception(
                    ConnectionError(
                        f"Timeout waiting for welcome message after {timeout_ms}ms"
                    )
                )
                if not welcome_future.done()
                else None
            ),
        )

        def on_welcome_message(data: str) -> None:
            try:
                msg = json.loads(data)
            except (json.JSONDecodeError, TypeError):
                return

            if isinstance(msg, dict) and msg.get("type") == "welcome":
                timeout_handle.cancel()
                unsub()
                if not welcome_future.done():
                    welcome_future.set_result(
                        WelcomeInfo(
                            version=msg.get("version", ""),
                            server_time=msg.get("serverTime", 0),
                            requires_auth=msg.get("requiresAuth", False),
                        )
                    )

        unsub = self._transport.on("message", on_welcome_message)

        try:
            await self._transport.connect()
        except Exception:
            timeout_handle.cancel()
            unsub()
            welcome_future.cancel()
            raise

        return await welcome_future

    # ── Auto-login ────────────────────────────────────────────────

    async def _auto_login(self) -> None:
        if self._session_token is not None:
            try:
                await self.request("auth.login", {"token": self._session_token})
                return
            except Exception:
                self._session_token = None

        if self._options.auth and self._options.auth.token:
            await self.request("auth.login", {"token": self._options.auth.token})
            return

        if self._options.auth and self._options.auth.credentials:
            cred = self._options.auth.credentials
            result = await self.request(
                "identity.login",
                {"username": cred.username, "password": cred.password},
            )
            if isinstance(result, dict):
                self._session_token = result.get("token")

    # ── Reconnect ─────────────────────────────────────────────────

    async def _handle_reconnect(self) -> None:
        self._state = "reconnecting"
        attempt = 0

        while not self._intentional_disconnect:
            delay = self._reconnect_strategy.get_delay(attempt)  # type: ignore[union-attr]
            if delay is None:
                self._state = "disconnected"
                self._reconnecting = False
                self._emit_event(
                    "disconnected", "Max reconnect attempts reached"
                )
                self._emit_event(
                    "error", Exception("Max reconnect attempts reached")
                )
                return

            self._emit_event("reconnecting", attempt + 1)

            self._reconnect_abort = asyncio.Event()
            try:
                await asyncio.wait_for(
                    self._reconnect_abort.wait(),
                    timeout=delay / 1000,
                )
            except asyncio.TimeoutError:
                pass

            if self._intentional_disconnect:
                break

            try:
                welcome = await self._perform_connect()
                if self._intentional_disconnect:
                    break

                self._state = "connected"

                if welcome.requires_auth:
                    await self._auto_login()
                if self._intentional_disconnect:
                    break

                await self._subscription_manager.resubscribe_all(self.request)
                if self._intentional_disconnect:
                    break

                self._emit_event("connected")
                self._emit_event("reconnected")
                self._emit_event("welcome", welcome)
                self._reconnecting = False
                return
            except Exception:
                self._state = "reconnecting"
                attempt += 1

        self._state = "disconnected"
        self._reconnecting = False

    def _create_reconnect_strategy(self) -> ReconnectStrategy | None:
        if self._options.reconnect is False:
            return None
        opts = (
            self._options.reconnect
            if isinstance(self._options.reconnect, ReconnectOptions)
            else None
        )
        return ReconnectStrategy(opts)
