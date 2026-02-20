from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from websockets.asyncio.server import serve, ServerConnection

from noex_client import ClientOptions, NoexClient, ReconnectOptions

FAST_RECONNECT = ReconnectOptions(
    initial_delay_ms=50,
    max_delay_ms=100,
    jitter_ms=0,
    max_retries=5,
)

NON_RETRYABLE_CASES = [
    (1003, "binary_not_supported"),
    (4002, "session_revoked"),
    (4003, "too_many_connections"),
]


async def _create_close_server(
    close_code: int, close_reason: str
) -> tuple[Any, int]:
    """Start a minimal WS server that sends welcome, then closes with given code."""

    async def handler(ws: ServerConnection) -> None:
        await ws.send(
            json.dumps(
                {
                    "type": "welcome",
                    "version": "1.0.0",
                    "serverTime": 0,
                    "requiresAuth": False,
                }
            )
        )
        await asyncio.sleep(0.05)
        await ws.close(close_code, close_reason)

    server = await serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, port


class TestNonRetryableCloseCodes:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("code,reason", NON_RETRYABLE_CASES)
    async def test_does_not_reconnect(self, code: int, reason: str) -> None:
        server, port = await _create_close_server(code, reason)
        try:
            client = NoexClient(
                f"ws://127.0.0.1:{port}",
                ClientOptions(reconnect=FAST_RECONNECT),
            )
            await client.connect()
            assert client.is_connected

            reconnect_attempts: list[int] = []
            disconnected_reasons: list[str] = []
            disconnected_event = asyncio.Event()

            client.on(
                "reconnecting",
                lambda attempt: reconnect_attempts.append(attempt),
            )
            client.on(
                "disconnected",
                lambda r: (disconnected_reasons.append(r), disconnected_event.set()),
            )

            # Wait for the server to close the connection
            await asyncio.wait_for(disconnected_event.wait(), timeout=3)

            assert client.state == "disconnected"
            assert len(disconnected_reasons) == 1
            assert disconnected_reasons[0] == reason

            # Give time to confirm no reconnect attempts
            await asyncio.sleep(0.3)
            assert len(reconnect_attempts) == 0
        finally:
            await client.disconnect()
            server.close()
            await server.wait_closed()

    @pytest.mark.asyncio
    async def test_still_reconnects_on_normal_close(self) -> None:
        """Normal close code (1001) should still trigger reconnect."""
        server, port = await _create_close_server(1001, "going_away")
        try:
            client = NoexClient(
                f"ws://127.0.0.1:{port}",
                ClientOptions(reconnect=FAST_RECONNECT),
            )
            await client.connect()

            reconnect_event = asyncio.Event()
            client.on("reconnecting", lambda _: reconnect_event.set())

            # Wait for at least one reconnect attempt
            await asyncio.wait_for(reconnect_event.wait(), timeout=3)
            assert client.state == "reconnecting"
        finally:
            await client.disconnect()
            server.close()
            await server.wait_closed()
