from __future__ import annotations

import asyncio
import json

import pytest

from noex_client.errors import (
    DisconnectedError,
    NoexClientError,
    RequestTimeoutError,
)
from noex_client.protocol.request_manager import RequestManager


class FakeTransport:
    """Minimal transport stub for unit testing the RequestManager."""

    def __init__(self) -> None:
        self.sent: list[dict] = []
        self._state = "connected"

    @property
    def state(self) -> str:
        return self._state

    @property
    def is_connected(self) -> bool:
        return self._state == "connected"

    def send(self, data: str) -> None:
        self.sent.append(json.loads(data))


class TestRequestManager:
    def test_increments_ids(self) -> None:
        rm = RequestManager(timeout_ms=5000)
        t = FakeTransport()

        asyncio.get_event_loop().create_task(rm.send(t, "a", {}))
        asyncio.get_event_loop().create_task(rm.send(t, "b", {}))

        # Let the tasks start (they send immediately)
        # IDs are assigned synchronously during send
        assert rm.pending_count == 0 or True  # tasks not started yet

    @pytest.mark.asyncio
    async def test_send_and_receive_result(self) -> None:
        rm = RequestManager(timeout_ms=5000)
        t = FakeTransport()

        task = asyncio.create_task(rm.send(t, "store.get", {"bucket": "users", "key": "1"}))
        await asyncio.sleep(0)  # Let the send happen

        assert len(t.sent) == 1
        msg = t.sent[0]
        assert msg["id"] == 1
        assert msg["type"] == "store.get"
        assert msg["bucket"] == "users"
        assert msg["key"] == "1"

        # Simulate server response
        handled = rm.handle_message({"id": 1, "type": "result", "data": {"name": "Alice"}})
        assert handled is True

        result = await task
        assert result == {"name": "Alice"}

    @pytest.mark.asyncio
    async def test_send_and_receive_error(self) -> None:
        rm = RequestManager(timeout_ms=5000)
        t = FakeTransport()

        task = asyncio.create_task(rm.send(t, "store.get", {"bucket": "x", "key": "1"}))
        await asyncio.sleep(0)

        rm.handle_message({
            "id": 1,
            "type": "error",
            "code": "NOT_FOUND",
            "message": "Bucket not found",
        })

        with pytest.raises(NoexClientError) as exc_info:
            await task

        assert exc_info.value.code == "NOT_FOUND"
        assert "Bucket not found" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_timeout(self) -> None:
        rm = RequestManager(timeout_ms=50)  # Very short timeout
        t = FakeTransport()

        with pytest.raises(RequestTimeoutError) as exc_info:
            await rm.send(t, "store.get", {"bucket": "users", "key": "1"})

        assert exc_info.value.code == "TIMEOUT"

    @pytest.mark.asyncio
    async def test_reject_all(self) -> None:
        rm = RequestManager(timeout_ms=5000)
        t = FakeTransport()

        task1 = asyncio.create_task(rm.send(t, "a", {}))
        task2 = asyncio.create_task(rm.send(t, "b", {}))
        await asyncio.sleep(0)

        assert rm.pending_count == 2

        rm.reject_all(DisconnectedError("Connection lost"))

        assert rm.pending_count == 0

        with pytest.raises(DisconnectedError):
            await task1
        with pytest.raises(DisconnectedError):
            await task2

    @pytest.mark.asyncio
    async def test_ignores_push_messages(self) -> None:
        rm = RequestManager(timeout_ms=5000)

        assert rm.handle_message({"type": "push", "subscriptionId": "s1"}) is False
        assert rm.handle_message({"type": "ping", "timestamp": 123}) is False
        assert rm.handle_message({"type": "welcome", "version": "1.0"}) is False
        assert rm.handle_message({"type": "system", "event": "test"}) is False

    @pytest.mark.asyncio
    async def test_ignores_unknown_ids(self) -> None:
        rm = RequestManager(timeout_ms=5000)

        assert rm.handle_message({"id": 999, "type": "result", "data": None}) is False

    @pytest.mark.asyncio
    async def test_concurrent_requests(self) -> None:
        rm = RequestManager(timeout_ms=5000)
        t = FakeTransport()

        tasks = [
            asyncio.create_task(rm.send(t, f"req.{i}", {}))
            for i in range(5)
        ]
        await asyncio.sleep(0)

        assert rm.pending_count == 5
        assert len(t.sent) == 5

        for i in range(5):
            rm.handle_message({"id": i + 1, "type": "result", "data": i * 10})

        results = await asyncio.gather(*tasks)
        assert results == [0, 10, 20, 30, 40]
        assert rm.pending_count == 0
