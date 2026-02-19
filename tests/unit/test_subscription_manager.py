from __future__ import annotations

import pytest

from noex_client.subscription.subscription_manager import (
    ResubscribeInfo,
    SubscriptionEntry,
    SubscriptionManager,
)


class TestSubscriptionManager:
    def test_register_and_count(self) -> None:
        sm = SubscriptionManager()
        assert sm.count == 0

        sm.register(SubscriptionEntry(
            id="s1",
            channel="subscription",
            callback=lambda d: None,
            resubscribe=ResubscribeInfo(type="store.subscribe", payload={"query": "q1"}),
        ))
        assert sm.count == 1

    def test_unregister(self) -> None:
        sm = SubscriptionManager()
        sm.register(SubscriptionEntry(
            id="s1",
            channel="subscription",
            callback=lambda d: None,
            resubscribe=ResubscribeInfo(type="store.subscribe", payload={}),
        ))
        sm.unregister("s1")
        assert sm.count == 0

    def test_unregister_nonexistent_is_noop(self) -> None:
        sm = SubscriptionManager()
        sm.unregister("nonexistent")  # Should not raise

    def test_handle_push(self) -> None:
        received: list = []
        sm = SubscriptionManager()
        sm.register(SubscriptionEntry(
            id="s1",
            channel="subscription",
            callback=lambda d: received.append(d),
            resubscribe=ResubscribeInfo(type="store.subscribe", payload={}),
        ))

        sm.handle_push("s1", {"records": [1, 2, 3]})
        assert received == [{"records": [1, 2, 3]}]

    def test_handle_push_ignores_unknown(self) -> None:
        sm = SubscriptionManager()
        sm.handle_push("unknown", {"data": "test"})  # Should not raise

    def test_handle_push_catches_callback_error(self) -> None:
        sm = SubscriptionManager()
        sm.register(SubscriptionEntry(
            id="s1",
            channel="subscription",
            callback=lambda d: (_ for _ in ()).throw(ValueError("boom")),
            resubscribe=ResubscribeInfo(type="store.subscribe", payload={}),
        ))

        # Should not raise even if callback throws
        sm.handle_push("s1", "data")

    def test_clear(self) -> None:
        sm = SubscriptionManager()
        for i in range(5):
            sm.register(SubscriptionEntry(
                id=f"s{i}",
                channel="subscription",
                callback=lambda d: None,
                resubscribe=ResubscribeInfo(type="store.subscribe", payload={}),
            ))
        assert sm.count == 5
        sm.clear()
        assert sm.count == 0

    @pytest.mark.asyncio
    async def test_resubscribe_all(self) -> None:
        received: list = []
        sm = SubscriptionManager()
        sm.register(SubscriptionEntry(
            id="old-1",
            channel="subscription",
            callback=lambda d: received.append(d),
            resubscribe=ResubscribeInfo(type="store.subscribe", payload={"query": "q1"}),
        ))

        call_log: list = []

        async def fake_send(msg_type: str, payload: dict) -> dict:
            call_log.append((msg_type, payload))
            return {"subscriptionId": "new-1", "data": [{"id": "1"}]}

        await sm.resubscribe_all(fake_send)

        assert call_log == [("store.subscribe", {"query": "q1"})]
        assert sm.count == 1
        # Old ID removed, new ID present
        assert received == [[{"id": "1"}]]
