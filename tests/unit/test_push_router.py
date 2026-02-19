from __future__ import annotations

from noex_client.protocol.push_router import PushRouter


class TestPushRouter:
    def test_routes_push_message(self) -> None:
        received: list[tuple] = []
        router = PushRouter(lambda sid, ch, data: received.append((sid, ch, data)))

        result = router.handle_message({
            "type": "push",
            "subscriptionId": "sub-1",
            "channel": "subscription",
            "data": {"records": []},
        })

        assert result is True
        assert len(received) == 1
        assert received[0] == ("sub-1", "subscription", {"records": []})

    def test_ignores_non_push(self) -> None:
        received: list = []
        router = PushRouter(lambda *a: received.append(a))

        assert router.handle_message({"type": "result", "id": 1}) is False
        assert router.handle_message({"type": "welcome"}) is False
        assert router.handle_message({"type": "ping"}) is False
        assert len(received) == 0

    def test_ignores_invalid_push(self) -> None:
        received: list = []
        router = PushRouter(lambda *a: received.append(a))

        # Missing subscriptionId
        assert router.handle_message({"type": "push", "channel": "subscription"}) is False
        # Missing channel
        assert router.handle_message({"type": "push", "subscriptionId": "s1"}) is False
        # Wrong types
        assert router.handle_message({"type": "push", "subscriptionId": 123, "channel": "x"}) is False
        assert len(received) == 0
