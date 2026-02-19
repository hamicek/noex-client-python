from __future__ import annotations

from noex_client.config import ReconnectOptions
from noex_client.transport.reconnect import ReconnectStrategy


class TestReconnectStrategy:
    def test_first_attempt_uses_initial_delay(self) -> None:
        strategy = ReconnectStrategy(ReconnectOptions(jitter_ms=0))
        delay = strategy.get_delay(0)
        assert delay is not None
        assert delay == 1000  # initial_delay_ms default

    def test_exponential_backoff(self) -> None:
        strategy = ReconnectStrategy(
            ReconnectOptions(
                initial_delay_ms=100,
                backoff_multiplier=2.0,
                max_delay_ms=100_000,
                jitter_ms=0,
            )
        )
        assert strategy.get_delay(0) == 100
        assert strategy.get_delay(1) == 200
        assert strategy.get_delay(2) == 400
        assert strategy.get_delay(3) == 800

    def test_caps_at_max_delay(self) -> None:
        strategy = ReconnectStrategy(
            ReconnectOptions(
                initial_delay_ms=1000,
                backoff_multiplier=10.0,
                max_delay_ms=5000,
                jitter_ms=0,
            )
        )
        assert strategy.get_delay(0) == 1000
        assert strategy.get_delay(1) == 5000  # 10000 capped to 5000
        assert strategy.get_delay(2) == 5000  # 100000 capped to 5000

    def test_max_retries(self) -> None:
        strategy = ReconnectStrategy(
            ReconnectOptions(max_retries=3, jitter_ms=0)
        )
        assert strategy.get_delay(0) is not None
        assert strategy.get_delay(1) is not None
        assert strategy.get_delay(2) is not None
        assert strategy.get_delay(3) is None  # Exceeded max retries
        assert strategy.get_delay(10) is None

    def test_jitter_adds_randomness(self) -> None:
        strategy = ReconnectStrategy(
            ReconnectOptions(
                initial_delay_ms=1000,
                jitter_ms=500,
                backoff_multiplier=1.0,
                max_delay_ms=100_000,
            )
        )
        delays = {strategy.get_delay(0) for _ in range(20)}
        # With jitter, not all delays should be the same
        assert len(delays) > 1
        # All delays should be in [1000, 1500)
        for d in delays:
            assert d is not None
            assert 1000 <= d < 1500

    def test_default_options(self) -> None:
        strategy = ReconnectStrategy()
        delay = strategy.get_delay(0)
        assert delay is not None
        assert delay >= 1000  # initial_delay_ms default
        assert delay < 1500  # initial_delay_ms + jitter_ms

    def test_infinite_retries_by_default(self) -> None:
        strategy = ReconnectStrategy()
        # Should never return None for reasonable attempts
        assert strategy.get_delay(100) is not None
        assert strategy.get_delay(1000) is not None
