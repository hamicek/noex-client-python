from __future__ import annotations

import random

from ..config import ReconnectOptions


class ReconnectStrategy:
    """Exponential backoff with jitter for reconnection attempts."""

    def __init__(self, options: ReconnectOptions | None = None) -> None:
        opts = options or ReconnectOptions()
        self._max_retries = opts.max_retries
        self._initial_delay_ms = opts.initial_delay_ms
        self._max_delay_ms = opts.max_delay_ms
        self._backoff_multiplier = opts.backoff_multiplier
        self._jitter_ms = opts.jitter_ms

    def get_delay(self, attempt: int) -> float | None:
        """Return delay in milliseconds, or None if max retries exceeded."""
        if attempt >= self._max_retries:
            return None

        base = self._initial_delay_ms * (self._backoff_multiplier**attempt)
        capped = min(base, self._max_delay_ms)
        jitter = random.random() * self._jitter_ms
        return capped + jitter
