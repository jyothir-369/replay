"""Retry policy: exponential backoff with jitter, max attempts."""

from __future__ import annotations

import random
import time


class RetryPolicy:
    """Per-step retry configuration.

    Attributes:
        max_attempts: total number of calls before giving up.
        base_delay: initial backoff delay in seconds.
        max_delay: cap on backoff delay.
        jitter: whether to add random jitter (True = full jitter).
    """

    def __init__(self, max_attempts: int = 3, base_delay: float = 1.0, max_delay: float = 60.0, jitter: bool = True):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def schedule(self, attempt: int) -> float:
        """Return the next delay (seconds) before the next retry attempt."""
        if attempt >= self.max_attempts:
            return 0.0
        delay = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        if self.jitter:
            delay *= (0.5 + random.random() * 0.5)  # uniform(0.25, 1.0)
        return delay

    def should_retry(self, error: str | None = None) -> bool:
        """Decide whether an error is retryable vs fatal."""
        if not error:
            return False
        error_lower = error.lower()
        # Retryable: network errors, timeout, transient failures
        retryable_patterns = [
            "timeout", "connection refused", "reset", "5xx",
            "network error", "unavailable",
        ]
        if any(p in error_lower for p in retryable_patterns):
            return True
        # Fatal: auth failure, permission denied, invalid input
        fatal_patterns = ["auth", "forbidden", "invalid", "denied"]
        if any(p in error_lower for p in fatal_patterns):
            return False
        return False
