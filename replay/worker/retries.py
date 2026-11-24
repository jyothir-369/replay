"""Retry policy: bounded exponential backoff with full jitter.

RetryPolicy is a pure value object — it never touches the DB or the clock.
The caller (worker loop) reads the attempt count from the task row and asks
the policy whether to retry and how long to wait before re-queuing.

Error classification:
  - Retryable: transient network or server-side failures.
  - Fatal: permanent errors (auth, validation) that will not clear on retry.
  - Unknown errors default to fatal so we don't loop forever on unexpected bugs.
"""

from __future__ import annotations

import random

__all__ = ["RetryDecision", "RetryPolicy"]


class RetryDecision:
    """Result of asking a RetryPolicy what to do after a failure."""

    def __init__(self, should_retry: bool, delay: float = 0.0, reason: str = "") -> None:
        self.should_retry = should_retry
        self.delay = delay          # seconds to wait before next attempt
        self.reason = reason

    def __repr__(self) -> str:
        return f"RetryDecision(retry={self.should_retry}, delay={self.delay:.2f}s, reason={self.reason!r})"


class RetryPolicy:
    """Per-step retry configuration with exponential backoff and full jitter.

    Attempt numbering starts at 1 (first execution).  max_attempts is the
    total number of executions allowed, so max_attempts=3 means one original
    attempt plus two retries.

    delay(attempt) = uniform(0, min(base_delay * 2^(attempt-1), max_delay))
    """

    # Patterns that indicate a transient error worth retrying.
    _RETRYABLE = (
        "timeout",
        "timed out",
        "connection refused",
        "connection reset",
        "connection error",
        "network error",
        "unavailable",
        "service unavailable",
        "too many requests",
        "rate limit",
        "5xx",
        "500",
        "502",
        "503",
        "504",
    )
    # Patterns that indicate a permanent error we should not retry.
    _FATAL = (
        "auth",
        "forbidden",
        "unauthorized",
        "permission denied",
        "invalid",
        "bad request",
        "not found",
        "400",
        "401",
        "403",
        "404",
        "422",
    )

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        jitter: bool = True,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter

    def evaluate(self, attempt: int, error: str | None = None) -> RetryDecision:
        """Return a RetryDecision given the current attempt number and error message.

        attempt: the 1-based number of the attempt that just failed.
        error: the error string from the failed execution (may be None).
        """
        if attempt >= self.max_attempts:
            return RetryDecision(False, reason="max_attempts reached")

        if error and self._is_fatal(error):
            return RetryDecision(False, reason=f"fatal error: {error[:80]}")

        if error is None or not self._is_retryable(error):
            # Unknown errors are treated as fatal by default.
            return RetryDecision(False, reason="non-retryable error")

        delay = self._compute_delay(attempt)
        return RetryDecision(True, delay=delay, reason="retryable error")

    def schedule(self, attempt: int) -> float:
        """Return the delay in seconds before retry number `attempt`.

        attempt is 1-based (attempt=2 means the second execution, i.e. first retry).
        Returns 0.0 if attempt >= max_attempts (no retry should happen).
        """
        if attempt >= self.max_attempts:
            return 0.0
        return self._compute_delay(attempt)

    # ------------------------------------------------------------------
    # Backward-compatible helper kept for existing call-sites
    # ------------------------------------------------------------------

    def should_retry(self, error: str | None = None, attempt: int = 1) -> bool:
        """Return True if this error is retryable and attempts remain."""
        return self.evaluate(attempt, error).should_retry

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _compute_delay(self, attempt: int) -> float:
        cap = min(self.base_delay * (2 ** (attempt - 1)), self.max_delay)
        if self.jitter:
            return random.uniform(0, cap)
        return cap

    def _is_retryable(self, error: str) -> bool:
        low = error.lower()
        return any(p in low for p in self._RETRYABLE)

    def _is_fatal(self, error: str) -> bool:
        low = error.lower()
        return any(p in low for p in self._FATAL)
