"""Validated tool registry (Phase 4)."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

__all__ = ["ToolConfig", "ToolRegistry", "ToolCallError", "validate_input"]


class ToolCallError(Exception):
    """Raised when a tool call is rejected before/at execution."""

    def __init__(self, message: str, *, retryable: bool = True) -> None:
        super().__init__(message)
        self.retryable = retryable


def validate_input(schema: Optional[Dict[str, Any]], value: Any) -> None:
    """Validate value against a minimal JSON-Schema subset."""
    if not schema:
        return
    expected = schema.get("type")
    if expected and not _matches_type(expected, value):
        raise ToolCallError(
            f"input type mismatch: expected {expected}, got {type(value).__name__}",
            retryable=False,
        )
    if expected == "object" or isinstance(value, dict):
        for req in schema.get("required", []):
            if req not in value:
                raise ToolCallError(f"missing required field {req!r}", retryable=False)
        for key, subschema in schema.get("properties", {}).items():
            if key in value:
                validate_input(subschema, value[key])
    if expected == "array" or isinstance(value, list):
        item_schema = schema.get("items")
        if item_schema:
            for i, item in enumerate(value):
                try:
                    validate_input(item_schema, item)
                except ToolCallError as exc:
                    raise ToolCallError(f"item[{i}]: {exc}", retryable=False) from exc
    if "enum" in schema and value not in schema["enum"]:
        raise ToolCallError(f"value {value!r} not in enum {schema['enum']}", retryable=False)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if "minimum" in schema and value < schema["minimum"]:
            raise ToolCallError(f"value {value} < minimum {schema['minimum']}", retryable=False)
        if "maximum" in schema and value > schema["maximum"]:
            raise ToolCallError(f"value {value} > maximum {schema['maximum']}", retryable=False)


def _matches_type(expected: str, value: Any) -> bool:
    if expected == "object":
        return isinstance(value, dict)
    if expected == "array":
        return isinstance(value, list)
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    return True


@dataclass
class ToolConfig:
    """Configuration and runtime state for a single tool."""

    name: str
    handler: Callable[..., Any]
    input_schema: Optional[Dict[str, Any]] = None
    timeout: float = 30.0
    failure_threshold: int = 3
    cooldown_seconds: float = 30.0
    max_calls_per_window: int = 60
    window_seconds: float = 60.0
    _consecutive_failures: int = field(default=0, init=False, repr=False)
    _opened_at: float = field(default=0.0, init=False, repr=False)
    _call_times: List[float] = field(default_factory=list, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def record_failure(self) -> None:
        with self._lock:
            if time.monotonic() - self._opened_at < self.cooldown_seconds:
                return
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.failure_threshold:
                self._opened_at = time.monotonic()
                self._consecutive_failures = 0

    def record_success(self) -> None:
        with self._lock:
            self._consecutive_failures = 0

    @property
    def is_open(self) -> bool:
        with self._lock:
            if time.monotonic() - self._opened_at < self.cooldown_seconds:
                return True
            self._opened_at = 0.0
            return False

    def allow(self) -> bool:
        """Rate limit: allow at most max_calls_per_window calls per window_seconds."""
        with self._lock:
            now = time.monotonic()
            self._call_times = [t for t in self._call_times if now - t < self.window_seconds]
            if len(self._call_times) >= self.max_calls_per_window:
                return False
            self._call_times.append(now)
            return True


class ToolRegistry:
    """Registry of validated tools with timeout, circuit breaker, rate limit."""

    def __init__(self) -> None:
        self._tools: Dict[str, ToolConfig] = {}

    def register(self, name: str, handler: Callable[..., Any], schema: Optional[Dict[str, Any]] = None, **kwargs: Any) -> ToolConfig:
        cfg = ToolConfig(name=name, handler=handler, input_schema=schema, **kwargs)
        self._tools[name] = cfg
        return cfg

    def get(self, name: str) -> Optional[ToolConfig]:
        return self._tools.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    def __len__(self) -> int:
        return len(self._tools)
