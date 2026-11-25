"""LLM Called event and adapter interface for Phase 4."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from replay.core.events import Event, EVENT_VOCABULARY


__all__ = ["LLMCalledEvent", "LlmAdapterInterface"]


class LlmAdapterInterface:
    """Adapter interface for LLM providers."""

    def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        raise NotImplementedError

    def token_usage(self, messages: List[Dict[str, Any]]) -> Dict[str, int]:
        raise NotImplementedError

    def cost_estimate(self, params: Dict[str, Any]) -> float:
        raise NotImplementedError


@dataclass(frozen=True)
class LLMCalledEvent(Event):
    """Event type for LLM calls (event_type = 'LLMCalled')."""
    model: str = ""
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: float = 0.0
    messages: List[Dict[str, Any]] = field(default_factory=list)
    tools: Dict[str, Any] = field(default_factory=dict)
    params: Dict[str, Any] = field(default_factory=dict)

    @property
    def type_name(self) -> str:
        return "LLMCalled"