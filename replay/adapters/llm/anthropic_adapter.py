"""Anthropic LLM adapter stub for Phase 4."""

from __future__ import annotations

from typing import Any, Dict, List

from replay.core.llmcalled import LlmAdapterInterface


class AnthropicAdapter(LlmAdapterInterface):
    """Adapter for Anthropic API (stub for demonstration)."""

    def __init__(self, api_key: str, model: str = "claude-sonnet-5") -> None:
        self.api_key = api_key
        self.model = model

    def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Stub: in real impl, call Anthropic API
        return {"content": f"[{self.model} stub response]", "tokens": 50}

    def token_usage(self, messages: List[Dict[str, Any]]) -> Dict[str, int]:
        # Approximate: 1 token ≈ 4 chars
        return {"input": sum(len(m.get("content", "")) // 4 for m in messages), "output": 0}

    def cost_estimate(self, params: Dict[str, Any]) -> float:
        # Claude Sonnet pricing: $0.015/1k input, $0.075/1k output
        return (params.get("input_tokens", 0) * 0.015 + params.get("output_tokens", 0) * 0.075) / 1000