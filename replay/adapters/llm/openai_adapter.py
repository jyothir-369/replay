"""OpenAI LLM adapter stub for Phase 4."""

from __future__ import annotations

from typing import Any, Dict, List

from replay.core.llmcalled import LlmAdapterInterface


class OpenAIAdapter(LlmAdapterInterface):
    """Adapter for OpenAI API (stub for demonstration)."""

    def __init__(self, api_key: str, model: str = "gpt-4") -> None:
        self.api_key = api_key
        self.model = model

    def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        # Stub: in real impl, call OpenAI API
        return {"content": f"[{self.model} stub response]", "tokens": 50}

    def token_usage(self, messages: List[Dict[str, Any]]) -> Dict[str, int]:
        # Approximate: 1 token ≈ 4 chars
        return {"input": sum(len(m.get("content", "")) // 4 for m in messages), "output": 0}

    def cost_estimate(self, params: Dict[str, Any]) -> float:
        # GPT-4 pricing: $0.03/1k input, $0.06/1k output
        return (params.get("input_tokens", 0) * 0.03 + params.get("output_tokens", 0) * 0.06) / 1000