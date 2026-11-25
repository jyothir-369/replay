"""Fake LLM for Phase 4 testing - deterministic scripted responses."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Dict, List

from replay.core.llmcalled import LlmAdapterInterface


class FakeLLM(LlmAdapterInterface):
    """Deterministic LLM that returns scripted outputs for testing.

    Supports:
    - Scripted responses keyed by message hash
    - Token/cost accounting for deterministic replay
    - Tool calls with matching tool IDs
    """

    def __init__(self, scripted_responses: Dict[str, Dict[str, Any]] = None) -> None:
        self._scripted = scripted_responses or {}
        self._call_log: List[Dict[str, Any]] = []

    def complete(
        self,
        messages: List[Dict[str, Any]],
        tools: Dict[str, Any],
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Return a scripted response based on the message hash."""
        msg_hash = self._hash_messages(messages)
        response = self._scripted.get(msg_hash, self._default_response(messages))
        self._call_log.append({
            "messages": messages,
            "tools": tools,
            "params": params,
            "response": response,
        })
        return response

    def token_usage(self, messages: List[Dict[str, Any]]) -> Dict[str, int]:
        """Estimate tokens from message lengths."""
        input_tokens = sum(len(msg.get("content", "")) for msg in messages)
        output_tokens = self._count_response_tokens(messages)
        return {"input": input_tokens, "output": output_tokens}

    def cost_estimate(self, params: Dict[str, Any]) -> float:
        """Fake cost: 0.002 per 1k tokens."""
        return 0.002 * (params.get("input_tokens", 0) + params.get("output_tokens", 0))

    def _hash_messages(self, messages: List[Dict[str, Any]]) -> str:
        canonical = json.dumps(messages, sort_keys=True, ensure_ascii=False).encode()
        return hashlib.sha256(canonical).hexdigest()

    def _default_response(self, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
        last_content = messages[-1].get("content", "") if messages else ""
        if "hold" in last_content.lower():
            return {"action": "propose_hold", "reason": "yield excursion detected", "lots": ["L001", "L002"]}
        return {"action": "continue", "reason": "no excursion"}

    def _count_response_tokens(self, messages: List[Dict[str, Any]]) -> int:
        return sum(len(msg.get("content", "")) for msg in messages)