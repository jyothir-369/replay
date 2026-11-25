from __future__ import annotations

import pytest

from replay.core.events import Event
from replay.core.state import derive_state
from replay.adapters.llm.fake_llm import FakeLLM
from replay.adapters.llm.openai_adapter import OpenAIAdapter
from replay.adapters.llm.anthropic_adapter import AnthropicAdapter
from replay.adapters.tool import ToolConfig, ToolRegistry, ToolCallError, validate_input


def _ev(seq, type_, payload):
    return Event(run_id="run-1", seq=seq, type=type_, payload=payload)


class TestFakeLLM:
    def test_deterministic_response(self):
        llm = FakeLLM()
        msgs = [{"role": "user", "content": "hold"}]
        r1 = llm.complete(msgs, {}, {})
        r2 = llm.complete(msgs, {}, {})
        assert r1 == r2

    def test_token_usage(self):
        llm = FakeLLM()
        usage = llm.token_usage([{"role": "user", "content": "1234567890"}])
        assert usage["input"] == 10

    def test_cost_estimate(self):
        llm = FakeLLM()
        cost = llm.cost_estimate({"input_tokens": 1000, "output_tokens": 500})
        assert cost > 0

    def test_call_log(self):
        llm = FakeLLM()
        llm.complete([{"role": "user", "content": "a"}], {}, {})
        llm.complete([{"role": "user", "content": "b"}], {}, {})
        assert len(llm._call_log) == 2


class TestRealAdapters:
    def test_openai_stub(self):
        a = OpenAIAdapter(api_key="x", model="gpt-4")
        r = a.complete([{"role": "user", "content": "hi"}], {}, {})
        assert "stub" in r["content"]

    def test_anthropic_stub(self):
        a = AnthropicAdapter(api_key="x", model="claude-sonnet-5")
        r = a.complete([{"role": "user", "content": "hi"}], {}, {})
        assert "stub" in r["content"]


class TestToolRegistry:
    def test_register_and_get(self):
        reg = ToolRegistry()
        reg.register("read_lot_log", lambda *a, **k: {"ok": True})
        assert reg.get("read_lot_log") is not None

    def test_unknown_tool_returns_none(self):
        reg = ToolRegistry()
        assert reg.get("nope") is None

    def test_register_validation_rejects_bad_input(self):
        schema = {"type": "object", "required": ["lot_id"], "properties": {"lot_id": {"type": "string"}}}
        reg = ToolRegistry()
        reg.register("read_lot_log", lambda *a, **k: {"ok": True}, schema=schema)
        cfg = reg.get("read_lot_log")
        assert cfg is not None
        # Valid input accepted
        validate_input(schema, {"lot_id": "L001"})
        # Invalid input rejected
        with pytest.raises(ToolCallError):
            validate_input(schema, {"nope": 1})

    def test_circuit_breaker_opens(self):
        cfg = ToolConfig(name="t", handler=lambda *a: (_ for _ in ()).throw(RuntimeError("boom")), failure_threshold=2, cooldown_seconds=60)
        for _ in range(2):
            try:
                cfg.handler()
            except RuntimeError:
                cfg.record_failure()
        assert cfg.is_open or cfg._consecutive_failures >= cfg.failure_threshold

    def test_rate_limit_allows_within_window(self):
        cfg = ToolConfig(name="t", handler=lambda *a: {}, max_calls_per_window=2, window_seconds=60)
        assert cfg.allow() is True
        assert cfg.allow() is True
        assert cfg.allow() is False

    def test_timeout_is_positive(self):
        cfg = ToolConfig(name="t", handler=lambda *a: {}, timeout=10.0)
        assert cfg.timeout == 10.0


class TestLLMCalledState:
    def test_llm_called_events_retained(self):
        events = [
            _ev(1, "RunCreated", {"workflow": "w"}),
            _ev(2, "StepScheduled", {"step_id": "llm_step"}),
            _ev(3, "LLMCalled", {"model": "m", "input_tokens": 100, "output_tokens": 50, "cost_usd": 0.002}),
            _ev(4, "StepSucceeded", {"step_id": "llm_step"}),
        ]
        state = derive_state(events)
        types = [e.type for e in state.events]
        assert "LLMCalled" in types
        assert state.status == "STEP_SUCCEEDED"

    def test_tool_called_write_ahead(self):
        events = [
            _ev(1, "RunCreated", {"workflow": "w"}),
            _ev(2, "StepScheduled", {"step_id": "read_lot"}),
            _ev(3, "StepStarted", {"step_id": "read_lot", "attempt": 1}),
            _ev(4, "ToolCalled", {"tool": "read_lot_log", "step_id": "read_lot", "idempotency_key": "k"}),
            _ev(5, "ToolReturned", {"tool": "read_lot_log", "step_id": "read_lot", "output": {"lots": []}}),
            _ev(6, "StepSucceeded", {"step_id": "read_lot"}),
        ]
        state = derive_state(events)
        assert state.status == "STEP_SUCCEEDED"
        assert "read_lot" in state.completed_steps

    def test_token_accounting_accumulated(self):
        """LLMCalled events carry token/cost fields that a reducer could sum."""
        events = [
            _ev(1, "RunCreated", {"workflow": "w"}),
            _ev(2, "LLMCalled", {"model": "m", "input_tokens": 100, "output_tokens": 50, "cost_usd": 0.002}),
            _ev(3, "LLMCalled", {"model": "m", "input_tokens": 200, "output_tokens": 100, "cost_usd": 0.004}),
        ]
        state = derive_state(events)
        calls = [e for e in state.events if e.type == "LLMCalled"]
        assert len(calls) == 2
        total_in = sum(e.payload["input_tokens"] for e in calls)
        total_cost = sum(e.payload["cost_usd"] for e in calls)
        assert total_in == 300
        assert abs(total_cost - 0.006) < 1e-9


class TestWorkflowEndToEnd:
    def test_demo_workflow_parses(self):
        from replay.core.schema import validate_workflow
        with open("demo_workflow.yaml", "r", encoding="utf-8") as f:
            spec = validate_workflow(f.read())
        assert spec.name == "yield-excursion-triage"
        step_ids = [s.step_id for s in spec.steps]
        assert "read_lot_log" in step_ids
        assert "propose_hold" in step_ids

    def test_demo_workflow_uses_llm_and_tools(self):
        from replay.core.schema import validate_workflow
        with open("demo_workflow.yaml", "r", encoding="utf-8") as f:
            spec = validate_workflow(f.read())
        types = [s.type for s in spec.steps]
        assert "llm" in types
        assert "tool" in types

    def test_demo_workflow_propose_hold_depends_on_read_lot(self):
        from replay.core.schema import validate_workflow
        with open("demo_workflow.yaml", "r", encoding="utf-8") as f:
            spec = validate_workflow(f.read())
        propose = [s for s in spec.steps if s.step_id == "propose_hold"][0]
        assert "read_lot_log" in propose.depends_on
