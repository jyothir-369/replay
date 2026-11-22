# Product Requirements Document (PRD): Replay

**Project:** Replay — Durable, Forkable, Auditable Agent Runtime

**Target role:** Emergence AI — Agent Runtime & Workflow Engine, Trusted AI Platform

**Duration:** ~6–8 weeks

---

## 1. Problem

At 2 a.m. a fab's sensor logs show a yield drop on three lots. An AI agent reads the lot history, proposes placing the lots on hold, and must wait for a process engineer's approval.

Midway, the MES (manufacturing execution system) API times out. The worker crashes. Six hours later an auditor asks:

> *"What did the agent see, what did it decide, who approved it, and can you prove nobody edited the record?"*

Today most agent frameworks lose state on crash, double-fire side-effects on retry, and cannot answer the auditor.

## 2. Personas

| Persona | Needs |
|---|---|
| Platform engineer | Durable execution, crash-safety, retries, observability. |
| Compliance reviewer | Tamper-evident audit trail, approver identity, exact decision payload. |
| Agent developer | Declarative workflows, policies, fork/branch semantics. |

## 3. Success Criteria

- 1,000 chaos-killed runs complete with zero duplicated side-effects.
- Any run can be replayed byte-identically offline and forked from any checkpoint.
- `replay audit verify` detects tampering of any single row.

## 4. Event Vocabulary

The 18 event types (see `replay/core/events.py`):

`RunCreated, StepScheduled, StepStarted, LLMCalled, ToolCalled, ToolReturned, StepSucceeded, StepFailed, RetryScheduled, ApprovalRequested, ApprovalGranted, ApprovalRejected, PolicyDenied, RunPaused, RunResumed, RunForked, RunCompleted, RunFailed`.

## 5. Workflow Schema

YAML + JSON-Schema. Step types: `llm`, `tool`, `approval`, `branch`, `parallel`. Per-step `retry`, `timeout`, `idempotency_key`, `policy`.

## 6. Non-Goals (Phase 1)

This phase delivers docs and contracts only. Phase 2 begins event store implementation.
