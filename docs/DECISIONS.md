# Architecture Decision Records (ADRs)

## ADR-0001: Why Postgres-only

Replay's core loop is event sourcing plus a durable queue plus a relational store.
Postgres alone covers all three; adding Redis or a message broker would add operational
complexity without removing a real failure mode. Decision can be revisited if scale demands.

## ADR-0002: Why event sourcing

State is derived by folding events (pure reducer). Mutable state is never trusted.
This gives replay, audit, and fork semantics for free.

## ADR-0003: Why hash-chain, not blockchain

A hash chain over SHA-256 within one run is sufficient for tamper evidence at the
operational scale of agent runs; no distributed consensus is needed.

## ADR-0004: Why SKIP LOCKED queues

Postgres-native queue with `FOR UPDATE SKIP LOCKED` avoids a separate broker while
providing lease-based execution and at-least-once delivery.

## ADR-0005: Why record/replay cassettes

Every external I/O is captured in events; replay substitutes recorded outputs and
fails if any network call is attempted.

---

## ADR-0006: Write-ahead events before tool execution

The two-generals problem (tool executed but result not recorded) is solved by writing the
`ToolCalled` idempotency-key event before execution, then recording the result. On resume,
the idempotency table is checked first.

## ADR-0007: Deterministic replays via injected `now()` / `random()`

Non-determinism is recorded through the event log, not papered over by seeding.
