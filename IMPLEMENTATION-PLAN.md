# Project 1: Replay, Implementation Plan
**Durable, forkable, auditable agent runtime**
Target role: Emergence AI (Agent Runtime & Workflow Engine, Trusted AI Platform)
Duration: ~6–8 weeks · Builder: Jyothir Raghavalu Bhogi

---

## The use-case we are solving

> **Scenario: semiconductor yield-excursion triage.**
> At 2 a.m. a fab's sensor logs show a yield drop on three lots. An AI agent reads the lot history, proposes placing the lots on hold, and must wait for a process engineer's approval. Midway, the MES (manufacturing execution system) API times out. The worker crashes. Six hours later an auditor asks: *"What did the agent see, what did it decide, who approved it, and can you prove nobody edited the record?"*

Today, most agent frameworks lose state on a crash, double-fire side-effects on retry, and cannot answer the auditor. **Replay makes every agent run a durable, resumable, forkable, tamper-evident record.** The engineer who finds a bad decision can rewind to step 14, change one tool output, and branch a new run, like `git branch` for agent behaviour.

(Use only public or synthetic data in the demo, e.g. a synthetic lot log or the public UCI SECOM sensor dataset. Never real customer data.)

## Why this is *your* project

| Your strength (from your profile) | How it shows up here |
|---|---|
| Multi-agent / async work in `hive` | Replay is the production-grade evolution of that idea: durable execution, not just orchestration |
| Data-validation pipelines at flickDone | Event schemas, invariants and validation are the heart of an event-sourced system |
| Python, FastAPI, Postgres, Docker | The entire core stack, no exotic dependencies |
| Mission: "auditable, accountable AI" | Tamper-evident audit and human approval are first-class features, not add-ons |
| Curiosity about systems | Optional Rust/Go sidecar shows you learn languages fast, as the JD asks |

**Builder's statement (paste into the README):**
> I am building Replay because I believe an AI agent that cannot explain and reproduce what it did should not touch production systems. I started with multi-agent orchestration in `hive`; Replay is what I learned *after* watching agents fail: crashes, retries, duplicate actions, and no audit trail. Every feature here exists because a real failure demanded it.

---

## Definition of done (project level)
- 1,000 chaos-killed runs complete with **zero duplicated side-effects**
- Any run can be replayed byte-identically offline and forked from any checkpoint
- `replay audit verify` detects tampering of any single row
- Dashboard, CI, benchmarks, demo video, write-up published

---

## Phase 1: Use-case framing, contracts and foundations (Week 1, days 1–3)

**Objective:** decide the *shape* of the system before writing runtime code.

**Tasks**
1. Write `docs/PRD.md` from the use-case above (personas: platform engineer, compliance reviewer, agent developer).
2. Define the **event vocabulary** (the contract everything else depends on):
   `RunCreated, StepScheduled, StepStarted, LLMCalled, ToolCalled, ToolReturned, StepSucceeded, StepFailed, RetryScheduled, ApprovalRequested, ApprovalGranted, ApprovalRejected, PolicyDenied, RunPaused, RunResumed, RunForked, RunCompleted, RunFailed`.
3. Define the workflow schema (YAML + JSON-Schema): step types `llm`, `tool`, `approval`, `branch`, `parallel`; per-step `retry`, `timeout`, `idempotency_key`, `policy`.
4. Write 5 ADRs in `docs/DECISIONS.md`: why Postgres-only, why event sourcing, why `SKIP LOCKED` queues, why hash-chain not blockchain, why record/replay cassettes.
5. Scaffold the repo (monorepo layout below), pre-commit (ruff, mypy), CI skeleton, `docker-compose.yml` (Postgres, API, worker).

```
replay/
  core/            # events, state machine, hashing, workflow schema (pure Python, no I/O)
  store/           # Postgres event store, leases, migrations (alembic)
  worker/          # poll loop, step executors, retry logic
  adapters/        # llm/ (2 providers + fake), tools/ (registry)
  policy/          # rule engine
  api/             # FastAPI
  cli/             # replay run|fork|audit|eval
  ui/              # TypeScript (React)
  sidecar/         # optional Rust or Go tool executor
  tests/           # unit, integration, chaos
  docs/
```

**Deliverables:** PRD, ADRs, event + workflow schemas, scaffold with green CI.
**Exit criteria:** schemas validate sample workflows; a reviewer can understand the system from the docs alone.

---

## Phase 2: Event store and run state machine (Week 1–2)

**Objective:** a run's state is *derived from events*, never stored as mutable truth.

**Tasks**
1. Postgres schema:
```sql
CREATE TABLE runs (run_id UUID PRIMARY KEY, workflow TEXT, parent_run_id UUID,
                   fork_seq INT, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE events (
  run_id UUID, seq INT, type TEXT NOT NULL, payload JSONB NOT NULL,
  prev_hash BYTEA, hash BYTEA NOT NULL, created_at TIMESTAMPTZ DEFAULT now(),
  PRIMARY KEY (run_id, seq));
```
2. Append-only enforcement: revoke `UPDATE/DELETE` on `events` for the app role; add a trigger as a second guard.
3. `hash = SHA256(prev_hash || canonical_json(type, payload, seq))`, canonical JSON with sorted keys.
4. Pure-function **reducer**: `state = fold(events)`; property-based tests (Hypothesis) that any valid event sequence yields a valid state and invalid sequences are rejected.
5. Optimistic concurrency: `INSERT` with expected `seq`; conflicting writers retry.
6. Snapshotting every N events for fast load (optional, measured).

**Deliverables:** event store library, reducer, migrations, property tests.
**Exit criteria:** rebuild state of a 10,000-event run in under 500 ms; fuzz tests find no invariant breaks.

---

## Phase 3: Workers, durable queue, retries, idempotency (Week 2)

**Objective:** survive `kill -9` at any instruction without losing or duplicating work.

**Tasks**
1. Work queue in Postgres using `SELECT … FOR UPDATE SKIP LOCKED`; table `tasks(run_id, step_id, available_at, lease_until, attempts)`.
2. **Leases and heartbeats:** worker takes a lease; a background heartbeat extends it; an expired lease makes the task claimable by another worker.
3. **Retry policy** per step: exponential backoff with jitter, max attempts, retryable-vs-fatal error classification.
4. **Idempotency for side-effects:** each tool call carries `idempotency_key = hash(run_id, step_id, attempt-independent input)`. The tool executor stores `(key → result)`; a retried call returns the stored result instead of re-executing.
5. The "two generals" edge: tool executed but `ToolReturned` event not yet written when the worker dies. Resolve by *write-ahead* `ToolCalled(key)` event, then execute, then record; on resume, check the idempotency table first. Document this carefully in an ADR.
6. Build the **chaos harness** now (not later): spawns N workers, randomly kills them, asserts invariants at the end.

**Deliverables:** worker, queue, retries, idempotency store, chaos harness v1.
**Exit criteria:** 200 runs × random kills → all complete, zero duplicate side-effects (counted via a mock tool that records every real execution).

---

## Phase 4: Step executors and adapters (Week 3)

**Objective:** real workflows, not toy loops.

**Tasks**
1. `llm` step: provider adapter interface (`complete(messages, tools, params) -> response`) with two real providers plus a **Fake LLM** that returns scripted outputs for tests.
2. `tool` step: registry with JSON-Schema input validation; per-tool timeout, circuit breaker, and rate limit.
3. `branch` (conditional on prior output) and `parallel` (fan-out/fan-in with partial-failure policy).
4. Token and cost accounting recorded inside `LLMCalled` events.
5. **Optional language-signal task:** implement the tool executor as a small **Rust or Go** sidecar (HTTP tool calls with timeouts, retries, circuit breaking). Document the trade-off versus staying in Python, and benchmark both. This addresses the JD's Rust/Go interest honestly: you only claim it if you ship it.
6. Build the demo workflow: *yield-excursion triage* with tools `read_lot_log`, `compute_excursion_stats`, `propose_hold`, `mes_place_hold` (flaky on purpose).

**Deliverables:** four step types, adapters, demo workflow running end-to-end.
**Exit criteria:** demo workflow completes on the Fake LLM in CI and on a real LLM manually.

---

## Phase 5: Human-in-the-loop approvals and policy engine (Week 3–4)

**Objective:** make the agent *accountable*.

**Tasks**
1. `approval` step: emits `ApprovalRequested`, run goes to `PAUSED` consuming **zero compute**; API endpoints to approve / reject / edit-and-approve; timeout action (auto-reject or escalate).
2. Store approver identity, timestamp and the exact payload shown at decision time inside the event (this is what the auditor needs).
3. **Policy engine:** YAML rules evaluated *before* every tool call, e.g.
```yaml
- name: hold-requires-approval
  when: {tool: mes_place_hold, input.lot_count: {gt: 2}}
  action: require_approval
- name: no-pii-to-external
  when: {tool: send_email, input.body: {matches: "\\b\\d{12}\\b"}}
  action: deny
```
4. Every denial becomes a `PolicyDenied` event with the matched rule name.
5. Policy unit tests table-driven; policy changes are versioned and the active version is recorded per run.

**Deliverables:** approval flow, policy engine, tests.
**Exit criteria:** a paused run survives a full API/worker restart and resumes correctly after approval; denied calls never reach the tool.

---

## Phase 6: Record/replay and fork-from-checkpoint (Week 4–5)

**Objective:** the signature feature, *time-travel debugging*.

**Tasks**
1. **Cassettes:** all external I/O (LLM responses, tool results) is already in events; replay mode substitutes recorded outputs for live calls and **fails the test if any network call is attempted**.
2. **Determinism audit:** list non-deterministic sources (time, randomness, dict ordering, parallel completion order) and inject them via the event log (`now()` and `random()` become recorded values).
3. **Fork:** `replay fork <run> --at-seq 14 --override step=propose_hold.output=@edited.json` creates a new run whose events 1–13 are *references* (not copies) to the parent, then continues live.
4. **Diff view:** side-by-side of parent vs fork at the divergence point (CLI first, UI later).
5. Regression use-case: `replay replay-all --corpus failures/` re-runs saved failing runs after a code change to prove the bug stays fixed.

**Deliverables:** replay mode, fork command, diff output, determinism ADR.
**Exit criteria:** replay of the entire test corpus is byte-identical with zero network calls; forked run diverges exactly at the chosen step.

---

## Phase 7: Observability, evals and the TypeScript UI (Week 5–6)

**Objective:** make runs measurable and visible to humans.

**Tasks**
1. OpenTelemetry: one trace per run, a span per step, attributes for tokens, cost, retries; Prometheus metrics; ship a Grafana dashboard JSON.
2. **Eval hook:** attach assertions to runs (`must_call_tool`, `output_matches_schema`, `judge: rubric.yaml`); `replay eval run suite.yaml` outputs pass-rate and a regression diff against a stored baseline; CI fails on regression.
3. **Audit CLI:** `replay audit verify <run>` recomputes the hash chain; `replay audit export` produces a signed JSON bundle for the compliance reviewer.
4. **TypeScript/React UI:** run timeline (events), step detail, **Fork** button, **Approval inbox**, audit-status badge.
5. Auth: simple API-key or OIDC-ready middleware; roles `viewer`, `approver`, `admin`.

**Deliverables:** traces, dashboard, eval runner, audit CLI, UI.
**Exit criteria:** an approval round trip under 2 s; UI renders a 1,000-event run smoothly; tampering a row flips the audit badge red.

---

## Phase 8: Chaos testing, benchmarks, documentation and launch (Week 6–8)

**Objective:** turn it into evidence a hiring manager can verify in five minutes.

**Tasks**
1. **Final chaos suite:** 1,000 runs, random worker kills, DB connection drops, slow tools; publish the invariants and the pass result.
2. **Benchmarks** (state hardware): runs/sec, steps/sec, p50/p95 orchestration overhead (excluding LLM time), audit-verify events/sec, replay speed.
3. **Security notes:** prompt-injection via tool outputs, tool abuse, SQL injection, secrets handling; document mitigations and residual risks.
4. **Honest comparison:** a doc comparing Replay to Temporal / LangGraph persistence: where Replay is lighter, where it is weaker. Credibility comes from candour.
5. Docs: README with the use-case story first, architecture diagram, one-command demo (`docker compose up && make demo`), `RESULTS.md`, `AI_WORKFLOW.md` (how you used AI coding tools, what you verified manually, one case where AI was wrong).
6. Release v0.1.0, 2-minute demo video: *kill a worker, fork a run with an edited tool output, verify the audit chain*.
7. Write one article: "What I learned making agent runs replayable".

**Deliverables:** tagged release, results doc, video, article.
**Exit criteria:** a stranger can clone, run the demo, and reproduce the headline chaos result in under 10 minutes.

---

## What to say in interviews
- "I treat agent runs as event logs. State is derived, never trusted."
- "The hardest bug was the gap between executing a tool and recording its result; I solved it with write-ahead events plus idempotency keys. Here's the ADR."
- "I can show you a run, fork it at step 14 and prove the audit chain is intact."

## Risks and mitigations
| Risk | Mitigation |
|---|---|
| Scope creep into a full workflow product | Cut UI polish before cutting chaos tests |
| "Why not Temporal?" | Honest comparison doc; Replay is agent-specific (fork, cassette, policy) |
| Non-determinism leaks into replay | Determinism audit in Phase 6 plus replay-in-CI |
