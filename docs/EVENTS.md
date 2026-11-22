# Replay Event Vocabulary

## Run lifecycle

| Event | When | Payload keys |
|---|---|---|
| RunCreated | run starts | run_id, workflow, parent_run_id, fork_seq |
| StepScheduled | before executing a step | run_id, step_id, workflow |
| StepStarted | worker begins step | run_id, step_id, attempt |
| LLMCalled | LLM invocation | model, input_tokens, output_tokens, cost_usd |
| ToolCalled | tool invocation (WAL) | tool, idempotency_key, input |
| ToolReturned | tool result | tool, idempotency_key, output |
| StepSucceeded | step completed OK | run_id, step_id, output |
| StepFailed | step failed | run_id, step_id, error, retryable |
| RetryScheduled | backoff set | run_id, step_id, next_attempt_at, delay_ms |
| ApprovalRequested | run paused | run_id, step_id, approver_id, payload |
| ApprovalGranted | human approved | run_id, approval_id, approver_id, timestamp |
| ApprovalRejected | human rejected | run_id, approval_id, approver_id, reason |
| PolicyDenied | policy blocked tool call | run_id, tool, rule, input |
| RunPaused | pause requested | run_id, reason |
| RunResumed | resume after pause | run_id, actor |
| RunForked | fork created | run_id, parent_run_id, fork_seq, events_ref |
| RunCompleted | run finished OK | run_id, result |
| RunFailed | run finished with error | run_id, error |
