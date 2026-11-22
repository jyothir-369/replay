# Workflow Schema Reference

Workflows are YAML definitions with a `name`, `version`, and `steps` list.

```yaml
name: yield-excursion-triage
version: "0.1.0"
steps:
  - step_id: read_lot_log
    type: tool
    config:
      tool: read_lot_log
      lot_id: L001
    retry:
      max_attempts: 3
      base_delay: 1.0
    timeout: 30
    idempotency_key: "read_lot_log.L001"
    policy:
      - hold-requires-approval
  - step_id: propose_hold
    type: llm
    config:
      model: claude-sonnet-5
      prompt: "Propose whether to place lot L001 on hold based on the above data."
    retry: {}
    depends_on: [read_lot_log]
```

## Step Types

| Type | Config | Description |
|---|---|---|
| `tool` | `tool: <name>`, `lot_id: <id>` | Invoke a registered tool |
| `llm` | `model`, `prompt` | LLM call with full event logging |
| `approval` | `approver_role` | Pause until human approves |
| `branch` | `condition`, `then`, `else` | Conditional branching |
| `parallel` | `steps: [step_ids]` | Fan-out / fan-in |

## Validation Rules

- Step IDs must be unique within a workflow.
- Step types must be one of: `llm`, `tool`, `approval`, `branch`, `parallel`.
- `depends_on` references must exist in the same workflow.
- Unknown step types and duplicate IDs are rejected at load time.
