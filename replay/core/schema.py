"""Workflow schema (YAML + JSON-Schema) validation.

Supported step types: llm, tool, approval, branch, parallel.
Per-step keys: retry, timeout, idempotency_key, policy.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

__all__ = ["WorkflowSchema", "StepSpec", "WorkflowSpec", "validate_workflow"]

VALID_STEP_TYPES = {"llm", "tool", "approval", "branch", "parallel"}


class StepSpec(BaseModel):
    step_id: str
    type: str
    config: Dict[str, Any] = Field(default_factory=dict)
    retry: Dict[str, Any] = Field(default_factory=dict)
    timeout: Optional[int] = None
    idempotency_key: Optional[str] = None
    policy: List[str] = Field(default_factory=list)
    depends_on: List[str] = Field(default_factory=list)

    @field_validator("type")
    @classmethod
    def _valid_type(cls, v: str) -> str:
        if v not in VALID_STEP_TYPES:
            raise ValueError(f"step type must be one of {sorted(VALID_STEP_TYPES)}, got {v!r}")
        return v

    @field_validator("step_id")
    @classmethod
    def _non_empty_step_id(cls, v: str) -> str:
        if not v or not v.strip():
            raise ValueError("step_id must be non-empty")
        return v.strip()


class WorkflowSpec(BaseModel):
    name: str
    version: str = "0.1.0"
    steps: List[StepSpec] = Field(default_factory=list)


class WorkflowSchema:
    """Validate a workflow definition (dict or YAML string) against the schema."""

    def __init__(self) -> None:
        self.spec: WorkflowSpec | None = None

    def parse(self, definition: str | Dict[str, Any]) -> WorkflowSpec:
        if isinstance(definition, str):
            raw = yaml.safe_load(definition)
        else:
            raw = definition
        self.spec = WorkflowSpec(**raw)
        return self.spec

    def validate(self, definition: str | Dict[str, Any]) -> WorkflowSpec:
        spec = self.parse(definition)
        step_ids = [s.step_id for s in spec.steps]
        if len(step_ids) != len(set(step_ids)):
            raise ValueError("duplicate step_id values found in workflow")
        for s in spec.steps:
            for dep in s.depends_on:
                if dep not in step_ids:
                    raise ValueError(f"step {s.step_id!r} depends on unknown step {dep!r}")
        return spec


def validate_workflow(definition: str | Dict[str, Any]) -> WorkflowSpec:
    """Convenience function: parse + validate a workflow definition."""
    return WorkflowSchema().validate(definition)
