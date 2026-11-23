"""Workflow schema validation test: step types, configs, and validation."""

import pytest
from replay.core.schema import WorkflowSchema, validate_workflow

def test_workflow_schema_valid():
    yaml_def = """
    name: yield-excursion
    steps:
      - step_id: read_lot_log
        type: tool
        config: {}
        retry: {}
      - step_id: compute_excursion_stats
        type: tool
        config: {}
        retry: {}
      - step_id: propose_hold
        type: llm
        config: {}
        retry: {}
        policy: []
    """
    spec = validate_workflow(yaml_def)
    assert len(spec.steps) == 3

def test_workflow_schema_invalid_step_type():
    yaml_def = """
    name: invalid
    steps:
      - step_id: bad_step
        type: invalid_type
        config: {}
    """
    with pytest.raises(ValueError, match="step type must be one of"):
        validate_workflow(yaml_def)

def test_workflow_schema_duplicate_steps():
    yaml_def = """
    name: dup
    steps:
      - step_id: step1
        type: tool
        config: {}
      - step_id: step1
        type: tool
        config: {}
    """
    with pytest.raises(ValueError, match="duplicate step_id values"):
        validate_workflow(yaml_def)

def test_workflow_schema_unknown_dependency():
    yaml_def = """
    name: deps
    steps:
      - step_id: step1
        type: tool
        config: {}
        depends_on: [step_nonexistent]
      - step_id: step2
        type: tool
        config: {}
    """
    with pytest.raises(ValueError, match="depends on unknown step"):
        validate_workflow(yaml_def)

def test_workflow_schema_parsing_error():
    # Invalid YAML: unquoted key starting with a digit-like or malformed block
    yaml_def = """
    name: broken
    steps:
      - step_id: "step one with spaces"
        type: tool
        config: {}
    """
    spec = validate_workflow(yaml_def)
    assert spec.steps[0].step_id == "step one with spaces"