"""Policy engine: YAML rules evaluated before every tool call.

Rules are evaluated against the StepSpec payload and context.

Example rule:
- name: hold-requires-approval
  when: {tool: mes_place_hold, input.lot_count: {gt: 2}}
  action: require_approval
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Optional


class PolicyEngine:
    """Parse and evaluate policy rules against a StepSpec payload."""

    def __init__(self, rules: List[Dict[str, Any]]) -> None:
        self.rules = rules

    def evaluate(
        self,
        step_type: str,
        step_id: str,
        payload: Dict[str, Any],
        step_config: Dict[str, Any],
    ) -> Optional[str]:
        """Return action if a rule matches, else None."""
        for rule in self.rules:
            when = rule.get("when", {})
            action = rule.get("action")
            if not when or not action:
                continue

            # Evaluate condition: when: {tool: mes_place_hold, input.lot_count: {gt: 2}}
            condition_matches = self._evaluate_condition(when, step_type, step_id, payload)

            if condition_matches:
                return action
        return None

    def _evaluate_condition(
        self,
        condition: Dict[str, Any],
        step_type: str,
        step_id: str,
        payload: Dict[str, Any],
    ) -> bool:
        """Evaluate a nested condition expression.

        Supported syntax:
            condition: {tool: mes_place_hold, input.lot_count: {gt: 2}}
        """
        if "tool" in condition:
            tool_name = condition["tool"]
            if step_type != tool_name:
                return False

        if "input" in condition:
            input_cond = condition["input"]
            # Support: input.lot_count: {gt: 2}
            for key_path, op_dict in input_cond.items():
                # key_path format: "lot_count" or "lot_count.subfield"
                parts = key_path.split(".")
                current = payload
                for part in parts:
                    if isinstance(current, dict) and part in current:
                        current = current[part]
                    else:
                        return False

                if not isinstance(current, (dict, int, float, str)):
                    return False

                # Check operation: {gt: 2}, {eq: "value"}, etc.
                op = next(iter(op_dict))
                value = next(iter(op_dict.values()))
                if op == "gt":
                    return current > value
                if op == "lt":
                    return current < value
                if op == "eq":
                    return current == value
                if op == "matches":
                    return isinstance(current, str) and re.search(value, current) is not None
        return True