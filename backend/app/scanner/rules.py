from __future__ import annotations

import re
from datetime import datetime
from typing import Any


def _number(value: Any) -> float | None:
    try:
        return float(str(value).replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def evaluate_rule(
    question_id: str,
    value: Any,
    definition: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any] | None:
    target = definition.get("question_id")
    if target and target != question_id:
        return None
    operator = definition.get("operator")
    message = str(definition.get("message") or "Configured validation rule failed")
    failed = False
    suggestion = definition.get("correction")
    if operator == "range":
        number = _number(value)
        minimum = definition.get("min")
        maximum = definition.get("max")
        failed = number is None or (minimum is not None and number < float(minimum)) or (maximum is not None and number > float(maximum))
    elif operator == "allowed":
        allowed = [str(item).casefold() for item in definition.get("values", [])]
        failed = _text(value).casefold() not in allowed
    elif operator == "regex":
        failed = re.fullmatch(str(definition.get("pattern", "")), _text(value)) is None
    elif operator == "required_if":
        other = context.get(str(definition.get("field")))
        failed = other == definition.get("equals") and not _text(value)
    elif operator == "equals_field":
        failed = value != context.get(str(definition.get("field")))
    elif operator == "date_after_field":
        try:
            current = datetime.fromisoformat(_text(value))
            other = datetime.fromisoformat(_text(context.get(str(definition.get("field")))))
            failed = current < other
        except ValueError:
            failed = True
    if not failed:
        return None
    return {
        "question_id": question_id,
        "message": message,
        "suggestion": suggestion,
        "rule_id": str(definition.get("rule_id", "configured")),
        "evidence_basis": "deterministic_rule",
    }


def generic_findings(answer: dict[str, Any]) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    value = answer.get("scanner_value")
    allowed = answer.get("allowed_options") or []
    allowed_labels = [str(item.get("label") if isinstance(item, dict) else item) for item in allowed]
    answer_type = answer.get("answer_type")
    if allowed_labels and answer_type in {"single_choice", "yes_no", "consent", "scale"}:
        if _text(value) and _text(value).casefold() not in {item.casefold() for item in allowed_labels}:
            findings.append(
                {
                    "question_id": answer.get("question_id"),
                    "message": "Scanned value is not one of the printed options",
                    "suggestion": None,
                    "rule_id": "generic.allowed_option",
                    "evidence_basis": "printed_option",
                }
            )
    return findings
